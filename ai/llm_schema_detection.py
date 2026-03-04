"""
LLM-based Schema Change Detection (RAG-style)

This module uses the same LLM backends as the mapping regenerator to infer
schema changes between two versions of a dataset. It takes as input two
schema JSON files (as produced by `SchemaChangeDetector.extract_schema`) and
asks the model to emit a JSON object with the same structure as the
heuristic detector's `changes` dictionary:

{
  "added_columns": [
    {"column": "...", "type": "...", "sample_values": [...]},
    ...
  ],
  "removed_columns": [
    {"column": "...", "type": "..."},
    ...
  ],
  "renamed_columns": [
    {"old_column": "...", "new_column": "...", "old_type": "...", "new_type": "..."},
    ...
  ],
  "type_changes": [
    {"column": "...", "old_type": "...", "new_type": "..."},
    ...
  ]
}

In the thesis, this serves as a RAG-style detector: the LLM is grounded in
machine-readable schema descriptions (column names, dtypes, sample values)
and optionally past change sets or documentation, and its JSON output can be
evaluated against the same ground truth as the heuristic detector.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

from ai.regenerate_mapping import MappingRegenerator


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DOCS_DIR = PROJECT_ROOT / "docs"


def _load_json(path: Path) -> Dict[str, Any]:
    with path.open("r") as f:
        return json.load(f)


def _extract_json_object(text: str) -> Dict[str, Any]:
    """
    Best-effort extraction of a top-level JSON object from model output.

    The detector prompt instructs the model to return a single JSON object
    without explanations, but this helper makes the caller more robust
    against minor deviations (e.g. leading/trailing commentary).
    """
    text = text.strip()
    try:
        return json.loads(text)
    except Exception:
        # Fallback: take substring from first '{' to last '}' and try again.
        start = text.find("{")
        end = text.rfind("}")
        if start == -1 or end == -1 or end <= start:
            raise
        snippet = text[start : end + 1]
        return json.loads(snippet)


def build_detection_prompt(
    old_schema: Dict[str, Any],
    new_schema: Dict[str, Any],
    scenario_name: str,
) -> str:
    """
    Construct a prompt that asks the LLM to infer schema changes between two
    versions and to emit a JSON changes object compatible with the heuristic
    detector output.
    """
    # Optionally load any ground-truth documentation for few-shot-style context.
    gt_path = DOCS_DIR / "schema_detection_ground_truth.json"
    gt_snippet = ""
    if gt_path.exists():
        try:
            gt = _load_json(gt_path)
            # Only show the entry for this scenario if present, to keep context small.
            cfg = gt.get(scenario_name)
            if cfg:
                gt_snippet = json.dumps({scenario_name: cfg}, indent=2)
        except Exception:
            gt_snippet = ""

    old_schema_str = json.dumps(old_schema, indent=2)
    new_schema_str = json.dumps(new_schema, indent=2)

    prompt_parts = [
        "You are an expert in data integration and schema evolution.",
        "Your task is to infer schema changes between two versions of a tabular dataset.",
        "",
        "The schemas below are machine-readable descriptions extracted from CSV files.",
        "Each schema has a `columns` object mapping column names to metadata:",
        "- `dtype`: the inferred pandas / NumPy dtype (e.g. 'int64', 'float64', 'object'),",
        "- `nullable`: whether the column can contain nulls,",
        "- `sample_values`: up to three representative values seen in the data.",
        "",
        "Based ONLY on these schemas (names, types, and sample values),",
        "you must propose a set of schema change operations between the OLD and NEW version.",
        "",
        f"SCENARIO NAME: {scenario_name}",
        "",
        "OLD SCHEMA:",
        old_schema_str,
        "",
        "NEW SCHEMA:",
        new_schema_str,
        "",
        "The output MUST be a single JSON object with the following structure:",
        "",
        "{",
        '  "added_columns": [',
        '    {"column": "<name>", "type": "<dtype>", "sample_values": [<values>]}',
        "  ],",
        '  "removed_columns": [',
        '    {"column": "<name>", "type": "<dtype>"}',
        "  ],",
        '  "renamed_columns": [',
        '    {"old_column": "<old_name>", "new_column": "<new_name>", '
        '"old_type": "<old_dtype>", "new_type": "<new_dtype>"}',
        "  ],",
        '  "type_changes": [',
        '    {"column": "<name>", "old_type": "<old_dtype>", "new_type": "<new_dtype>"}',
        "  ]",
        "}",
        "",
        "Semantics of the change categories:",
        "- added_columns: columns that exist in NEW but not in OLD (ignoring renames).",
        "- removed_columns: columns that exist in OLD but not in NEW (ignoring renames).",
        "- renamed_columns: pairs where an OLD column has effectively been renamed to a NEW column",
        "  that plays the same role (e.g. 'trip_distance' -> 'trip_km' or 'airport_fee' -> 'Airport_fee').",
        "- type_changes: columns that exist in both versions but change dtype.",
        "",
        "If a column is treated as renamed, do NOT also report it as added/removed.",
        "Be conservative: only declare a rename when the names are similar AND the sample values",
        "have compatible meaning (for example, two numeric distance columns with similar ranges).",
        "",
        "Your JSON must be syntactically valid and contain ALL four keys above, even if some",
        "of the lists are empty. Do not include any extra keys or commentary.",
    ]

    if gt_snippet:
        prompt_parts.extend(
            [
                "",
                "For reference, here is ground-truth change information for this scenario",
                "in a simpler format (ADDED/REMOVED/RENAMED at the level of column names):",
                gt_snippet,
                "",
                "Use this ONLY as high-level guidance and to understand what kinds of",
                "operations are expected. You must still emit the detailed JSON structure",
                "described above; do not copy this example format directly.",
            ]
        )

    prompt_parts.append("")
    prompt_parts.append("Return ONLY the JSON object, with no explanation or markdown.")
    return "\n".join(prompt_parts)


def detect_changes_with_llm(
    old_schema_path: Path,
    new_schema_path: Path,
    scenario_name: str,
    model: str = "llama3",
) -> Dict[str, Any]:
    """
    Run LLM-based schema change detection given two schema JSON files.

    Args:
        old_schema_path: Path to JSON schema for the OLD version.
        new_schema_path: Path to JSON schema for the NEW version.
        scenario_name: Name of the scenario (used in prompts and reporting).
        model: LLM model identifier (e.g. 'llama3', 'mistral', 'hf:openai/gpt-oss-120b:groq').

    Returns:
        Parsed JSON dictionary with keys added_columns / removed_columns /
        renamed_columns / type_changes.
    """
    if not old_schema_path.is_file():
        raise FileNotFoundError(f"Old schema not found: {old_schema_path}")
    if not new_schema_path.is_file():
        raise FileNotFoundError(f"New schema not found: {new_schema_path}")

    old_schema = _load_json(old_schema_path)
    new_schema = _load_json(new_schema_path)

    prompt = build_detection_prompt(old_schema, new_schema, scenario_name=scenario_name)

    regenerator = MappingRegenerator()
    raw = regenerator.call_ai(prompt, model=model)
    changes = _extract_json_object(raw)

    # Basic sanity: ensure required keys exist, even if empty.
    for key in ("added_columns", "removed_columns", "renamed_columns", "type_changes"):
        if key not in changes:
            changes[key] = []

    return changes


__all__ = [
    "build_detection_prompt",
    "detect_changes_with_llm",
]

