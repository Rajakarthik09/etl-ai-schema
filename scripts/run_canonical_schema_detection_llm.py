#!/usr/bin/env python3
"""
Run LLM-based schema change detection for the canonical ↔ V2 scenario.

This script treats the canonical schema (as defined in etl/canonical_schema.py)
as the OLD schema and the NYC taxi V2 source schema as the NEW schema, and asks
the LLM to infer the change set between them.

It writes the inferred changes to:

  results/canonical_changes_v2_llm_<label>.json

where <label> is a filesystem-safe version of the model identifier
(e.g. HF_openai_gpt-oss-120b_groq).
"""

import argparse
import json
import os
from pathlib import Path
from typing import Dict, Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
SCHEMAS_DIR = PROJECT_ROOT / "schemas"
RESULTS_DIR = PROJECT_ROOT / "results"

import sys

sys.path.insert(0, str(PROJECT_ROOT))

from etl.canonical_schema import get_canonical_schema_dict  # type: ignore  # noqa: E402
from scripts.canonical_schema_support import (  # type: ignore  # noqa: E402
    get_current_schema_for_version,
)
from ai.llm_schema_detection import detect_changes_with_llm  # noqa: E402


def _safe_label(label: str) -> str:
    """Make a filesystem-safe label from a model identifier."""
    cleaned = label.replace("hf:", "HF_").replace("hf/", "HF_")
    cleaned = cleaned.replace("/", "_").replace(":", "_")
    return cleaned


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run LLM-based schema change detection for canonical ↔ V2 and "
            "persist the inferred change JSON under results/."
        )
    )
    parser.add_argument(
        "--model",
        default="llama3",
        help=(
            "LLM model identifier (e.g. llama3, mistral, "
            "hf:openai/gpt-oss-120b:groq). Must be supported by MappingRegenerator."
        ),
    )
    args = parser.parse_args()

    version = "v2"
    csv_path = RAW_DIR / f"yellow_base_{version}.csv"
    if not csv_path.is_file():
        raise SystemExit(f"Source CSV not found for version {version}: {csv_path}")

    # Materialise or load the V2 schema (same format as SchemaChangeDetector.extract_schema).
    current_schema, current_schema_path = get_current_schema_for_version(
        version, str(csv_path)
    )

    # Write canonical schema to JSON so the generic LLM detector can consume it.
    canonical_schema = get_canonical_schema_dict()
    canonical_schema_path = SCHEMAS_DIR / "canonical_schema.json"
    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    with canonical_schema_path.open("w") as f:
        json.dump(canonical_schema, f, indent=2)

    model_label = _safe_label(args.model)
    out_path = RESULTS_DIR / f"canonical_changes_v2_llm_{model_label}.json"

    print(
        f"Running LLM-based schema detection for canonical ↔ V2 using model '{args.model}'..."
    )
    # IMPORTANT: The ground-truth file canonical_changes_v2.json encodes changes
    # in the direction V2 -> canonical (i.e. NEW = canonical schema). To make
    # the LLM output comparable, we call the detector with OLD = V2 schema and
    # NEW = canonical schema here.
    changes: Dict[str, Any] = detect_changes_with_llm(
        old_schema_path=Path(current_schema_path),  # V2 schema
        new_schema_path=canonical_schema_path,      # canonical schema
        scenario_name="canonical_v2",
        model=args.model,
    )

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        json.dump(changes, f, indent=2)
    print(f"✓ Saved LLM-inferred canonical↔V2 changes to {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

