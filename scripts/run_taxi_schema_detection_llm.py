#!/usr/bin/env python3
"""
Run LLM-based schema change detection on the NYC taxi V1 -> V2 scenario.

This script assumes that `scripts/run_taxi_schema_detection.py` has already
been executed to materialise:

  schemas/yellow_v1_schema.json
  schemas/yellow_v2_schema.json

It then calls the LLM-based detector to infer the change set between these
schemas and writes a JSON file with the same structure as the heuristic
detector output:

  schemas/yellow_v1_to_v2_llm_<label>.json

where <label> is a filesystem-safe version of the model label, so that
different models (llama3, mistral, HF gpt-oss-120b, ...) can be compared.
"""

import argparse
import json
import os
from pathlib import Path
from typing import Dict

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMAS_DIR = PROJECT_ROOT / "schemas"

import sys

sys.path.insert(0, str(PROJECT_ROOT))

from ai.llm_schema_detection import detect_changes_with_llm  # noqa: E402


def _safe_label(label: str) -> str:
    """Make a filesystem-safe label from a model identifier."""
    cleaned = label.replace("hf:", "HF_").replace("hf/", "HF_")
    cleaned = cleaned.replace("/", "_").replace(":", "_")
    return cleaned


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Run LLM-based schema change detection for NYC taxi V1 -> V2 and "
            "persist the inferred change JSON."
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

    old_schema_path = SCHEMAS_DIR / "yellow_v1_schema.json"
    new_schema_path = SCHEMAS_DIR / "yellow_v2_schema.json"

    if not old_schema_path.is_file() or not new_schema_path.is_file():
        raise SystemExit(
            "Schema JSONs not found. Run 'python scripts/run_taxi_schema_detection.py' "
            "first to materialise yellow_v1_schema.json and yellow_v2_schema.json."
        )

    model_label = _safe_label(args.model)
    out_path = SCHEMAS_DIR / f"yellow_v1_to_v2_llm_{model_label}.json"

    print(
        f"Running LLM-based schema detection for taxi V1 -> V2 using model '{args.model}'..."
    )
    changes: Dict = detect_changes_with_llm(
        old_schema_path=old_schema_path,
        new_schema_path=new_schema_path,
        scenario_name="yellow_v1_to_v2_changes",
        model=args.model,
    )

    SCHEMAS_DIR.mkdir(parents=True, exist_ok=True)
    with out_path.open("w") as f:
        json.dump(changes, f, indent=2)
    print(f"✓ Saved LLM-inferred changes to {out_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

