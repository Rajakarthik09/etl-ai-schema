#!/usr/bin/env python3
"""
Run AI mapping regeneration for NYC taxi schema evolution scenarios.

Loads taxi business logic from etl/transform_taxi.py, then calls regenerate_mapping
for the chosen scenario (v1_to_v2 or v2_to_v3). Output: etl/transform_generated.py.

Usage:
  python scripts/run_taxi_regenerate_mapping.py --scenario v1_to_v2
  python scripts/run_taxi_regenerate_mapping.py --scenario v2_to_v3
  python scripts/run_taxi_regenerate_mapping.py   # defaults to v1_to_v2
"""
import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCHEMAS_DIR = os.path.join(PROJECT_ROOT, "schemas")
ETL_DIR = os.path.join(PROJECT_ROOT, "etl")
OUTPUT_PATH = os.path.join(ETL_DIR, "transform_generated.py")

sys.path.insert(0, PROJECT_ROOT)
from ai.regenerate_mapping import regenerate_mapping


def get_taxi_transform_snippet():
    """Read etl/transform_taxi.py and return the transform function as snippet for the prompt."""
    path = os.path.join(ETL_DIR, "transform_taxi.py")
    with open(path, "r") as f:
        content = f.read()
    # Extract from first def transform to end of function (next def or end of file)
    start = content.find("def transform(")
    if start == -1:
        return content
    end = content.find("\n\ndef ", start + 1)
    if end == -1:
        end = len(content)
    return content[start:end].strip()


def main():
    parser = argparse.ArgumentParser(description="Regenerate taxi ETL mapping via AI.")
    parser.add_argument(
        "--scenario",
        choices=["v1_to_v2", "v2_to_v3"],
        default="v1_to_v2",
        help="Schema evolution scenario",
    )
    parser.add_argument(
        "--output",
        default=OUTPUT_PATH,
        help="Output path for generated transform",
    )
    args = parser.parse_args()

    old_key = "yellow_v1" if args.scenario == "v1_to_v2" else "yellow_v2"
    new_key = "yellow_v2" if args.scenario == "v1_to_v2" else "yellow_v3"
    changes_key = "yellow_v1_to_v2_changes" if args.scenario == "v1_to_v2" else "yellow_v2_to_v3_changes"

    old_schema_path = os.path.join(SCHEMAS_DIR, f"{old_key}_schema.json")
    new_schema_path = os.path.join(SCHEMAS_DIR, f"{new_key}_schema.json")
    changes_path = os.path.join(SCHEMAS_DIR, f"{changes_key}.json")

    for p in (old_schema_path, new_schema_path, changes_path):
        if not os.path.isfile(p):
            print(f"Error: {p} not found. Run scripts/run_taxi_schema_detection.py first.", file=sys.stderr)
            sys.exit(1)

    snippet = get_taxi_transform_snippet()
    regenerate_mapping(
        old_schema_path,
        new_schema_path,
        changes_path,
        args.output,
        current_transform_snippet=snippet,
    )
    print("Done. Use etl/transform_generated.py in the pipeline for the AI-generated taxi transform.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
