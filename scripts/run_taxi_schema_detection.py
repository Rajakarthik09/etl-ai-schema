#!/usr/bin/env python3
"""
Run SchemaChangeDetector on NYC taxi V1/V2/V3 and persist schemas and change JSONs.

Produces:
  schemas/yellow_v1_schema.json, yellow_v2_schema.json, yellow_v3_schema.json
  schemas/yellow_v1_to_v2_changes.json, schemas/yellow_v2_to_v3_changes.json
  (optional) schemas/yellow_v1_to_v3_changes.json
"""
import json
import os
import sys

# Project root and paths
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
SCHEMAS_DIR = os.path.join(PROJECT_ROOT, "schemas")

sys.path.insert(0, PROJECT_ROOT)
from ai.detect_schema_change import detect_changes


def run_scenario(old_file: str, new_file: str, old_schema_name: str, new_schema_name: str, changes_name: str):
    """Run detection and save schema + changes JSONs."""
    old_path = os.path.join(RAW_DIR, old_file)
    new_path = os.path.join(RAW_DIR, new_file)
    old_schema_path = os.path.join(SCHEMAS_DIR, f"{old_schema_name}_schema.json")
    new_schema_path = os.path.join(SCHEMAS_DIR, f"{new_schema_name}_schema.json")
    changes_path = os.path.join(SCHEMAS_DIR, f"{changes_name}_changes.json")

    if not os.path.isfile(old_path) or not os.path.isfile(new_path):
        print(f"Skipping {old_file} -> {new_file}: files missing")
        return
    os.makedirs(SCHEMAS_DIR, exist_ok=True)

    result = detect_changes(old_path, new_path, old_schema_path, new_schema_path)
    with open(changes_path, "w") as f:
        json.dump(result["changes"], f, indent=2)
    print(f"Saved {changes_path}")
    return result


def main():
    os.makedirs(SCHEMAS_DIR, exist_ok=True)

    run_scenario(
        "yellow_base_v1.csv", "yellow_base_v2.csv",
        "yellow_v1", "yellow_v2", "yellow_v1_to_v2",
    )
    run_scenario(
        "yellow_base_v2.csv", "yellow_base_v3.csv",
        "yellow_v2", "yellow_v3", "yellow_v2_to_v3",
    )
    run_scenario(
        "yellow_base_v1.csv", "yellow_base_v3.csv",
        "yellow_v1", "yellow_v3", "yellow_v1_to_v3",
    )
    print("Taxi schema detection complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
