#!/usr/bin/env python3
"""
Regenerate ETL mapping from a current taxi schema (e.g. V1 or V2) to the
canonical schema. The generated transform maps upstream CSV columns into
the single canonical table schema.

Usage:
  python scripts/run_canonical_to_canonical_regeneration.py --version v1
  python scripts/run_canonical_to_canonical_regeneration.py --version v2 --model llama3
"""
import argparse
import json
import os
import sys
import tempfile

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
SCHEMAS_DIR = os.path.join(PROJECT_ROOT, "schemas")
ETL_DIR = os.path.join(PROJECT_ROOT, "etl")
OUTPUT_PATH = os.path.join(ETL_DIR, "transform_generated.py")

sys.path.insert(0, PROJECT_ROOT)
from ai.regenerate_mapping import regenerate_mapping
from ai.detect_schema_change import SchemaChangeDetector
from etl.canonical_schema import get_canonical_schema_dict

try:
    from scripts.canonical_schema_support import get_current_schema_for_version
except ImportError:
    from canonical_schema_support import get_current_schema_for_version


def get_taxi_transform_snippet():
    """Read etl/transform_taxi.py and return the transform function as snippet for the prompt."""
    path = os.path.join(ETL_DIR, "transform_taxi.py")
    with open(path, "r") as f:
        content = f.read()
    start = content.find("def transform(")
    if start == -1:
        return content
    end = content.find("\n\ndef ", start + 1)
    if end == -1:
        end = len(content)
    return content[start:end].strip()


def main():
    parser = argparse.ArgumentParser(description="Regenerate mapping from current schema to canonical.")
    parser.add_argument("--version", required=True, help="Current taxi version (e.g. v1, v4, v5). CSV must be data/raw/yellow_base_<version>.csv.")
    parser.add_argument("--model", default="llama3", help="LLM model: llama3, mistral, mistral-small-latest, etc.")
    parser.add_argument("--output", default=OUTPUT_PATH, help="Output path for generated transform.")
    args = parser.parse_args()

    csv_path = os.path.join(RAW_DIR, f"yellow_base_{args.version}.csv")
    if not os.path.isfile(csv_path):
        print(f"Error: {csv_path} not found.", file=sys.stderr)
        sys.exit(1)
    try:
        current_schema, current_schema_path = get_current_schema_for_version(args.version, csv_path)
    except Exception as e:
        print(f"Error getting schema: {e}", file=sys.stderr)
        sys.exit(1)

    canonical_schema = get_canonical_schema_dict()
    detector = SchemaChangeDetector()
    changes = detector.compare_schemas(current_schema, canonical_schema)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        canonical_path = f.name
        json.dump(canonical_schema, f, indent=2)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        changes_path = f.name
        json.dump(changes, f, indent=2)

    try:
        snippet = get_taxi_transform_snippet()
        regenerate_mapping(
            current_schema_path,
            canonical_path,
            changes_path,
            args.output,
            current_transform_snippet=snippet,
            model=args.model,
        )
    finally:
        os.unlink(canonical_path)
        os.unlink(changes_path)

    print("Done. Generated transform maps current schema -> canonical. Use with run_canonical_ingestion.py --method llm.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
