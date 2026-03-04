"""
Shared support for canonical ingestion: get current schema for any version (e.g. v1, v2, v4, v5, …).
If schemas/yellow_{version}_schema.json exists, use it; otherwise extract schema from the CSV
and write it so the LLM path and evaluation can run without pre-creating schema files.
"""
import json
import os
import re

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
SCHEMAS_DIR = os.path.join(PROJECT_ROOT, "schemas")


def discover_versions():
    """Discover versions from data/raw: yellow_base_*.csv -> version (e.g. yellow_base_v4.csv -> v4)."""
    if not os.path.isdir(RAW_DIR):
        return []
    versions = []
    for name in os.listdir(RAW_DIR):
        m = re.match(r"yellow_base_(.+)\.csv$", name)
        if m:
            versions.append(m.group(1))
    return sorted(versions)


def get_current_schema_for_version(version: str, csv_path: str):
    """
    Get current schema for this version. If schemas/yellow_{version}_schema.json exists, load it.
    Otherwise extract schema from csv_path, write it to that JSON, and return (schema_dict, path).
    Returns (schema_dict, schema_path) so callers can pass the path to regenerate_mapping.
    """
    schema_path = os.path.join(SCHEMAS_DIR, f"yellow_{version}_schema.json")
    if os.path.isfile(schema_path):
        with open(schema_path, "r") as f:
            schema = json.load(f)
        return schema, schema_path
    # Extract from CSV and cache
    import sys
    sys.path.insert(0, PROJECT_ROOT)
    from ai.detect_schema_change import SchemaChangeDetector
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"CSV not found: {csv_path}")
    detector = SchemaChangeDetector()
    schema = detector.extract_schema(csv_path)
    os.makedirs(SCHEMAS_DIR, exist_ok=True)
    with open(schema_path, "w") as f:
        json.dump(schema, f, indent=2)
    return schema, schema_path
