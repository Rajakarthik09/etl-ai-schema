#!/usr/bin/env python3
"""
Ingest NYC taxi CSVs into the single canonical table (trips_canonical).
Runs baseline and/or LLM path per version; all data lands in one table with
source_version and ingestion_method for comparison.

Usage:
  python scripts/run_canonical_ingestion.py                    # baseline only, all versions
  python scripts/run_canonical_ingestion.py --method llm        # baseline + LLM, all versions
  python scripts/run_canonical_ingestion.py --version v2 --method llm
"""
import argparse
import json
import os
import sys
import tempfile

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
SCHEMAS_DIR = os.path.join(PROJECT_ROOT, "schemas")
ETL_DIR = os.path.join(PROJECT_ROOT, "etl")
sys.path.insert(0, PROJECT_ROOT)

from etl.load import ensure_canonical_table_exists, load_into_canonical
from etl.baseline_to_canonical import normalize_to_canonical
from etl.canonical_schema import get_canonical_columns_with_metadata

# Support v4, v5, or any version: discover from CSV filenames or use --version
try:
    from scripts.canonical_schema_support import discover_versions, get_current_schema_for_version
except ImportError:
    from canonical_schema_support import discover_versions, get_current_schema_for_version


def align_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    cols = get_canonical_columns_with_metadata()
    out = pd.DataFrame(index=df.index)
    for c in cols:
        out[c] = df[c] if c in df.columns else pd.NA
    return out[cols]


def run_llm_ingestion(csv_path: str, version: str, model: str) -> pd.DataFrame:
    """Regenerate mapping current -> canonical, run transform, return canonical DataFrame."""
    from ai.regenerate_mapping import regenerate_mapping
    from ai.detect_schema_change import SchemaChangeDetector
    from etl.canonical_schema import get_canonical_schema_dict

    current_schema, current_schema_path = get_current_schema_for_version(version, csv_path)
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
        with open(os.path.join(ETL_DIR, "transform_taxi.py"), "r") as f:
            content = f.read()
        start = content.find("def transform(")
        end = content.find("\n\ndef ", start + 1) if start != -1 else len(content)
        snippet = content[start:end].strip() if start != -1 else ""
        regenerate_mapping(
            current_schema_path,
            canonical_path,
            changes_path,
            os.path.join(ETL_DIR, "transform_generated.py"),
            current_transform_snippet=snippet,
            model=model,
        )
    finally:
        os.unlink(canonical_path)
        os.unlink(changes_path)

    df = pd.read_csv(csv_path)
    import importlib
    import etl.transform_generated as gen
    importlib.reload(gen)
    # Some LLM-generated transforms use `pd` without importing pandas; make sure
    # the module has a `pd` binding pointing at pandas.
    if not hasattr(gen, "pd"):
        gen.pd = pd
    L = gen.transform(df)
    return align_to_canonical(L)


def main():
    parser = argparse.ArgumentParser(description="Ingest taxi data into single canonical table.")
    parser.add_argument("--version", default=None, help="Only this version (e.g. v1, v4, v5). Default: all discovered from data/raw/yellow_base_*.csv.")
    parser.add_argument("--method", choices=["baseline", "llm", "both"], default="baseline",
                        help="Ingestion method: baseline only, llm only, or both.")
    parser.add_argument("--model", default="llama3", help="LLM model: llama3, mistral (Ollama or Mistral Cloud), mistral-small-latest, etc.")
    args = parser.parse_args()

    versions = [args.version] if args.version else discover_versions()
    if not versions:
        print("No versions found. Add data/raw/yellow_base_<version>.csv (e.g. yellow_base_v4.csv) or pass --version.", file=sys.stderr)
        return 1
    ensure_canonical_table_exists()

    for version in versions:
        csv_path = os.path.join(RAW_DIR, f"yellow_base_{version}.csv")
        if not os.path.isfile(csv_path):
            print(f"Skipping {version}: {csv_path} not found.", file=sys.stderr)
            continue

        df = pd.read_csv(csv_path)

        if args.method in ("baseline", "both"):
            B = normalize_to_canonical(df)
            load_into_canonical(B, source_version=version, ingestion_method="baseline")
            print(f"Loaded {len(B)} rows for {version} (baseline).")

        if args.method in ("llm", "both"):
            try:
                L = run_llm_ingestion(csv_path, version, args.model)
                load_into_canonical(L, source_version=version, ingestion_method="llm")
                print(f"Loaded {len(L)} rows for {version} (llm).")
            except Exception as e:
                print(f"LLM ingestion failed for {version}: {e}", file=sys.stderr)

    print("Done. Query trips_canonical with source_version and ingestion_method to compare.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
