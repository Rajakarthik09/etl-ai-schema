#!/usr/bin/env python3
"""
Quantitative evaluation: compare baseline algorithm vs LLM mapping regeneration
for ingesting NYC taxi data into the single canonical table.

For each discovered schema version (e.g. v1, v2):
  1. Run baseline: normalize_to_canonical(df) -> B
  2. Run LLM path: regenerate mapping (current -> canonical), then generated transform(df) -> L
  3. Align L to canonical columns (fill missing with NA, drop extra)
  4. Compare B and L: row counts, cell agreement, null mismatches

Output: JSON with per-version and aggregate metrics (no hallucinations; real numbers).
"""
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
from etl.baseline_to_canonical import normalize_to_canonical
from etl.canonical_schema import get_canonical_columns_with_metadata, get_canonical_schema_dict

try:
    from scripts.canonical_schema_support import discover_versions, get_current_schema_for_version
except ImportError:
    from canonical_schema_support import discover_versions, get_current_schema_for_version


def align_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure df has exactly canonical columns in order; fill missing with NA."""
    cols = get_canonical_columns_with_metadata()
    out = pd.DataFrame(index=df.index)
    for c in cols:
        out[c] = df[c] if c in df.columns else pd.NA
    return out[cols]


def compare_dataframes(B: pd.DataFrame, L: pd.DataFrame, version: str) -> dict:
    """Compare baseline output B vs LLM output L. Return metrics dict."""
    cols = get_canonical_columns_with_metadata()
    B = align_to_canonical(B)
    L = align_to_canonical(L)

    n_b, n_l = len(B), len(L)
    row_count_match = n_b == n_l

    # Cell-level comparison: only over rows that exist in both (min length)
    n_rows = min(n_b, n_l)
    metrics = {
        "version": version,
        "baseline_row_count": n_b,
        "llm_row_count": n_l,
        "row_count_match": row_count_match,
        "rows_compared": n_rows,
    }

    if n_rows == 0:
        metrics["cell_agreement_pct"] = 0.0
        metrics["null_mismatch_count"] = 0
        metrics["per_column_agreement"] = {}
        return metrics

    # Compare first n_rows rows, positionally. Reset index on both so that
    # elementwise operations (&, ==) do not rely on matching index labels.
    B_sub = B.iloc[:n_rows].reset_index(drop=True)
    L_sub = L.iloc[:n_rows].reset_index(drop=True)

    total_cells = n_rows * len(cols)
    agreement = 0
    null_mismatch = 0
    per_col = {}

    for c in cols:
        b_vals = B_sub[c]
        l_vals = L_sub[c]
        # Agreement: both null or both equal
        both_null = b_vals.isna() & l_vals.isna()
        both_eq = (b_vals == l_vals) & b_vals.notna()
        col_agree = (both_null | both_eq).sum()
        agreement += col_agree
        per_col[c] = round(100.0 * col_agree / n_rows, 2)
        # Null mismatch: one null other not
        one_null = b_vals.isna() != l_vals.isna()
        null_mismatch += one_null.sum()

    metrics["cell_agreement_pct"] = round(100.0 * agreement / total_cells, 2)
    metrics["null_mismatch_count"] = int(null_mismatch)
    metrics["per_column_agreement_pct"] = per_col

    return metrics


def run_llm_path(csv_path: str, version: str, model: str) -> pd.DataFrame:
    """
    Regenerate mapping from current schema to canonical, run generated transform on CSV, return canonical DataFrame.
    """
    from ai.regenerate_mapping import regenerate_mapping
    from ai.detect_schema_change import SchemaChangeDetector

    current_schema, current_schema_path = get_current_schema_for_version(version, csv_path)
    canonical_schema = get_canonical_schema_dict()
    detector = SchemaChangeDetector()
    changes = detector.compare_schemas(current_schema, canonical_schema)

    # Save canonical changes for debugging (what we pass to the LLM)
    results_dir = os.path.join(PROJECT_ROOT, "results")
    os.makedirs(results_dir, exist_ok=True)
    canonical_changes_path = os.path.join(results_dir, f"canonical_changes_{version}.json")
    with open(canonical_changes_path, "w") as f:
        json.dump(changes, f, indent=2)
    # Temp file for regenerate_mapping (same contents)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        canonical_path = f.name
        json.dump(canonical_schema, f, indent=2)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        changes_path = f.name
        json.dump(changes, f, indent=2)

    try:
        etl_dir = os.path.join(PROJECT_ROOT, "etl")
        snippet_path = os.path.join(etl_dir, "transform_taxi.py")
        with open(snippet_path, "r") as f:
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
    # Prefer a generic `transform` function if present; otherwise fall back
    # to a version-specific name such as `transform_v1`, `transform_v2`, etc.
    fn = getattr(gen, "transform", None)
    if fn is None:
        candidate = f"transform_{version}"
        fn = getattr(gen, candidate, None)
    if fn is None:
        raise AttributeError(
            f"etl.transform_generated has no 'transform' or 'transform_{version}' function"
        )
    L = fn(df)
    return L


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Evaluate baseline vs LLM for canonical ingestion.")
    parser.add_argument("--version", default=None, help="Run only this version (e.g. v1, v4, v5). Default: all discovered from data/raw/yellow_base_*.csv.")
    parser.add_argument("--model", default="llama3", help="LLM model: llama3, mistral, mistral-small-latest, etc.")
    parser.add_argument("--skip-llm", action="store_true", help="Only run baseline; compare to self (for testing).")
    parser.add_argument("--output", default=None, help="Write JSON metrics to this file.")
    args = parser.parse_args()

    versions = [args.version] if args.version else discover_versions()
    if not versions:
        print("No versions found. Add data/raw/yellow_base_<version>.csv or pass --version.", file=sys.stderr)
        return 1
    results = []

    for version in versions:
        csv_path = os.path.join(RAW_DIR, f"yellow_base_{version}.csv")
        if not os.path.isfile(csv_path):
            print(f"Skipping {version}: {csv_path} not found.", file=sys.stderr)
            continue

        df = pd.read_csv(csv_path)
        B = normalize_to_canonical(df)

        if args.skip_llm:
            L = B.copy()
            metrics = compare_dataframes(B, L, version)
            metrics["note"] = "skip_llm: L = B"
        else:
            try:
                L = run_llm_path(csv_path, version, args.model)
                L = align_to_canonical(L)
                metrics = compare_dataframes(B, L, version)
            except Exception as e:
                metrics = {
                    "version": version,
                    "error": str(e),
                    "baseline_row_count": len(B),
                    "llm_row_count": 0,
                    "row_count_match": False,
                    "cell_agreement_pct": 0.0,
                    "null_mismatch_count": 0,
                }
        results.append(metrics)
        print(json.dumps(metrics, indent=2))

    if args.output:
        with open(args.output, "w") as f:
            json.dump({"by_version": results}, f, indent=2)
        print(f"Wrote {args.output}")

    # If we attempted all versions and every one failed with an error, surface this
    # as a non-zero exit code so orchestrators (e.g. Airflow) mark the task as failed.
    has_results = bool(results)
    all_error = has_results and all("error" in r for r in results)
    if all_error:
        print("All baseline vs LLM canonical evaluations failed; see 'error' fields above.", file=sys.stderr)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
