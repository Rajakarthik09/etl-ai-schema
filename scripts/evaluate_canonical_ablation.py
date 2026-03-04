#!/usr/bin/env python3
"""
Ablation study for canonical ingestion:

Compare a zero-shot LLM mapping (no retrieved schemas or transform snippet)
against the full RAG-based mapping regeneration workflow for the NYC taxi
V2 -> canonical ingestion scenario.

For each run and condition, the script:
  1. Computes the deterministic baseline canonical output B using
     `normalize_to_canonical(df)` from the raw CSV.
  2. Obtains an LLM-based canonical output L via either:
       - RAG condition: regenerate_mapping with schemas + changes + snippet.
       - Zero-shot condition: a plain-language prompt with source/target
         column lists and change description, but no retrieved artefacts.
  3. Compares B vs L using the same metrics as the main evaluation:
       - row counts,
       - cell_agreement_pct,
       - null_mismatch_count,
       - per-column agreement.
  4. Computes additional ablation metrics:
       - code_executability (success/failure),
       - schema_hallucination_count and hallucinated_columns
         (extra columns in L that are not part of the canonical schema).

Results are written as JSON with one entry per run and condition.

Usage examples:
  python scripts/evaluate_canonical_ablation.py --condition both --version v2 --runs 10 --model llama3 --output results/canonical_ablation_llama3.json
"""

import json
import os
import sys
import tempfile
from typing import Dict, List, Tuple

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
ETL_DIR = os.path.join(PROJECT_ROOT, "etl")

sys.path.insert(0, PROJECT_ROOT)

from etl.baseline_to_canonical import normalize_to_canonical
from etl.canonical_schema import (
    get_canonical_columns_with_metadata,
    get_canonical_schema_dict,
)

try:
    from scripts.canonical_schema_support import get_current_schema_for_version
except ImportError:
    from canonical_schema_support import get_current_schema_for_version

from ai.detect_schema_change import SchemaChangeDetector
from ai.regenerate_mapping import MappingRegenerator, regenerate_mapping


def align_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure df has exactly canonical columns in order; fill missing with NA."""
    cols = get_canonical_columns_with_metadata()
    out = pd.DataFrame(index=df.index)
    for c in cols:
        out[c] = df[c] if c in df.columns else pd.NA
    return out[cols]


def compare_dataframes(B: pd.DataFrame, L: pd.DataFrame, version: str) -> Dict:
    """
    Compare baseline output B vs LLM output L. Return metrics dict.

    This mirrors the logic in scripts/evaluate_baseline_vs_llm_canonical.py so
    that the ablation metrics are directly comparable to the main results.
    """
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
        metrics["per_column_agreement_pct"] = {}
        return metrics

    # Compare first n_rows rows, positionally.
    B_sub = B.iloc[:n_rows].reset_index(drop=True)
    L_sub = L.iloc[:n_rows].reset_index(drop=True)

    total_cells = n_rows * len(cols)
    agreement = 0
    null_mismatch = 0
    per_col: Dict[str, float] = {}

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


def compute_schema_hallucinations(df_llm: pd.DataFrame) -> Tuple[int, List[str]]:
    """
    Compute the number of hallucinated columns: columns present in df_llm that
    are not part of the canonical schema.
    """
    canonical_cols = set(get_canonical_columns_with_metadata())
    llm_cols = set(df_llm.columns)
    extras = sorted(llm_cols - canonical_cols)
    return len(extras), extras


def run_rag_path(csv_path: str, version: str, model: str) -> pd.DataFrame:
    """
    RAG condition: regenerate mapping from current schema to canonical using
    JSON schemas, change sets, and the baseline transform snippet, then run
    the generated transform on the CSV and return the canonical DataFrame.
    """
    current_schema, current_schema_path = get_current_schema_for_version(version, csv_path)
    canonical_schema = get_canonical_schema_dict()
    detector = SchemaChangeDetector()
    changes = detector.compare_schemas(current_schema, canonical_schema)

    # Temp files for regenerate_mapping (same contents as main evaluation)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        canonical_path = f.name
        json.dump(canonical_schema, f, indent=2)
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        changes_path = f.name
        json.dump(changes, f, indent=2)

    try:
        snippet_path = os.path.join(ETL_DIR, "transform_taxi.py")
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
    # Some LLM-generated transforms use `pd` without importing pandas; make sure
    # the module has a `pd` binding pointing at pandas.
    if not hasattr(gen, "pd"):
        gen.pd = pd

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


def build_zero_shot_prompt(
    source_columns: List[str],
    target_columns: List[str],
) -> str:
    """
    Build a plain-language prompt for the zero-shot condition. The prompt
    provides only column lists and a natural-language description of the
    schema evolution and business logic, without retrieved JSON schemas,
    change sets, or historical transform code.
    """
    src_cols_str = ", ".join(source_columns)
    tgt_cols_str = ", ".join(target_columns)

    change_desc = (
        "The source schema is NYC yellow taxi V2, derived from the TLC trip record data. "
        "Compared to an earlier version, it renames the distance column from 'trip_distance' "
        "to 'trip_km', adds a derived column 'tip_ratio' (tip_amount / fare_amount with "
        "safe handling of zero fares), removes the column 'payment_type', and stores "
        "'VendorID' as a string instead of an integer. "
    )

    business_logic_desc = (
        "The target canonical table should preserve the original business logic: "
        "compute 'trip_duration_minutes' from the pickup and dropoff timestamps, "
        "compute 'trip_revenue' as the sum of fare_amount, tip_amount, and tolls_amount, "
        "and filter out implausible trips (non-positive distance or clearly invalid fares). "
    )

    instructions = (
        "Using ONLY the column lists and the description above, write a single Python "
        "function:\n\n"
        "    def transform(df: pandas.DataFrame) -> pandas.DataFrame:\n\n"
        "that accepts a DataFrame with the V2 source columns and returns a DataFrame with "
        "EXACTLY the target canonical columns, in any order. Do not invent new columns "
        "or silently drop canonical columns; if some canonical columns cannot be computed, "
        "you may fill them with NA but they must still be present. Use pandas idioms "
        "and handle missing values and type conversions carefully (avoid 'boolean of NA' "
        "errors and errors when converting non-finite values to integers)."
    )

    prompt = (
        "You are an expert Python and ETL engineer. "
        "You are given only high-level information about a schema evolution and "
        "the desired canonical schema.\n\n"
        f"SOURCE COLUMNS (NYC taxi V2):\n{src_cols_str}\n\n"
        f"TARGET CANONICAL COLUMNS:\n{tgt_cols_str}\n\n"
        f"SCENARIO:\n{change_desc}\n\n"
        f"BUSINESS LOGIC:\n{business_logic_desc}\n\n"
        f"TASK:\n{instructions}\n"
    )

    return prompt


def run_zero_shot_path(csv_path: str, version: str, model: str) -> pd.DataFrame:
    """
    Zero-shot condition: call the LLM with a plain-language prompt that includes
    only source/target column lists and a natural-language change description,
    but no retrieved JSON schemas, change sets, or historical transform code.
    """
    # Discover source columns from the CSV header.
    df_head = pd.read_csv(csv_path, nrows=0)
    source_cols = list(df_head.columns)
    target_cols = get_canonical_columns_with_metadata()

    prompt = build_zero_shot_prompt(source_cols, target_cols)

    reg = MappingRegenerator()
    code = reg.call_ai(prompt, model=model)

    # Heuristic: ensure pandas is imported if the generated code uses `pd`.
    if "import pandas" not in code and "pd." in code:
        code = "import pandas as pd\n\n" + code

    zero_shot_path = os.path.join(ETL_DIR, "transform_zero_shot.py")
    with open(zero_shot_path, "w") as f:
        f.write(code)

    df = pd.read_csv(csv_path)
    import importlib
    import etl.transform_zero_shot as zs

    importlib.reload(zs)
    if not hasattr(zs, "pd"):
        zs.pd = pd

    fn = getattr(zs, "transform", None)
    if fn is None:
        candidate = f"transform_{version}"
        fn = getattr(zs, candidate, None)
    if fn is None:
        raise AttributeError(
            f"etl.transform_zero_shot has no 'transform' or 'transform_{version}' function"
        )

    L = fn(df)
    return L


def main() -> int:
    import argparse

    parser = argparse.ArgumentParser(
        description="Ablation study: zero-shot LLM vs RAG-based mapping for canonical ingestion."
    )
    parser.add_argument(
        "--condition",
        choices=["zero_shot", "rag", "both"],
        default="both",
        help="Which LLM condition(s) to evaluate.",
    )
    parser.add_argument(
        "--version",
        default="v2",
        help="Taxi version to evaluate (default: v2).",
    )
    parser.add_argument(
        "--model",
        default="llama3",
        help="LLM model to use (e.g. llama3, mistral, mistral-small-latest).",
    )
    parser.add_argument(
        "--runs",
        type=int,
        default=5,
        help="Number of independent runs per condition.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional path to write JSON results.",
    )
    args = parser.parse_args()

    version = args.version
    csv_path = os.path.join(RAW_DIR, f"yellow_base_{version}.csv")
    if not os.path.isfile(csv_path):
        print(f"Error: {csv_path} not found.", file=sys.stderr)
        return 1

    df = pd.read_csv(csv_path)
    B = normalize_to_canonical(df)

    results = []

    for run_idx in range(1, args.runs + 1):
        if args.condition in ("rag", "both"):
            entry = {"run": run_idx, "condition": "rag"}
            try:
                L_rag = run_rag_path(csv_path, version, args.model)
                L_rag_aligned = align_to_canonical(L_rag)
                metrics = compare_dataframes(B, L_rag_aligned, version)
                hall_count, hall_cols = compute_schema_hallucinations(L_rag)
                metrics["schema_hallucination_count"] = hall_count
                metrics["schema_hallucinated_columns"] = hall_cols
                metrics["code_executability"] = True
            except Exception as e:
                metrics = {
                    "version": version,
                    "error": str(e),
                    "baseline_row_count": len(B),
                    "llm_row_count": 0,
                    "row_count_match": False,
                    "cell_agreement_pct": 0.0,
                    "null_mismatch_count": 0,
                    "per_column_agreement_pct": {},
                    "schema_hallucination_count": None,
                    "schema_hallucinated_columns": [],
                    "code_executability": False,
                }
            entry["metrics"] = metrics
            results.append(entry)
            print(json.dumps(entry, indent=2))

        if args.condition in ("zero_shot", "both"):
            entry = {"run": run_idx, "condition": "zero_shot"}
            try:
                L_zero = run_zero_shot_path(csv_path, version, args.model)
                L_zero_aligned = align_to_canonical(L_zero)
                metrics = compare_dataframes(B, L_zero_aligned, version)
                hall_count, hall_cols = compute_schema_hallucinations(L_zero)
                metrics["schema_hallucination_count"] = hall_count
                metrics["schema_hallucinated_columns"] = hall_cols
                metrics["code_executability"] = True
            except Exception as e:
                metrics = {
                    "version": version,
                    "error": str(e),
                    "baseline_row_count": len(B),
                    "llm_row_count": 0,
                    "row_count_match": False,
                    "cell_agreement_pct": 0.0,
                    "null_mismatch_count": 0,
                    "per_column_agreement_pct": {},
                    "schema_hallucination_count": None,
                    "schema_hallucinated_columns": [],
                    "code_executability": False,
                }
            entry["metrics"] = metrics
            results.append(entry)
            print(json.dumps(entry, indent=2))

    if args.output:
        with open(args.output, "w") as f:
            json.dump({"results": results}, f, indent=2)
        print(f"Wrote {args.output}")

    return 0


if __name__ == "__main__":
    sys.exit(main())

