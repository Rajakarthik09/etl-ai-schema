#!/usr/bin/env python3
"""
Convert NYC TLC yellow taxi Parquet files to downsampled CSV baselines (V1, V2).

By default:
- V1 is sampled from data/raw/yellow_tripdata_2015-01.parquet -> data/raw/yellow_base_v1.csv
- V2 is sampled from data/raw/yellow_tripdata_2025-01.parquet -> data/raw/yellow_base_v2.csv

No schema changes are applied; versions reflect the underlying Parquet schemas.

Usage:
  python scripts/convert_parquet_to_csv.py
  python scripts/convert_parquet_to_csv.py --rows 100000
  python scripts/convert_parquet_to_csv.py --v1-input data/raw/yellow_tripdata_2015-01.parquet --v2-input data/raw/yellow_tripdata_2025-01.parquet
  python scripts/convert_parquet_to_csv.py --only v1
"""
import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
DEFAULT_ROWS = 100_000


DEFAULT_V1_PARQUET = os.path.join(RAW_DIR, "yellow_tripdata_2015-01.parquet")
DEFAULT_V2_PARQUET = os.path.join(RAW_DIR, "yellow_tripdata_2025-01.parquet")
DEFAULT_V1_CSV = os.path.join(RAW_DIR, "yellow_base_v1.csv")
DEFAULT_V2_CSV = os.path.join(RAW_DIR, "yellow_base_v2.csv")


def _read_and_sample_parquet(input_path: str, rows: int, seed: int):
    import pandas as pd

    df = pd.read_parquet(input_path, engine="pyarrow")
    n_total = len(df)
    n_sample = min(rows, n_total)
    if n_sample < n_total:
        df = df.sample(n=n_sample, random_state=seed)
    return df


def main():
    parser = argparse.ArgumentParser(description="Convert NYC TLC Parquet to CSV samples (V1/V2).")
    parser.add_argument(
        "--input", "-i",
        default=None,
        help="(Deprecated) Alias for --v1-input",
    )
    parser.add_argument(
        "--v1-input",
        default=None,
        help=f"Input Parquet for V1 (default: {os.path.basename(DEFAULT_V1_PARQUET)})",
    )
    parser.add_argument(
        "--v2-input",
        default=None,
        help=f"Input Parquet for V2 (default: {os.path.basename(DEFAULT_V2_PARQUET)})",
    )
    parser.add_argument(
        "--rows", "-n",
        type=int,
        default=DEFAULT_ROWS,
        help=f"Number of rows to sample (default: {DEFAULT_ROWS})",
    )
    parser.add_argument(
        "--v1-output",
        default=DEFAULT_V1_CSV,
        help="Output CSV path for V1",
    )
    parser.add_argument(
        "--v2-output",
        default=DEFAULT_V2_CSV,
        help="Output CSV path for V2",
    )
    parser.add_argument(
        "--only",
        choices=["v1", "v2", "both"],
        default="both",
        help="Which outputs to generate (default: both)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for sampling",
    )
    args = parser.parse_args()

    v1_input = args.input or args.v1_input or DEFAULT_V1_PARQUET
    v2_input = args.v2_input or DEFAULT_V2_PARQUET

    if args.only in ("v1", "both"):
        if not os.path.isfile(v1_input):
            print(f"Error: V1 Parquet not found: {v1_input}", file=sys.stderr)
            sys.exit(1)
        os.makedirs(os.path.dirname(args.v1_output) or ".", exist_ok=True)
        print(f"Reading V1 {v1_input}...")
        df1 = _read_and_sample_parquet(v1_input, rows=args.rows, seed=args.seed)
        df1.to_csv(args.v1_output, index=False)
        print(f"Wrote {args.v1_output} ({len(df1)} rows)")

    if args.only in ("v2", "both"):
        if not os.path.isfile(v2_input):
            print(f"Error: V2 Parquet not found: {v2_input}", file=sys.stderr)
            sys.exit(1)
        os.makedirs(os.path.dirname(args.v2_output) or ".", exist_ok=True)
        print(f"Reading V2 {v2_input}...")
        df2 = _read_and_sample_parquet(v2_input, rows=args.rows, seed=args.seed)
        df2.to_csv(args.v2_output, index=False)
        print(f"Wrote {args.v2_output} ({len(df2)} rows)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
