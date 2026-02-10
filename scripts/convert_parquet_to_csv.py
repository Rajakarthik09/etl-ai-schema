#!/usr/bin/env python3
"""
Convert NYC TLC yellow taxi Parquet to a downsampled CSV baseline (V1).

Reads data/raw/yellow_tripdata_YYYY-MM.parquet, samples ~100k rows,
writes data/raw/yellow_base_v1.csv. No schema changes; V1 is a clean sample.

Usage:
  python scripts/convert_parquet_to_csv.py
  python scripts/convert_parquet_to_csv.py --input data/raw/yellow_tripdata_2023-01.parquet --rows 100000
"""
import argparse
import os
import sys

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
DEFAULT_ROWS = 100_000


def find_parquet_source():
    """Use yellow_tripdata_2023-01.parquet if present, else first yellow_tripdata_*.parquet."""
    preferred = os.path.join(RAW_DIR, "yellow_tripdata_2023-01.parquet")
    if os.path.isfile(preferred):
        return preferred
    for name in sorted(os.listdir(RAW_DIR or ".")):
        if name.startswith("yellow_tripdata_") and name.endswith(".parquet"):
            return os.path.join(RAW_DIR, name)
    return None


def main():
    parser = argparse.ArgumentParser(description="Convert NYC TLC Parquet to CSV sample (V1).")
    parser.add_argument(
        "--input", "-i",
        default=None,
        help="Input Parquet path (default: yellow_tripdata_2023-01.parquet or first yellow_*.parquet in data/raw)",
    )
    parser.add_argument(
        "--rows", "-n",
        type=int,
        default=DEFAULT_ROWS,
        help=f"Number of rows to sample (default: {DEFAULT_ROWS})",
    )
    parser.add_argument(
        "--output", "-o",
        default=os.path.join(RAW_DIR, "yellow_base_v1.csv"),
        help="Output CSV path",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for sampling",
    )
    args = parser.parse_args()

    input_path = args.input or find_parquet_source()
    if not input_path or not os.path.isfile(input_path):
        print("Error: No Parquet file found. Place yellow_tripdata_YYYY-MM.parquet in data/raw/", file=sys.stderr)
        sys.exit(1)

    os.makedirs(os.path.dirname(args.output) or ".", exist_ok=True)

    import pandas as pd

    print(f"Reading {input_path}...")
    df = pd.read_parquet(input_path, engine="pyarrow")
    n_total = len(df)
    n_sample = min(args.rows, n_total)
    if n_sample < n_total:
        df = df.sample(n=n_sample, random_state=args.seed)
    df.to_csv(args.output, index=False)
    print(f"Wrote {args.output} ({len(df)} rows)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
