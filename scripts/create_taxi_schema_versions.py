#!/usr/bin/env python3
"""
Create taxi CSV versions from real Parquet sources (no synthetic evolution).

- V1 is sampled from data/raw/yellow_tripdata_2015-01.parquet -> data/raw/yellow_base_v1.csv
- V2 is sampled from data/raw/yellow_tripdata_2025-01.parquet -> data/raw/yellow_base_v2.csv
"""
import os
import sys

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
V1_PATH = os.path.join(RAW_DIR, "yellow_base_v1.csv")
V2_PATH = os.path.join(RAW_DIR, "yellow_base_v2.csv")

V1_PARQUET = os.path.join(RAW_DIR, "yellow_tripdata_2015-01.parquet")
V2_PARQUET = os.path.join(RAW_DIR, "yellow_tripdata_2025-01.parquet")
DEFAULT_ROWS = 100_000
DEFAULT_SEED = 42

def _read_and_sample_parquet(path: str, rows: int, seed: int) -> pd.DataFrame:
    df = pd.read_parquet(path, engine="pyarrow")
    if len(df) > rows:
        df = df.sample(n=rows, random_state=seed)
    return df


def main():
    if not os.path.isfile(V1_PARQUET):
        print(f"Error: {V1_PARQUET} not found.", file=sys.stderr)
        sys.exit(1)
    if not os.path.isfile(V2_PARQUET):
        print(f"Error: {V2_PARQUET} not found.", file=sys.stderr)
        sys.exit(1)

    print("Reading V1 parquet:", V1_PARQUET)
    v1 = _read_and_sample_parquet(V1_PARQUET, rows=DEFAULT_ROWS, seed=DEFAULT_SEED)
    v1.to_csv(V1_PATH, index=False)
    print("Wrote", V1_PATH, "rows:", len(v1), "columns:", list(v1.columns))

    print("Reading V2 parquet:", V2_PARQUET)
    v2 = _read_and_sample_parquet(V2_PARQUET, rows=DEFAULT_ROWS, seed=DEFAULT_SEED)
    v2.to_csv(V2_PATH, index=False)
    print("Wrote", V2_PATH, "rows:", len(v2), "columns:", list(v2.columns))

    return 0


if __name__ == "__main__":
    sys.exit(main())
