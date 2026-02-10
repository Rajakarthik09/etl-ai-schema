#!/usr/bin/env python3
"""
Create controlled schema evolution versions (V2, V3) from NYC taxi baseline V1.

Schema changes (documented for thesis):

V1 -> V2 (moderate):
  - Rename: trip_distance -> trip_km
  - Add: tip_ratio = tip_amount / fare_amount (0 where fare_amount is 0)
  - Drop: payment_type
  - Type change: VendorID int -> string

V2 -> V3 (more complex):
  - Rename: trip_km -> trip_distance_km
  - Add: pickup_info = PULocationID | tpep_pickup_datetime (compound field)
  - Add: time_of_day (bucket: morning/afternoon/evening/night from pickup hour)
"""
import os
import sys

import pandas as pd

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RAW_DIR = os.path.join(PROJECT_ROOT, "data", "raw")
V1_PATH = os.path.join(RAW_DIR, "yellow_base_v1.csv")
V2_PATH = os.path.join(RAW_DIR, "yellow_base_v2.csv")
V3_PATH = os.path.join(RAW_DIR, "yellow_base_v3.csv")


def create_v2_from_v1(df: pd.DataFrame) -> pd.DataFrame:
    """V1 -> V2: rename trip_distance, add tip_ratio, drop payment_type, VendorID -> string."""
    df = df.copy()
    # Rename
    df = df.rename(columns={"trip_distance": "trip_km"})
    # Add derived
    df["tip_ratio"] = df["tip_amount"] / df["fare_amount"].replace(0, float("nan"))
    df["tip_ratio"] = df["tip_ratio"].fillna(0)
    # Drop
    df = df.drop(columns=["payment_type"])
    # Type change
    df["VendorID"] = df["VendorID"].astype(str)
    return df


def create_v3_from_v2(df: pd.DataFrame) -> pd.DataFrame:
    """V2 -> V3: rename trip_km, add pickup_info and time_of_day."""
    df = df.copy()
    # Rename
    df = df.rename(columns={"trip_km": "trip_distance_km"})
    # Compound field
    df["pickup_info"] = (
        df["PULocationID"].astype(str) + "|" + df["tpep_pickup_datetime"].astype(str)
    )
    # Time-of-day bucket from pickup
    if pd.api.types.is_datetime64_any_dtype(df["tpep_pickup_datetime"]):
        hour = df["tpep_pickup_datetime"].dt.hour
    else:
        hour = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce").dt.hour
    df["time_of_day"] = pd.cut(
        hour.fillna(12),
        bins=[-0.1, 6, 12, 18, 24],
        labels=["night", "morning", "afternoon", "evening"],
    ).astype(str)
    return df


def main():
    if not os.path.isfile(V1_PATH):
        print(f"Error: {V1_PATH} not found. Run scripts/convert_parquet_to_csv.py first.", file=sys.stderr)
        sys.exit(1)

    print("Reading", V1_PATH)
    v1 = pd.read_csv(V1_PATH, nrows=None)
    # Parse datetimes for V3 time_of_day
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        if col in v1.columns:
            v1[col] = pd.to_datetime(v1[col], errors="coerce")

    v2 = create_v2_from_v1(v1)
    v2.to_csv(V2_PATH, index=False)
    print("Wrote", V2_PATH, "columns:", list(v2.columns))

    v3 = create_v3_from_v2(v2)
    v3.to_csv(V3_PATH, index=False)
    print("Wrote", V3_PATH, "columns:", list(v3.columns))

    return 0


if __name__ == "__main__":
    sys.exit(main())
