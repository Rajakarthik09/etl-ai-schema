"""
Taxi-specific ETL transform: assumes NYC TLC yellow taxi V1 schema.

Business logic to preserve across schema evolution:
  - trip_revenue = fare_amount + tip_amount + tolls_amount
  - trip_duration_minutes from tpep_pickup_datetime and tpep_dropoff_datetime
  - Filter outliers: trip_distance <= 0, fare_amount <= 0 or > 500
"""
import pandas as pd


def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Transform taxi DataFrame (V1 schema). Adds trip_revenue, trip_duration_minutes; filters outliers."""
    df = df.copy()

    # Parse datetimes if needed
    pickup = pd.to_datetime(df["tpep_pickup_datetime"], errors="coerce")
    dropoff = pd.to_datetime(df["tpep_dropoff_datetime"], errors="coerce")
    df["trip_duration_minutes"] = (dropoff - pickup).dt.total_seconds() / 60.0

    # Revenue (handle missing columns gracefully for evolution)
    fare = df["fare_amount"] if "fare_amount" in df.columns else 0
    tip = df["tip_amount"] if "tip_amount" in df.columns else 0
    tolls = df["tolls_amount"] if "tolls_amount" in df.columns else 0
    df["trip_revenue"] = fare + tip + tolls

    # Distance column may be trip_distance (V1) or trip_km (V2) or trip_distance_km (V3)
    dist_col = next((c for c in ["trip_distance", "trip_km", "trip_distance_km"] if c in df.columns), None)
    if dist_col is not None:
        mask = (df[dist_col] > 0) & (df["fare_amount"] > 0) & (df["fare_amount"] <= 500)
        df = df.loc[mask].copy()
    else:
        mask = (df["fare_amount"] > 0) & (df["fare_amount"] <= 500)
        df = df.loc[mask].copy()

    return df
