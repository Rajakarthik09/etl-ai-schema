"""
Baseline algorithm: normalize any supported NYC taxi schema (e.g. V1, V2) into the
canonical (V1) schema without using any LLM.

- Canonical = exactly V1 column names (VendorID, trip_distance, etc.).
- V2 is the evolved schema; we map its columns into V1 names (e.g. trip_km -> trip_distance).
- We compute trip_duration_minutes and trip_revenue; same outlier filter for consistency.

Used for quantitative comparison: LLM-generated mappings are evaluated against
this baseline (same input -> baseline output vs LLM output).
"""
import pandas as pd
from .canonical_schema import CANONICAL_COLUMNS


def _get_distance_series(df: pd.DataFrame) -> pd.Series:
    """Resolve trip_distance from trip_distance / trip_km / trip_distance_km if present."""
    for c in ["trip_distance", "trip_km", "trip_distance_km"]:
        if c in df.columns:
            return pd.to_numeric(df[c], errors="coerce")
    return pd.Series([float("nan")] * len(df))


def normalize_to_canonical(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize a raw taxi DataFrame (e.g. V1 or V2 schema) into the canonical schema.

    Algorithm:
    1. Copy dataframe; parse datetimes.
    2. Build canonical (V1) columns: VendorID pass-through; trip_distance from
       trip_distance / trip_km / trip_distance_km; pass-through for datetime, passenger_count, fare, tip, tolls, total.
    3. Compute trip_duration_minutes and trip_revenue.
    4. Apply outlier filter (distance > 0, fare > 0 and <= 500).
    5. Return DataFrame with only canonical (V1) columns in fixed order.

    Returns:
        DataFrame with exactly CANONICAL_COLUMNS.
    """
    df = df.copy()

    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    # Build canonical columns explicitly
    out = pd.DataFrame(index=df.index)

    out["VendorID"] = pd.to_numeric(df["VendorID"], errors="coerce").fillna(0).astype("int64") if "VendorID" in df.columns else 0
    out["tpep_pickup_datetime"] = df["tpep_pickup_datetime"] if "tpep_pickup_datetime" in df.columns else pd.NaT
    out["tpep_dropoff_datetime"] = df["tpep_dropoff_datetime"] if "tpep_dropoff_datetime" in df.columns else pd.NaT
    out["passenger_count"] = df["passenger_count"] if "passenger_count" in df.columns else pd.NA
    out["trip_distance"] = _get_distance_series(df)
    out["fare_amount"] = df["fare_amount"] if "fare_amount" in df.columns else pd.NA
    out["tip_amount"] = df["tip_amount"] if "tip_amount" in df.columns else pd.NA
    out["tolls_amount"] = df["tolls_amount"] if "tolls_amount" in df.columns else pd.NA
    out["total_amount"] = df["total_amount"] if "total_amount" in df.columns else pd.NA

    pickup = out["tpep_pickup_datetime"]
    dropoff = out["tpep_dropoff_datetime"]
    out["trip_duration_minutes"] = (dropoff - pickup).dt.total_seconds() / 60.0

    fare = out["fare_amount"].fillna(0)
    tip = out["tip_amount"].fillna(0)
    tolls = out["tolls_amount"].fillna(0)
    out["trip_revenue"] = fare + tip + tolls

    # Outlier filter
    mask = (
        (out["trip_distance"] > 0)
        & (out["fare_amount"] > 0)
        & (out["fare_amount"] <= 500)
    )
    out = out.loc[mask].copy()

    return out[CANONICAL_COLUMNS].reset_index(drop=True)
