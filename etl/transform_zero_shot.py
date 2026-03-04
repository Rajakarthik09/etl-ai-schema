import pandas as pd
import numpy as np

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform a NYC‑Yellow‑Taxi V2 DataFrame into the canonical schema.

    Parameters
    ----------
    df : pandas.DataFrame
        Input frame that must contain the V2 columns listed in the prompt.
        Columns may be missing or have the wrong dtype – the function is
        defensive and will coerce types where possible.

    Returns
    -------
    pandas.DataFrame
        DataFrame that contains **exactly** the canonical columns:

        - VendorID
        - tpep_pickup_datetime
        - tpep_dropoff_datetime
        - passenger_count
        - trip_distance
        - fare_amount
        - tip_amount
        - tolls_amount
        - total_amount
        - trip_duration_minutes
        - trip_revenue

        Rows that are clearly implausible (non‑positive distance or negative
        monetary fields) are removed.  Missing values are left as ``np.nan``.
    """
    # ------------------------------------------------------------------ #
    # 1. Make a shallow copy so we never mutate the caller's object.
    # ------------------------------------------------------------------ #
    df = df.copy()

    # ------------------------------------------------------------------ #
    # 2. Ensure the required source columns exist – if they do not, create
    #    them filled with NaN so that the later logic does not raise KeyError.
    # ------------------------------------------------------------------ #
    needed_src = [
        "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
        "passenger_count", "trip_km", "fare_amount", "tip_amount",
        "tolls_amount", "total_amount"
    ]
    for col in needed_src:
        if col not in df.columns:
            df[col] = np.nan

    # ------------------------------------------------------------------ #
    # 3. Rename/standardise columns.
    # ------------------------------------------------------------------ #
    df = df.rename(columns={"trip_km": "trip_distance"})

    # ------------------------------------------------------------------ #
    # 4. Cast/clean data types.
    # ------------------------------------------------------------------ #
    # VendorID is stored as a string in V2 – keep it as string, coerce others.
    df["VendorID"] = df["VendorID"].astype(str)

    # Datetime columns – coerce errors to NaT.
    df["tpep_pickup_datetime"] = pd.to_datetime(
        df["tpep_pickup_datetime"], errors="coerce", utc=False
    )
    df["tpep_dropoff_datetime"] = pd.to_datetime(
        df["tpep_dropoff_datetime"], errors="coerce", utc=False
    )

    # Numeric columns – convert to float (the most tolerant dtype for money).
    numeric_cols = [
        "passenger_count", "trip_distance", "fare_amount",
        "tip_amount", "tolls_amount", "total_amount"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ------------------------------------------------------------------ #
    # 5. Compute derived fields.
    # ------------------------------------------------------------------ #
    # Trip duration in minutes – if either timestamp is missing the result is NaN.
    duration_sec = (df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]).dt.total_seconds()
    df["trip_duration_minutes"] = duration_sec / 60.0

    # Trip revenue = fare + tip + tolls.
    df["trip_revenue"] = df["fare_amount"] + df["tip_amount"] + df["tolls_amount"]

    # ------------------------------------------------------------------ #
    # 6. Filter implausible trips.
    # ------------------------------------------------------------------ #
    #   • distance must be > 0
    #   • monetary fields must be >= 0 (negative fares/tips/tolls are impossible)
    #   • total_amount must be >= 0 as a sanity check
    mask = (
        (df["trip_distance"] > 0) &
        (df["fare_amount"] >= 0) &
        (df["tip_amount"] >= 0) &
        (df["tolls_amount"] >= 0) &
        (df["total_amount"] >= 0)
    )
    # Rows with any NaN in the evaluated columns evaluate to False; they are dropped.
    df = df.loc[mask].reset_index(drop=True)

    # ------------------------------------------------------------------ #
    # 7. Keep **only** the canonical columns (order is not important).
    # ------------------------------------------------------------------ #
    canonical_cols = [
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "fare_amount",
        "tip_amount",
        "tolls_amount",
        "total_amount",
        "trip_duration_minutes",
        "trip_revenue",
    ]
    # If for any reason a canonical column is missing (should not happen after step 2),
    # add it filled with NaN so the output schema is guaranteed.
    for col in canonical_cols:
        if col not in df.columns:
            df[col] = np.nan

    return df[canonical_cols]