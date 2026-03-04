"""
Canonical (single) target schema for NYC taxi trips.

Canonical = exactly V1 column names. V2 (and later) are evolved schemas; their
data is normalized into this V1-shaped schema so a single table holds all versions.
Used by both the baseline algorithm and the LLM mapping regeneration path.
"""
from typing import Dict, List, Any

# V1 column order and names (canonical = V1 exactly). Derived: trip_duration_minutes, trip_revenue.
CANONICAL_COLUMNS: List[str] = [
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


def get_canonical_columns_with_metadata() -> List[str]:
    """Canonical columns in table order (V1 = first and final)."""
    return list(CANONICAL_COLUMNS)


def get_canonical_schema_dict() -> Dict[str, Any]:
    """Return a schema dict in the same format as detect_schema_change output (for LLM prompt)."""
    columns: Dict[str, Dict[str, Any]] = {}
    for col in CANONICAL_COLUMNS:
        if col in ("trip_duration_minutes", "trip_revenue"):
            columns[col] = {"dtype": "float64", "nullable": True, "sample_values": []}
        elif col == "VendorID":
            columns[col] = {"dtype": "int64", "nullable": False, "sample_values": [1, 2]}
        elif "datetime" in col:
            columns[col] = {"dtype": "object", "nullable": False, "sample_values": []}
        elif col == "passenger_count":
            columns[col] = {"dtype": "int64", "nullable": True, "sample_values": []}
        else:
            columns[col] = {"dtype": "float64", "nullable": True, "sample_values": []}
    return {
        "columns": columns,
        "column_count": len(columns),
    }
