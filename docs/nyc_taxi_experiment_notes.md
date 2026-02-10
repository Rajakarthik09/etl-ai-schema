# NYC Taxi Schema Evolution Experiment – Notes for Thesis

## Data source

- **Base data:** NYC TLC Yellow Taxi trip records. Parquet file(s) in `data/raw/` (e.g. `yellow_tripdata_2015-01.parquet` or `yellow_tripdata_2023-01.parquet`). Official source: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
- **V1 baseline:** 100,000-row CSV sample produced by `scripts/convert_parquet_to_csv.py`, saved as `data/raw/yellow_base_v1.csv`. No schema changes; column set matches the chosen Parquet file.

## Controlled schema evolution (for thesis methods)

Schema changes are applied in code so that evolution is reproducible and classifiable (add/remove/rename/type change). Implemented in `scripts/create_taxi_schema_versions.py`.

### V1 → V2 (moderate)

| Change type   | Detail |
|---------------|--------|
| Rename        | `trip_distance` → `trip_km` |
| Add           | `tip_ratio` = tip_amount / fare_amount (0 when fare_amount is 0) |
| Remove        | `payment_type` dropped |
| Type change   | `VendorID` from integer to string |

Output: `data/raw/yellow_base_v2.csv`.

### V2 → V3 (more complex)

| Change type   | Detail |
|---------------|--------|
| Rename        | `trip_km` → `trip_distance_km` |
| Add           | `pickup_info` = PULocationID \| tpep_pickup_datetime (compound string) |
| Add           | `time_of_day` = bucket from pickup hour (night/morning/afternoon/evening) |

Output: `data/raw/yellow_base_v3.csv`.

## Business logic preserved across versions

The taxi ETL transform (baseline: `etl/transform_taxi.py`) defines the logic that manual and AI adaptation must preserve:

- **trip_revenue** = fare_amount + tip_amount + tolls_amount
- **trip_duration_minutes** = (tpep_dropoff_datetime − tpep_pickup_datetime) in minutes
- **Outlier filter:** drop rows where trip_distance (or trip_km / trip_distance_km) ≤ 0, or fare_amount ≤ 0 or > 500

Distance column name differs by version (trip_distance, trip_km, trip_distance_km); the transform handles all three.

## Artifacts produced by the pipeline

| Artifact | Path / command |
|----------|----------------|
| V1 CSV   | `data/raw/yellow_base_v1.csv` |
| V2 CSV   | `data/raw/yellow_base_v2.csv` |
| V3 CSV   | `data/raw/yellow_base_v3.csv` |
| Schemas  | `schemas/yellow_v1_schema.json`, `yellow_v2_schema.json`, `yellow_v3_schema.json` |
| Changes  | `schemas/yellow_v1_to_v2_changes.json`, `yellow_v2_to_v3_changes.json`, `yellow_v1_to_v3_changes.json` |
| Detection| `python scripts/run_taxi_schema_detection.py` |
| AI mapping | `python scripts/run_taxi_regenerate_mapping.py --scenario v1_to_v2` or `v2_to_v3` → `etl/transform_generated.py` |
| ETL run  | From `etl/`: `python pipeline.py taxi v1` (or v2, v3); loads to SQLite tables `yellow_trips_v1`, `yellow_trips_v2`, `yellow_trips_v3` |

## Thesis wording suggestion

For the methods chapter you can state that schema evolution is **controlled but realistic**: the base data are genuine NYC TLC yellow taxi records, and the applied changes (column renames, additions, removals, type changes, and compound fields) mirror patterns seen in real ETL settings. This design allows systematic comparison of manual vs AI-driven mapping adaptation (see [phase5_evaluation_protocol.md](phase5_evaluation_protocol.md)).
