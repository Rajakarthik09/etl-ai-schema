# Phase 5 Evaluation Protocol: Manual vs AI Schema Evolution

This document defines how to run the **manual vs AI** adaptation experiments for the NYC taxi schema evolution scenarios. It aligns with Phase 5 (Testing & Validation) in [THESIS_ROADMAP.md](../THESIS_ROADMAP.md).

## Scenarios

| Scenario   | Old CSV              | New CSV              | Schema changes (summary) |
|-----------|----------------------|----------------------|---------------------------|
| V1 → V2   | yellow_base_v1.csv   | yellow_base_v2.csv   | Rename trip_distance→trip_km; add tip_ratio; drop payment_type; VendorID→string |
| V2 → V3   | yellow_base_v2.csv   | yellow_base_v3.csv   | Rename trip_km→trip_distance_km; add pickup_info, time_of_day |

## Metrics to Record

For each scenario and each condition (Manual, AI), record:

- **T_manual / T_AI**: Elapsed time (minutes or seconds) from start of adaptation to pipeline run success.
- **Success (Y/N)**: Pipeline runs without error and load completes.
- **Correctness notes**: Derived columns present (`trip_revenue`, `trip_duration_minutes`), no obvious wrong values (e.g. empty output, negative revenue).
- **Notes**: Any manual fixes applied (e.g. one-line fix to generated code).

## Results Table (template)

| Scenario | # Added | # Removed | # Renamed | T_manual (min) | T_AI (min) | Success (Manual) | Success (AI) | Notes |
|----------|---------|-----------|-----------|----------------|------------|------------------|--------------|-------|
| V1 → V2  | 1       | 1         | 1         |                |            |                  |              |       |
| V2 → V3  | 2       | 0         | 1         |                |            |                  |              |       |

## Manual condition procedure

1. Start timer.
2. Open the **new** CSV (or schema JSON) and the current transform (e.g. `etl/transform_taxi.py`).
3. Hand-edit the transform (or a copy) to support the new schema: handle renames, dropped columns, new columns, type changes; preserve trip_revenue, trip_duration_minutes, and outlier filtering.
4. Run the pipeline for the new version, e.g. from `etl/`: `python pipeline.py taxi v2` (or v3).
5. Stop timer when the pipeline completes successfully. Record time and success.

## AI condition procedure

1. Start timer.
2. Run schema detection (if not already done): `python scripts/run_taxi_schema_detection.py`.
3. Run AI regeneration for the scenario:  
   `python scripts/run_taxi_regenerate_mapping.py --scenario v1_to_v2` (or `v2_to_v3`).
4. Run the pipeline for the new version using the AI-generated transform: from `etl/`, run `python pipeline.py taxi v2 --generated` (or `v3 --generated`). This uses `transform_generated.py` instead of `transform_taxi.py`. If the generated code has minor errors, fix them and count that time as part of T_AI.
6. Stop timer when the pipeline completes successfully. Record time and success.

## Correctness checks (after pipeline run)

- Query or inspect the loaded table (e.g. SQLite `yellow_trips_v2`):
  - Row count > 0.
  - Columns `trip_revenue` and `trip_duration_minutes` exist and have plausible values (e.g. trip_revenue ≥ 0, duration non-negative).
  - No unexpected nulls in key columns.

## Cross-reference with THESIS_ROADMAP.md

- **5.1 Unit Tests**: Schema detection accuracy is exercised by `scripts/run_taxi_schema_detection.py` and optional `test_schema_detection.py` taxi cases. AI mapping generation is exercised by `scripts/run_taxi_regenerate_mapping.py`.
- **5.2 Integration Tests**: End-to-end workflow = detection → regeneration → pipeline run per scenario above.
- **5.3 Validation Framework**: Data quality checks = correctness checks above; pipeline correctness = success flag and notes.

Success metrics from the roadmap (e.g. time reduction vs manual, AI mapping success rate) can be computed from the results table after both conditions are run for each scenario.
