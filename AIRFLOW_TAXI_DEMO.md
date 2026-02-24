## Airflow Taxi Demo Guide

This guide explains how to run the NYC yellow taxi schema-evolution demo using Airflow, so you can show:

- how the pipeline **detects schema changes** between taxi schema versions, and
- how the **AI-generated mapping** still preserves your business-level metrics after evolution.

The demo uses the DAG `taxi_integrated_etl_with_ai` defined in `airflow_home/dags/taxi_integrated_etl_dag.py`.

---

### 1. One-time setup

1. **Install dependencies** (from project root, if not already done):

   ```bash
   pip install -r requirements.txt
   ```

   Make sure `sqlalchemy`, `pandas`, and `pyarrow` are installed.

2. **Download TLC yellow taxi Parquet file(s)**:

   - Place at least one file such as `yellow_tripdata_2015-01.parquet` into:

     ```
     data/raw/
     ```

3. **Generate taxi CSV versions (V1, V2, V3)**:

   From project root:

   ```bash
   python scripts/convert_parquet_to_csv.py --rows 100000
   python scripts/create_taxi_schema_versions.py
   ```

   This creates:

   - `data/raw/yellow_base_v1.csv`
   - `data/raw/yellow_base_v2.csv`
   - `data/raw/yellow_base_v3.csv`

4. **Optional: pre-run schema detection and AI mapping from CLI** (good for debugging before Airflow):

   ```bash
   # Generate schemas and change JSONs for all taxi versions
   python scripts/run_taxi_schema_detection.py

   # Generate AI mapping for V1 -> V2 to etl/transform_generated.py
   export OPENAI_API_KEY=...  # set your key
   python scripts/run_taxi_regenerate_mapping.py --scenario v1_to_v2
   ```

---

### 2. Start Airflow

From project root:

```bash
./start_airflow_standalone.sh
```

This uses `airflow_home/` as `AIRFLOW_HOME` and starts the webserver and scheduler.

Open the Airflow UI in your browser (default: `http://localhost:8080`) and log in with the credentials printed by the script.

---

### 3. Enable and run the taxi DAG

1. In the Airflow UI, go to **DAGs**.
2. Find the DAG named:

   - `taxi_integrated_etl_with_ai`

3. Turn the DAG **On**.
4. Trigger it manually:
   - Click the play/trigger button, or select the DAG and choose “Trigger DAG”.

The DAG will run the following steps:

1. `detect_schema_changes`
2. `generate_ai_mapping`
3. `extract_taxi`
4. `transform_taxi`
5. `load_taxi`

---

### 4. Visualising schema evolution in the Airflow UI

1. Click into the `taxi_integrated_etl_with_ai` DAG run.
2. Open the **`detect_schema_changes`** task and view its **Log**:
   - You will see:
     - the old and new CSV paths (`yellow_base_v1.csv` and `yellow_base_v2.csv`),
     - number of added/removed/renamed/type-changed columns,
     - severity and whether migration is required.
   - These logs correspond directly to the schema changes summarised in the LaTeX tables in the methodology chapter.

3. If schema changes are detected, check the **`generate_ai_mapping`** task log:
   - It will confirm that:
     - `schemas/yellow_v1_schema.json`, `yellow_v2_schema.json`, and `yellow_v1_to_v2_changes.json` were found,
     - `etl/transform_generated.py` was written using the taxi business logic as a reference.

This gives you a clear, inspectable record of the detected schema evolution and the AI-based reaction.

---

### 5. What the DAG loads into SQLite

The final `load_taxi` task:

- Reads the transformed taxi data (`transformed_taxi.pkl`),
- Calls `etl.load.load(df, table_name="yellow_trips_v2")`,
- Writes into:

```text
data/database.db   # SQLite file
  └── table: yellow_trips_v2
```

You can inspect this table using:

- A SQLite GUI (e.g. DB Browser for SQLite), or
- A small notebook:

```python
import pandas as pd
from sqlalchemy import create_engine

engine = create_engine("sqlite:///data/database.db")
df = pd.read_sql("SELECT * FROM yellow_trips_v2 LIMIT 100", engine)
df.head()
```

Key columns to show your professor (from `etl/transform_taxi.py` logic):

- `trip_revenue`
- `trip_duration_minutes`
- plus the original TLC fields.

---

### 6. Demonstrating “before vs after” evolution

To show that the **business-level results are consistent** despite schema evolution:

1. **Baseline run (no AI mapping)**:
   - Temporarily remove or rename `etl/transform_generated.py` so only `etl/transform_taxi.py` exists.
   - Trigger the `taxi_integrated_etl_with_ai` DAG.
   - This will:
     - Still run detection,
     - Skip AI mapping if you like (or you can disable that task),
     - Use the baseline taxi transform and load into `yellow_trips_v2`.
   - Save a copy of `yellow_trips_v2` (or export a subset) as your **baseline**.

2. **AI-assisted run**:
   - Ensure `OPENAI_API_KEY` is set.
   - Trigger the DAG again with `generate_ai_mapping` enabled.
   - This time, if schema changes are detected, the DAG will write `etl/transform_generated.py` and `transform_taxi` logic will be reflected in the AI-generated transform.
   - The `transform_taxi` step will prefer `transform_generated` if present.
   - The result is again loaded into `yellow_trips_v2` (you can export or query it separately as the **AI** version).

3. **Compare results**:
   - Use a notebook or SQL to compute summary statistics on key metrics for both versions:

   ```python
   import pandas as pd
   from sqlalchemy import create_engine

   engine = create_engine("sqlite:///data/database.db")
   df = pd.read_sql("SELECT * FROM yellow_trips_v2", engine)

   df["trip_revenue"].describe()
   df["trip_duration_minutes"].describe()
   ```

   - For a stricter comparison, you can:
     - Export the baseline and AI runs into separate CSVs (e.g. by copying the table between runs), or
     - Add a version flag column in the ETL before load.
   - In the thesis, you mainly need to show that distributions of `trip_revenue` and `trip_duration_minutes` are stable across manual and AI-assisted adaptations.

---

### 7. Recap for a live demo

In front of your professor, you can:

1. Open Airflow and show the `taxi_integrated_etl_with_ai` DAG graph.
2. Trigger it and open:
   - `detect_schema_changes` logs (schema evolution visualisation),
   - `generate_ai_mapping` logs (AI reaction),
   - `transform_taxi` logs (which transform was used, list of output columns).
3. Open your SQLite viewer or notebook:
   - Show `yellow_trips_v2` contents,
   - Show summary statistics / distributions of `trip_revenue` and `trip_duration_minutes`.

This end-to-end flow demonstrates that your pipeline:

- Detects schema evolution,
- Uses an LLM to regenerate mappings,
- And still produces consistent analytic features after the evolution.

