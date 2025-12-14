# Airflow Setup Guide

This guide will help you start and access your Airflow instance to view your DAG.

## Quick Start (Recommended)

### Option 1: Standalone Mode (Easiest)

Run this command in your terminal:

```bash
cd /Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema
./start_airflow_standalone.sh
```

This will start both the API server and scheduler in one process. The output will show you when Airflow is ready.

### Option 2: Separate Processes

If you prefer to run the API server and scheduler separately:

```bash
cd /Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema
source venv/bin/activate
export AIRFLOW_HOME=/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema/airflow_home

# Terminal 1: Start API Server
airflow api-server --port 8080

# Terminal 2: Start Scheduler
airflow scheduler
```

## Accessing the Airflow UI

Once Airflow is running:

1. Open your web browser
2. Navigate to: **http://localhost:8080**
3. Login with:
   - **Username:** `admin`
   - **Password:** `99CQPvyyNQ43NYeg`

## Your DAG

Your ETL pipeline DAG is named: **`simple_etl_pipeline`**

It should appear in the DAGs list in the Airflow UI. The DAG contains three tasks:
- `extract_step` - Extracts data from CSV
- `transform_step` - Transforms the data
- `load_step` - Loads the transformed data

## Stopping Airflow

To stop Airflow:

```bash
./stop_airflow.sh
```

Or if running standalone, press `CTRL+C` in the terminal where it's running.

## Troubleshooting

### Port 8080 already in use

If port 8080 is already in use, you can change it:

```bash
airflow api-server --port 8081
```

Then access the UI at http://localhost:8081

### DAG not showing up

1. Check that your DAG file is in: `airflow_home/dags/etl_pipeline_dag.py`
2. Verify there are no syntax errors:
   ```bash
   airflow dags list
   ```
3. Check the DAG processor logs in: `airflow_home/logs/dag_processor/`

### Database issues

If you encounter database errors, you can reinitialize:

```bash
export AIRFLOW_HOME=/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema/airflow_home
airflow db reset  # WARNING: This will delete all data!
airflow db init
```

## Environment Variables

Make sure these are set when running Airflow commands:

```bash
export AIRFLOW_HOME=/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema/airflow_home
```

## Notes

- Your DAG is set to `schedule=None`, meaning it won't run automatically. You'll need to trigger it manually from the UI.
- The DAG expects a file at `data/raw/users_v1.csv` for the extract step.
- Make sure your virtual environment is activated before running Airflow commands.

