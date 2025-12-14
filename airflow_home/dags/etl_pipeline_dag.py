# etl_pipeline_dag.py
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys
import os

# -----------------------------
# Set project path and ensure it is in sys.path
# -----------------------------
PROJECT_PATH = "/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema"
if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

# -----------------------------
# Import ETL modules safely
# -----------------------------
try:
    from etl.extract import extract
    from etl.transform import transform
    from etl.load import load
except ImportError as e:
    raise ImportError(f"Failed to import ETL modules: {e}")

# -----------------------------
# Ensure data folders exist
# -----------------------------
RAW_DATA = os.path.join(PROJECT_PATH, "data", "raw")
PROCESSED_DATA = os.path.join(PROJECT_PATH, "data", "processed")
for folder in [RAW_DATA, PROCESSED_DATA]:
    os.makedirs(folder, exist_ok=True)

# -----------------------------
# Define task functions
# -----------------------------
def run_extract():
    input_file = os.path.join(RAW_DATA, "users_v1.csv")
    df = extract(input_file)
    df.to_pickle(os.path.join(PROCESSED_DATA, "extracted.pkl"))

def run_transform():
    import pandas as pd
    df = pd.read_pickle(os.path.join(PROCESSED_DATA, "extracted.pkl"))
    df = transform(df)
    df.to_pickle(os.path.join(PROCESSED_DATA, "transformed.pkl"))

def run_load():
    import pandas as pd
    df = pd.read_pickle(os.path.join(PROCESSED_DATA, "transformed.pkl"))
    load(df)

# -----------------------------
# Default DAG args
# -----------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
}

# -----------------------------
# Define DAG
# -----------------------------
with DAG(
    dag_id="simple_etl_pipeline",
    default_args=default_args,
    schedule=None,  # Airflow 3.x uses 'schedule' instead of 'schedule_interval'
    catchup=False,
    tags=["etl", "thesis"]
) as dag:

    extract_task = PythonOperator(
        task_id="extract_step",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform_step",
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id="load_step",
        python_callable=run_load,
    )

    extract_task >> transform_task >> load_task