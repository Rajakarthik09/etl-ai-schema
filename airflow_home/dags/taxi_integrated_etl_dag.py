"""
Taxi Integrated ETL DAG with Schema Detection and AI Mapping

This DAG demonstrates the complete workflow on the NYC yellow taxi dataset:
1. Detect schema changes between taxi schema versions (V1 -> V2)
2. Generate AI mappings if changes are detected
3. Run the taxi ETL pipeline with the (potentially) updated mappings

The DAG is designed to be used for the main evaluation described in the thesis.
"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys
import os
import json

# Set project path
PROJECT_PATH = "/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema"
if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

# Ensure data folders exist
RAW_DATA = os.path.join(PROJECT_PATH, "data", "raw")
PROCESSED_DATA = os.path.join(PROJECT_PATH, "data", "processed")
for folder in [RAW_DATA, PROCESSED_DATA]:
    os.makedirs(folder, exist_ok=True)


def detect_and_handle_schema_changes():
    """
    Step 1: Detect schema changes for NYC taxi data (V1 -> V2) and persist schema JSONs.
    """
    from ai.detect_schema_change import detect_changes

    old_file = os.path.join(RAW_DATA, "yellow_base_v1.csv")
    new_file = os.path.join(RAW_DATA, "yellow_base_v2.csv")

    if not os.path.exists(old_file) or not os.path.exists(new_file):
        print("⚠️ Taxi CSV files not found. Skipping schema detection.")
        return {"changes_detected": False}

    print("=" * 60)
    print("TAXI STEP 1: SCHEMA DETECTION (V1 -> V2)")
    print("=" * 60)

    schemas_dir = os.path.join(PROJECT_PATH, "schemas")
    os.makedirs(schemas_dir, exist_ok=True)

    old_schema_path = os.path.join(schemas_dir, "yellow_v1_schema.json")
    new_schema_path = os.path.join(schemas_dir, "yellow_v2_schema.json")
    changes_path = os.path.join(schemas_dir, "yellow_v1_to_v2_changes.json")

    # Use detect_changes so that schemas are extracted and saved
    result = detect_changes(
        old_file,
        new_file,
        old_schema_path=old_schema_path,
        new_schema_path=new_schema_path,
    )

    # Persist changes dict for downstream AI mapping
    with open(changes_path, "w") as f:
        json.dump(result["changes"], f, indent=2)
    print(f"✓ Saved schema changes to {changes_path}")

    changes_detected = result["classification"]["migration_required"]

    if changes_detected:
        print("⚠️ Schema changes detected for taxi data!")
        print(f"   Severity: {result['classification']['severity']}")
        print(f"   Added:   {len(result['changes']['added_columns'])} columns")
        print(f"   Removed: {len(result['changes']['removed_columns'])} columns")
        print(f"   Renamed: {len(result['changes']['renamed_columns'])} columns")
        print(f"   Type changes: {len(result['changes']['type_changes'])}")
        return {
            "changes_detected": True,
            "severity": result["classification"]["severity"],
        }
    else:
        print("✓ No schema changes detected. Using existing taxi mappings.")
        return {"changes_detected": False, "severity": result["classification"]["severity"]}


def _get_taxi_transform_snippet():
    """
    Helper: read etl/transform_taxi.py and extract the transform(df) function
    to be used as the reference in the AI prompt.
    """
    etl_dir = os.path.join(PROJECT_PATH, "etl")
    path = os.path.join(etl_dir, "transform_taxi.py")
    if not os.path.exists(path):
        return None
    with open(path, "r") as f:
        content = f.read()
    start = content.find("def transform(")
    if start == -1:
        return None
    end = content.find("\n\ndef ", start + 1)
    if end == -1:
        end = len(content)
    return content[start:end].strip()


def generate_mapping_if_needed(**context):
    """
    Step 2: Generate AI mapping for taxi if schema changes were detected.
    Uses the taxi transform as the business-logic reference.
    """
    ti = context["ti"]
    detection_result = ti.xcom_pull(task_ids="detect_schema_changes")

    if not detection_result.get("changes_detected", False):
        print("=" * 60)
        print("TAXI STEP 2: SKIPPING AI MAPPING GENERATION")
        print("=" * 60)
        print("No taxi schema changes detected. Using baseline transform_taxi.")
        return "skipped"

    print("=" * 60)
    print("TAXI STEP 2: AI MAPPING GENERATION (V1 -> V2)")
    print("=" * 60)

    from ai.regenerate_mapping import regenerate_mapping

    schemas_dir = os.path.join(PROJECT_PATH, "schemas")
    old_schema_path = os.path.join(schemas_dir, "yellow_v1_schema.json")
    new_schema_path = os.path.join(schemas_dir, "yellow_v2_schema.json")
    changes_path = os.path.join(schemas_dir, "yellow_v1_to_v2_changes.json")
    output_path = os.path.join(PROJECT_PATH, "etl", "transform_generated.py")

    for p in (old_schema_path, new_schema_path, changes_path):
        if not os.path.exists(p):
            raise FileNotFoundError(f"Required schema file not found: {p}")

    snippet = _get_taxi_transform_snippet()
    try:
        regenerate_mapping(
            old_schema_path,
            new_schema_path,
            changes_path,
            output_path,
            current_transform_snippet=snippet,
        )
        print("✓ AI taxi mapping generated and saved to etl/transform_generated.py")
        return "generated"
    except Exception as e:
        print(f"⚠️ AI mapping generation for taxi failed: {e}")
        print("Falling back to baseline transform_taxi.")
        return "failed"


def run_extract():
    """
    Step 3: Extract taxi data from source (V2 preferred, fall back to V1).
    """
    print("=" * 60)
    print("TAXI STEP 3: EXTRACT")
    print("=" * 60)

    from etl.extract import extract
    import pandas as pd

    input_file = os.path.join(RAW_DATA, "yellow_base_v2.csv")
    if not os.path.exists(input_file):
        input_file = os.path.join(RAW_DATA, "yellow_base_v1.csv")

    print(f"Extracting taxi data from: {input_file}")
    df = extract(input_file)
    df.to_pickle(os.path.join(PROCESSED_DATA, "extracted_taxi.pkl"))
    print(f"✓ Extracted {len(df)} taxi rows")
    return len(df)


def run_transform():
    """
    Step 4: Transform taxi data.
    Uses AI-generated taxi mapping if available; otherwise uses baseline transform_taxi.
    """
    print("=" * 60)
    print("TAXI STEP 4: TRANSFORM")
    print("=" * 60)

    import pandas as pd

    df = pd.read_pickle(os.path.join(PROCESSED_DATA, "extracted_taxi.pkl"))

    # For evaluation, always use the baseline taxi transform so that
    # V1 and V2 metrics are directly comparable.
    # The AI-generated mapping is still produced (see generate_ai_mapping),
    # but not applied in this transform step.
    from etl.transform_taxi import transform
    df = transform(df)
    df.to_pickle(os.path.join(PROCESSED_DATA, "transformed_taxi.pkl"))
    print(f"✓ Transformed {len(df)} taxi rows")
    print(f"  Columns: {list(df.columns)}")
    return len(df)


def run_load():
    """
    Step 5: Load taxi data to SQLite.
    """
    print("=" * 60)
    print("TAXI STEP 5: LOAD")
    print("=" * 60)

    import pandas as pd
    from etl.load import load

    df = pd.read_pickle(os.path.join(PROCESSED_DATA, "transformed_taxi.pkl"))
    # Use a specific table name for taxi evaluation
    load(df, table_name="yellow_trips_v2")
    print(f"✓ Loaded {len(df)} taxi rows to database table yellow_trips_v2")
    return len(df)


with DAG(
    dag_id="taxi_integrated_etl_with_ai",
    default_args={
        "owner": "thesis",
        "depends_on_past": False,
        "start_date": datetime(2023, 1, 1),
    },
    description="Taxi ETL pipeline with schema detection and AI mapping generation",
    schedule=None,  # Manual trigger for demo
    catchup=False,
    tags=["etl", "ai", "schema", "taxi", "thesis"],
) as dag:

    detect_task = PythonOperator(
        task_id="detect_schema_changes",
        python_callable=detect_and_handle_schema_changes,
    )

    generate_task = PythonOperator(
        task_id="generate_ai_mapping",
        python_callable=generate_mapping_if_needed,
    )

    extract_task = PythonOperator(
        task_id="extract_taxi",
        python_callable=run_extract,
    )

    transform_task = PythonOperator(
        task_id="transform_taxi",
        python_callable=run_transform,
    )

    load_task = PythonOperator(
        task_id="load_taxi",
        python_callable=run_load,
    )

    # Workflow: detect → generate → extract → transform → load
    detect_task >> generate_task >> extract_task >> transform_task >> load_task

