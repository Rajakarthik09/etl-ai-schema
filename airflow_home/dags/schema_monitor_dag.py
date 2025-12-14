"""
Schema Monitoring DAG

This DAG automatically monitors for schema changes in data sources.
It demonstrates how Airflow orchestrates the schema detection workflow.
"""
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# Set project path
PROJECT_PATH = "/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema"
if PROJECT_PATH not in sys.path:
    sys.path.append(PROJECT_PATH)

# Default arguments
default_args = {
    "owner": "thesis",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def detect_schema_changes():
    """
    Task: Detect schema changes between two data versions.
    This demonstrates integrating your schema detection module with Airflow.
    """
    from ai.detect_schema_change import detect_changes
    
    # Define file paths
    old_file = os.path.join(PROJECT_PATH, "data/raw/users_v1.csv")
    new_file = os.path.join(PROJECT_PATH, "data/raw/users_v2.csv")
    
    # Check if files exist
    if not os.path.exists(old_file):
        raise FileNotFoundError(f"Old file not found: {old_file}")
    if not os.path.exists(new_file):
        raise FileNotFoundError(f"New file not found: {new_file}")
    
    print(f"Comparing schemas:")
    print(f"  Old: {old_file}")
    print(f"  New: {new_file}")
    
    # Run schema detection
    result = detect_changes(old_file, new_file)
    
    # Log results
    print("\n" + "="*50)
    print("SCHEMA CHANGE DETECTION RESULTS")
    print("="*50)
    print(f"Added columns: {len(result['changes']['added_columns'])}")
    print(f"Removed columns: {len(result['changes']['removed_columns'])}")
    print(f"Renamed columns: {len(result['changes']['renamed_columns'])}")
    print(f"Type changes: {len(result['changes']['type_changes'])}")
    print(f"\nSeverity: {result['classification']['severity']}")
    print(f"Migration required: {result['classification']['migration_required']}")
    
    # Store result in XCom for downstream tasks
    return {
        "changes_detected": result['classification']['migration_required'],
        "severity": result['classification']['severity'],
        "added_count": len(result['changes']['added_columns']),
        "removed_count": len(result['changes']['removed_columns']),
    }

def evaluate_changes(**context):
    """
    Task: Evaluate if changes require action.
    This is a decision point in the workflow.
    """
    # Get results from previous task
    ti = context['ti']
    detection_result = ti.xcom_pull(task_ids='detect_schema_changes')
    
    changes_detected = detection_result['changes_detected']
    severity = detection_result['severity']
    
    print(f"\nEvaluating changes...")
    print(f"Changes detected: {changes_detected}")
    print(f"Severity: {severity}")
    
    if changes_detected:
        if severity == "high":
            print("⚠️ HIGH SEVERITY: Breaking changes detected!")
            print("Action required: Regenerate ETL mappings")
            return "regenerate_mappings"
        elif severity == "medium":
            print("⚠️ MEDIUM SEVERITY: Some changes detected")
            print("Action: Review and potentially update mappings")
            return "review_mappings"
        else:
            print("✓ LOW SEVERITY: Non-breaking changes")
            print("Action: Monitor, but no immediate action needed")
            return "monitor_only"
    else:
        print("✓ No schema changes detected. Pipeline can continue normally.")
        return "no_action"

def generate_ai_mapping(**context):
    """
    Task: Generate new ETL mappings using AI.
    This demonstrates integrating AI mapping generation with Airflow.
    """
    from ai.regenerate_mapping import regenerate_mapping
    import json
    
    # Get detection results
    ti = context['ti']
    detection_result = ti.xcom_pull(task_ids='detect_schema_changes')
    
    print("\n" + "="*50)
    print("GENERATING AI MAPPING")
    print("="*50)
    
    # Define paths
    old_schema_path = os.path.join(PROJECT_PATH, "schemas/old_schema.json")
    new_schema_path = os.path.join(PROJECT_PATH, "schemas/new_schema.json")
    changes_path = os.path.join(PROJECT_PATH, "schemas/changes.json")
    output_path = os.path.join(PROJECT_PATH, "etl/transform_generated.py")
    
    # Check if schema files exist
    if not os.path.exists(old_schema_path):
        raise FileNotFoundError(f"Old schema not found: {old_schema_path}")
    if not os.path.exists(new_schema_path):
        raise FileNotFoundError(f"New schema not found: {new_schema_path}")
    
    # Create a simple changes dict (in real scenario, this would come from detection)
    # For now, we'll create a basic one
    changes = {
        "added_columns": [],
        "removed_columns": [],
        "renamed_columns": [],
        "type_changes": []
    }
    
    # Save changes to file
    with open(changes_path, 'w') as f:
        json.dump(changes, f, indent=2)
    
    # Generate mapping
    try:
        code = regenerate_mapping(
            old_schema_path,
            new_schema_path,
            changes_path,
            output_path
        )
        print("✓ AI mapping generated successfully")
        print(f"Generated code saved to: {output_path}")
        return "success"
    except Exception as e:
        print(f"❌ Error generating mapping: {e}")
        return "failed"

def notify_changes(**context):
    """
    Task: Send notification about schema changes.
    In production, this would send email/Slack notifications.
    """
    ti = context['ti']
    detection_result = ti.xcom_pull(task_ids='detect_schema_changes')
    evaluation_result = ti.xcom_pull(task_ids='evaluate_changes')
    
    print("\n" + "="*50)
    print("NOTIFICATION")
    print("="*50)
    print(f"Schema changes detected: {detection_result['changes_detected']}")
    print(f"Severity: {detection_result['severity']}")
    print(f"Action: {evaluation_result}")
    print(f"Added columns: {detection_result['added_count']}")
    print(f"Removed columns: {detection_result['removed_count']}")
    print("\n📧 In production, this would send:")
    print("   - Email to data engineering team")
    print("   - Slack notification")
    print("   - Dashboard update")

# Define the DAG
with DAG(
    dag_id="schema_monitor",
    default_args=default_args,
    description="Monitor data sources for schema changes and trigger AI mapping regeneration",
    schedule=None,  # Set to "@daily" to run automatically, or None for manual trigger
    catchup=False,
    tags=["schema", "monitoring", "ai", "thesis"],
) as dag:
    
    # Task 1: Detect schema changes
    detect_task = PythonOperator(
        task_id="detect_schema_changes",
        python_callable=detect_schema_changes,
        doc_md="""
        Detects schema changes between old and new data files.
        Uses the schema detection module to compare schemas.
        """,
    )
    
    # Task 2: Evaluate changes
    evaluate_task = PythonOperator(
        task_id="evaluate_changes",
        python_callable=evaluate_changes,
        doc_md="""
        Evaluates the detected changes and determines what action is needed.
        Returns: 'regenerate_mappings', 'review_mappings', 'monitor_only', or 'no_action'
        """,
    )
    
    # Task 3: Generate AI mapping (only if changes detected)
    generate_mapping_task = PythonOperator(
        task_id="generate_ai_mapping",
        python_callable=generate_ai_mapping,
        doc_md="""
        Generates new ETL mappings using AI when schema changes are detected.
        Uses the AI mapping regeneration module.
        """,
    )
    
    # Task 4: Notify about changes
    notify_task = PythonOperator(
        task_id="notify_changes",
        python_callable=notify_changes,
        doc_md="""
        Sends notifications about schema changes.
        In production, this would send emails/Slack messages.
        """,
    )
    
    # Task 5: Print summary
    summary_task = BashOperator(
        task_id="print_summary",
        bash_command="""
        echo "=========================================="
        echo "SCHEMA MONITORING DAG COMPLETED"
        echo "=========================================="
        echo "Check the Airflow UI for detailed logs"
        echo "Generated files:"
        echo "  - schemas/old_schema.json"
        echo "  - schemas/new_schema.json"
        echo "  - etl/transform_generated.py (if AI mapping was generated)"
        """,
    )
    
    # Define task dependencies
    # detect → evaluate → [generate_mapping (if needed) + notify] → summary
    detect_task >> evaluate_task
    
    # Generate mapping only if evaluation says to regenerate
    # In a real scenario, you'd use BranchPythonOperator for conditional logic
    evaluate_task >> generate_mapping_task >> notify_task >> summary_task
    
    # Also notify even if no mapping generation needed
    evaluate_task >> notify_task

