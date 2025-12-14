# How Airflow Fits Into Your Thesis Project

## The Big Picture

Airflow is the **orchestration layer** that automates and coordinates your entire AI-powered ETL system. Think of it as the "conductor" that makes sure everything happens in the right order, at the right time.

## Your System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    AIRFLOW (Orchestrator)                    │
│                                                               │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  DAG 1: Schema Monitoring                            │   │
│  │  - Check for new data files                         │   │
│  │  - Detect schema changes                            │   │
│  │  - Trigger alerts if changes found                  │   │
│  └──────────────────────────────────────────────────────┘   │
│                          ↓                                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  DAG 2: AI Mapping Regeneration                       │   │
│  │  - Analyze schema changes                            │   │
│  │  - Call AI to generate new mappings                  │   │
│  │  - Validate generated code                           │   │
│  └──────────────────────────────────────────────────────┘   │
│                          ↓                                   │
│  ┌──────────────────────────────────────────────────────┐   │
│  │  DAG 3: ETL Pipeline Execution                       │   │
│  │  - Extract data with new schema                       │   │
│  │  - Transform using AI-generated mappings             │   │
│  │  - Load to destination                               │   │
│  └──────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────┘
```

## Why Use Airflow?

### 1. **Automation**
Instead of manually running scripts, Airflow can:
- Check for new data files every hour/day
- Automatically detect schema changes
- Trigger AI mapping regeneration when needed
- Run ETL pipelines on schedule

### 2. **Workflow Management**
Airflow handles:
- **Dependencies**: "Run schema detection BEFORE generating mappings"
- **Retries**: If something fails, retry automatically
- **Scheduling**: Run tasks daily, hourly, or on-demand
- **Monitoring**: See what's running, what failed, what succeeded

### 3. **Integration**
Airflow connects all your components:
- Schema detection module
- AI mapping generation
- ETL pipeline
- Data sources
- Destinations

### 4. **Visibility**
Airflow UI shows you:
- Which tasks are running
- Which tasks succeeded/failed
- Execution logs
- Task dependencies
- Historical runs

## How Airflow Works in Your Project

### Current Setup

You already have:
- ✅ **Basic ETL DAG** (`simple_etl_pipeline`) - Runs extract → transform → load
- ✅ **Schema detection module** - Detects changes between schemas
- ✅ **AI mapping module** - Generates new ETL mappings

### What's Missing (What We'll Build)

1. **Schema Monitoring DAG** - Automatically checks for schema changes
2. **Integrated DAG** - Combines schema detection + AI mapping + ETL
3. **Smart Pipeline** - Updates itself when schemas change

## Example: Complete Workflow

Here's how it all works together:

### Scenario: New Data File Arrives with Schema Changes

1. **Airflow DAG triggers** (scheduled or manual)
   ```
   ┌─────────────────┐
   │  Check for new   │
   │  data files      │
   └────────┬─────────┘
            ↓
   ┌─────────────────┐
   │  Detect schema  │
   │  changes        │
   └────────┬────────┘
            ↓
   ┌─────────────────┐
   │  Changes found? │
   └────────┬────────┘
       Yes  │  No
       │    └──→ End
       ↓
   ┌─────────────────┐
   │  Call AI to     │
   │  regenerate     │
   │  mappings       │
   └────────┬────────┘
            ↓
   ┌─────────────────┐
   │  Validate       │
   │  generated code │
   └────────┬────────┘
            ↓
   ┌─────────────────┐
   │  Update ETL     │
   │  pipeline       │
   └────────┬────────┘
            ↓
   ┌─────────────────┐
   │  Run ETL with   │
   │  new mappings   │
   └─────────────────┘
   ```

## Real-World Use Cases

### Use Case 1: Daily Schema Monitoring

**Problem:** You want to know immediately when a data source schema changes.

**Solution:** Airflow DAG runs every day:
```python
# Runs daily at 6 AM
@dag(schedule="0 6 * * *")
def schema_monitor_dag():
    check_for_new_files() >> detect_schema_changes() >> send_alert_if_changes()
```

### Use Case 2: Automatic Pipeline Updates

**Problem:** When schema changes, you want the ETL pipeline to update automatically.

**Solution:** Airflow DAG that:
1. Detects schema changes
2. Generates new mappings using AI
3. Updates the transform function
4. Runs the updated pipeline
5. Validates the results

### Use Case 3: Testing New Mappings

**Problem:** Before deploying new mappings, you want to test them.

**Solution:** Airflow DAG with a test branch:
```python
detect_changes() >> generate_mappings() >> [
    test_mappings(),  # Test in parallel
    validate_mappings()
] >> deploy_if_valid()
```

## Benefits for Your Thesis

1. **Demonstrates Real-World Application**
   - Shows how your system works in production
   - Proves automation is possible

2. **Provides Metrics**
   - Airflow tracks execution times
   - You can measure: detection time, AI generation time, pipeline runtime
   - Great data for your thesis results section

3. **Shows Integration**
   - Not just isolated modules
   - Complete end-to-end system
   - Professional workflow

4. **Enables Experiments**
   - Easy to test different scenarios
   - Can run multiple experiments in parallel
   - Track results over time

## Next Steps

1. **Create Schema Monitoring DAG** - Automatically check for changes
2. **Create Integrated DAG** - Full workflow: detect → generate → execute
3. **Add Error Handling** - What happens if AI fails? If detection fails?
4. **Add Notifications** - Alert when schema changes detected
5. **Add Testing** - Validate generated mappings before using them

## Key Airflow Concepts for Your Project

### DAGs (Directed Acyclic Graphs)
- Your workflows are DAGs
- Each DAG has tasks
- Tasks have dependencies

### Tasks
- Each step in your workflow is a task
- Examples: "detect_schema_changes", "generate_ai_mapping", "run_etl"

### Operators
- PythonOperator: Run Python functions (your modules)
- BashOperator: Run shell commands
- Sensors: Wait for conditions (e.g., "wait for new file")

### Scheduling
- `schedule=None`: Manual trigger only
- `schedule="@daily"`: Run once per day
- `schedule="0 6 * * *"`: Run at 6 AM daily (cron format)

## Example: Simple Integration

Here's a simple DAG that uses your schema detection:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def detect_schema_changes_task():
    from ai.detect_schema_change import detect_changes
    import os
    
    project_root = "/Users/rajakarthikchirumamilla/Documents/ThesisWork/etl-ai-schema"
    old_file = os.path.join(project_root, "data/raw/users_v1.csv")
    new_file = os.path.join(project_root, "data/raw/users_v2.csv")
    
    result = detect_changes(old_file, new_file)
    
    if result['classification']['migration_required']:
        print("⚠️ Schema changes detected! Migration required.")
        return "changes_detected"
    else:
        print("✓ No schema changes detected.")
        return "no_changes"

with DAG(
    dag_id="schema_monitor",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",  # Run daily
    catchup=False
) as dag:
    
    detect_task = PythonOperator(
        task_id="detect_schema_changes",
        python_callable=detect_schema_changes_task
    )
```

This DAG runs daily and checks for schema changes!

---

**In Summary:** Airflow is the glue that connects your schema detection, AI mapping generation, and ETL pipeline into a complete, automated system. It's what makes your thesis project production-ready and demonstrates real-world applicability.

