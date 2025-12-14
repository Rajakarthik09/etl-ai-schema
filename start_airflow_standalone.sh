#!/bin/bash

# Airflow Standalone Startup Script (Recommended for Airflow 3.x)
# This starts both webserver and scheduler in one process

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
cd "$SCRIPT_DIR"

# Activate virtual environment
source venv/bin/activate

# Set Airflow home directory
export AIRFLOW_HOME="$SCRIPT_DIR/airflow_home"

# Check if database is initialized
echo "Checking Airflow database..."
airflow db check

echo ""
echo "=========================================="
echo "Starting Airflow in standalone mode..."
echo "=========================================="
echo ""
echo "This will start both the API server and scheduler."
echo "Access the Airflow UI at: http://localhost:8080"
echo ""
echo "Login credentials:"
echo "  Username: admin"
echo "  Password: 99CQPvyyNQ43NYeg"
echo ""
echo "Your DAG 'simple_etl_pipeline' should be visible in the UI."
echo ""
echo "Press CTRL+C to stop Airflow."
echo "=========================================="
echo ""

# Start Airflow in standalone mode
airflow standalone

