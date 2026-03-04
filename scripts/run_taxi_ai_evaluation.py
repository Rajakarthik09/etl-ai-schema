#!/usr/bin/env python3
"""
Run AI-assisted evaluation for NYC taxi schema evolution scenarios across models.

For each (scenario, model) pair, this script:
  1. Calls scripts/run_taxi_regenerate_mapping.py to generate transform_generated.py
     using the specified model.
  2. Runs the taxi ETL pipeline in generated mode for the target version.
  3. Opens the SQLite database and performs simple correctness checks on the
     loaded table (row count > 0, non-negative trip_revenue, non-negative
     trip_duration_minutes).
  4. Records wall-clock time for steps 1–3.

The results are written as JSON lines to stdout so they can be collected into
CSV/JSON for plotting.
"""

import json
import os
import subprocess
import sys
import time
from typing import List, Dict, Any

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCHEMAS_DIR = PROJECT_ROOT / "schemas"
SCRIPTS_DIR = PROJECT_ROOT / "scripts"
ETL_DIR = PROJECT_ROOT / "etl"
DATA_DIR = PROJECT_ROOT / "data"


def run(cmd: List[str], cwd: Path) -> int:
    """Run a subprocess and return exit code."""
    proc = subprocess.run(cmd, cwd=str(cwd))
    return proc.returncode


def check_taxi_table(version: str) -> Dict[str, Any]:
    """Check basic correctness properties of the loaded taxi table."""
    import sqlite3

    db_path = DATA_DIR / "database.db"
    table = f"yellow_trips_{version}"
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    try:
        cur.execute(f"SELECT COUNT(*) FROM {table}")
        row_count = cur.fetchone()[0]
    except Exception:
        row_count = 0

    checks = {
        "row_count": row_count,
        "has_trip_revenue": False,
        "has_trip_duration_minutes": False,
        "non_negative_revenue": None,
        "non_negative_duration": None,
    }

    if row_count > 0:
        # Column existence
        cur.execute(f"PRAGMA table_info({table})")
        cols = [r[1] for r in cur.fetchall()]
        checks["has_trip_revenue"] = "trip_revenue" in cols
        checks["has_trip_duration_minutes"] = "trip_duration_minutes" in cols

        if checks["has_trip_revenue"]:
            cur.execute(f"SELECT MIN(trip_revenue) FROM {table}")
            mn = cur.fetchone()[0]
            checks["non_negative_revenue"] = (mn is None) or (mn >= 0)

        if checks["has_trip_duration_minutes"]:
            cur.execute(f"SELECT MIN(trip_duration_minutes) FROM {table}")
            mn = cur.fetchone()[0]
            checks["non_negative_duration"] = (mn is None) or (mn >= 0)

    conn.close()
    return checks


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Run AI evaluation across scenarios and models.")
    parser.add_argument(
        "--models",
        default="llama3",
        help="Comma-separated model names (e.g. llama3,mistral,codellama). Default: llama3",
    )
    parser.add_argument("--output", default=None, help="Write JSONL results to this file (in addition to stdout).")
    args = parser.parse_args()

    # Only evaluate the V1 -> V2 schema evolution scenario.
    scenarios = ["v1_to_v2"]
    models = [m.strip() for m in args.models.split(",") if m.strip()]

    results = []
    out_file = open(args.output, "w") if args.output else None

    for scenario in scenarios:
        # For V1 -> V2 schema evolution, the target version is always v2.
        target_version = "v2"
        for model in models:
            start = time.time()

            # 1. Regenerate mapping
            regen_cmd = [
                sys.executable,
                str(SCRIPTS_DIR / "run_taxi_regenerate_mapping.py"),
                "--scenario",
                scenario,
                "--model",
                model,
            ]
            regen_code = run(regen_cmd, PROJECT_ROOT)

            # 2. Run pipeline in generated mode if regeneration succeeded
            pipeline_code = -1
            if regen_code == 0:
                pipeline_cmd = [
                    sys.executable,
                    str(ETL_DIR / "pipeline.py"),
                    "taxi",
                    target_version,
                    "--generated",
                ]
                pipeline_code = run(pipeline_cmd, ETL_DIR)

            duration = time.time() - start

            checks: Dict[str, Any] = {}
            success = False
            if regen_code == 0 and pipeline_code == 0:
                checks = check_taxi_table(target_version)
                success = (
                    checks.get("row_count", 0) > 0
                    and checks.get("has_trip_revenue", False)
                    and checks.get("has_trip_duration_minutes", False)
                )

            result = {
                "scenario": scenario,
                "target_version": target_version,
                "model": model,
                "regen_exit_code": regen_code,
                "pipeline_exit_code": pipeline_code,
                "duration_seconds": duration,
                "success": success,
                "checks": checks,
            }
            results.append(result)
            line = json.dumps(result) + "\n"
            print(line, end="")
            sys.stdout.flush()
            if out_file:
                out_file.write(line)

    if out_file:
        out_file.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

