"""
Exercise: Airflow DAG design for the vehicle positions pipeline.

Goal
----
Design (in pure Python, without depending on Airflow being installed) the
structure of a DAG that orchestrates:

- Bronze step (Spark, batch/stream → Delta/Parquet)
- Silver step (Spark, dedup + quality rules)
- Gold step (Spark, aggregations for BI)

This exercise focuses on **thinking in terms of tasks and dependencies**.
Later, the same structure can be implemented as a real Airflow DAG under
the `airflow/dags/` directory.

Output structure
----------------
We describe the DAG as a simple Python dict:

{
  "dag_id": "vehicle_positions_bronze_silver_gold",
  "schedule": "0 * * * *",
  "tasks": [
      {"task_id": "bronze_ingestion", "type": "spark_job", ...},
      {"task_id": "silver_processing", "type": "spark_job", ...},
      {"task_id": "gold_aggregations", "type": "spark_job", ...},
  ],
  "dependencies": [
      ("bronze_ingestion", "silver_processing"),
      ("silver_processing", "gold_aggregations"),
  ],
}

You will design this spec and then print it in __main__.
"""

from __future__ import annotations

from typing import Any, Dict, List, Tuple


def build_vehicle_positions_dag_spec() -> Dict[str, Any]:
    """
    Build a high-level DAG specification for the Bronze → Silver → Gold
    pipeline.

    The spec should at least contain:
      - "dag_id": string
      - "schedule": string (cron expression or "@daily", "@hourly", etc.)
      - "tasks": list of task specs (dicts)
      - "dependencies": list of (upstream_task_id, downstream_task_id) tuples

    Suggestions for tasks:
      - bronze_ingestion (runs the Spark Bronze job / consumer)
      - silver_processing (runs the Silver Spark batch)
      - gold_aggregations (runs the Gold Spark batch)
      - optional: data_quality_checks on Gold tables
    """
    raise NotImplementedError(
        "Design the DAG spec: tasks and dependencies for the full pipeline."
    )


def format_dag_spec_for_console(dag_spec: Dict[str, Any]) -> str:
    """
    Format the DAG spec as a human-readable multi-line string that can be
    printed in the console.

    Suggested format (example):

        DAG: vehicle_positions_bronze_silver_gold
        Schedule: 0 * * * *

        Tasks:
          - bronze_ingestion (type: spark_job)
          - silver_processing (type: spark_job)
          - gold_aggregations (type: spark_job)

        Dependencies:
          - bronze_ingestion -> silver_processing
          - silver_processing -> gold_aggregations
    """
    raise NotImplementedError(
        "Implement pretty-printing of the DAG spec for the console."
    )


if __name__ == "__main__":
    dag_spec = build_vehicle_positions_dag_spec()
    output = format_dag_spec_for_console(dag_spec)
    print(output)

