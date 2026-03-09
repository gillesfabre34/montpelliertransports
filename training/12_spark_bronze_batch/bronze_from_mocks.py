"""
Exercise: Spark Bronze (batch) on real vehicle positions.

Goal
----
Use PySpark to:
- create a SparkSession (local)
- read Bronze-like data (mock Parquet/Delta) for vehicle positions
- inspect schema, partitions, and basic metrics

Data source
-----------
Same as for the pandas exercises:
- preferred: path from `MOCK_DATA_PATH` env variable
- fallback: local `consumer/mocks/` folder

Format:
- `MOCK_DATA_FORMAT=delta` → read as Delta table
- otherwise → read Parquet

Schema (Bronze-style, simplified)
---------------------------------
- entity_id: string
- trip_id: string
- route_id: string
- latitude: double
- longitude: double
- bearing: double
- speed: double
- event_timestamp: timestamp
- year: int
- month: int
- day: int
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def resolve_bronze_source_path() -> str:
    """
    Resolve the source path for Bronze-like mock data.

    Priority:
    1. `MOCK_DATA_PATH` env var (local or Azure/Blob path).
    2. Local `consumer/mocks/` folder (project-relative).
    """
    env_path = os.getenv("MOCK_DATA_PATH")
    if env_path:
        return env_path

    project_root = Path(__file__).resolve().parents[2]
    local_mocks = project_root / "consumer" / "mocks"
    return str(local_mocks)


def create_spark_session(app_name: str = "bronze_training") -> SparkSession:
    """
    Create a local SparkSession for batch processing.

    For this training exercise we do not configure Delta or Kafka; we only
    need a simple SparkSession capable of reading Parquet/Delta locally or
    via an already-configured environment (e.g. Azure access).
    """
    return (
        SparkSession.builder.appName(app_name)
        .getOrCreate()
    )


def read_bronze_batch(
    spark: SparkSession,
    path: Optional[str] = None,
    fmt: Optional[str] = None,
) -> DataFrame:
    """
    Read Bronze-like data in batch mode using Spark.

    Args:
        spark: SparkSession.
        path: Optional explicit path. If None, uses resolve_bronze_source_path().
        fmt: Optional format override. If None, uses MOCK_DATA_FORMAT env var:
             - "delta" → Spark Delta reader
             - anything else → Parquet

    Returns:
        DataFrame with the Bronze schema.
    """
    raise NotImplementedError(
        "Implement Spark batch read from Parquet/Delta using the given path "
        "and format (delta vs parquet)."
    )


def print_bronze_overview(df: DataFrame) -> None:
    """
    Print a quick overview of the Bronze DataFrame.

    Suggested info:
        - schema (df.printSchema())
        - total number of rows
        - distinct count of entity_id, trip_id, route_id
        - sample of 5 rows
    """
    raise NotImplementedError(
        "Implement basic exploration of the Bronze DataFrame (schema, counts, "
        "sample rows)."
    )


if __name__ == "__main__":
    source_path = resolve_bronze_source_path()
    mock_format = (os.getenv("MOCK_DATA_FORMAT") or "parquet").lower()

    print(f"Using Bronze source path: {source_path}")
    print(f"Using format: {mock_format}")

    spark = create_spark_session()
    df_bronze = read_bronze_batch(spark, path=source_path, fmt=mock_format)

    print_bronze_overview(df_bronze)

