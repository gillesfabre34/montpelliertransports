"""
Exercise: Spark Silver – deduplication and basic data quality on vehicle positions.

Goal
----
Starting from Bronze-like vehicle positions (same schema as 12_spark_bronze_batch),
build a Silver-level DataFrame that:
- deduplicates events per (entity_id, trip_id) keeping the latest record
  by event_timestamp (D1 "KEEP LAST" pattern)
- applies simple quality rules (e.g. valid coordinates, reasonable speeds)

This mirrors:
- 03_deduplication/dedup_keep_last.py (list/dict version)
- 11_vehicle_positions_pandas/dedup_latest_by_entity_trip_pandas.py (pandas)
but implemented here with PySpark and window functions.
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession

from training.12_spark_bronze_batch.bronze_from_mocks import (
    create_spark_session,
    read_bronze_batch,
    resolve_bronze_source_path,
)


def deduplicate_keep_last_entity_trip_spark(df: DataFrame) -> DataFrame:
    """
    Deduplicate vehicle positions by (entity_id, trip_id) using a Spark
    window function:

        ROW_NUMBER() OVER (
          PARTITION BY entity_id, trip_id
          ORDER BY event_timestamp DESC
        )

    and keeping only rows with row_number = 1.

    Args:
        df: Bronze-like DataFrame with at least:
            - entity_id
            - trip_id
            - event_timestamp

    Returns:
        Deduplicated DataFrame (one row per (entity_id, trip_id)).
    """
    raise NotImplementedError(
        "Implement D1 KEEP LAST per (entity_id, trip_id) using a Spark "
        "window and row_number."
    )


def apply_silver_quality_rules(df: DataFrame) -> DataFrame:
    """
    Apply basic Silver-level data quality rules, for example:

    - filter out rows with null entity_id or trip_id
    - filter out invalid coordinates:
        - latitude not in [-90, 90]
        - longitude not in [-180, 180]
    - filter out negative speeds or speeds above a chosen threshold
      (e.g. > 150 km/h for a tram/bus system)

    Args:
        df: Input DataFrame (typically already deduplicated).

    Returns:
        Cleaned DataFrame.
    """
    raise NotImplementedError(
        "Implement filtering rules for coordinates, speed and key columns."
    )


def build_silver_dataframe(
    spark: SparkSession,
    source_path: Optional[str] = None,
    fmt: Optional[str] = None,
) -> DataFrame:
    """
    Helper that:
      1. Reads Bronze data from Parquet/Delta.
      2. Applies deduplicate_keep_last_entity_trip_spark().
      3. Applies apply_silver_quality_rules().

    Args:
        spark: SparkSession.
        source_path: Optional override for Bronze input path.
        fmt: Optional override for format ("delta" or "parquet").

    Returns:
        Silver-level DataFrame (deduplicated + cleaned).
    """
    raise NotImplementedError(
        "Implement Silver pipeline: read Bronze → dedup KEEP LAST → "
        "quality rules."
    )


if __name__ == "__main__":
    source_path = resolve_bronze_source_path()
    from os import getenv

    mock_format = (getenv("MOCK_DATA_FORMAT") or "parquet").lower()

    print(f"Building Silver DataFrame from: {source_path} (format={mock_format})")

    spark = create_spark_session(app_name="silver_training")
    df_silver = build_silver_dataframe(spark, source_path, mock_format)

    print("Silver DataFrame sample:")
    df_silver.show(10, truncate=False)

