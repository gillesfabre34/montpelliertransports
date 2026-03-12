"""
Exercise: Spark Gold – route-level metrics for BI.

Goal
----
From a Silver-level vehicle positions DataFrame (deduplicated and cleaned),
build Gold-level aggregations suitable for BI:

1. Hourly route speed metrics:
   - group by (route_id, event_hour)
   - compute average speed, number of points, distinct vehicles

2. Daily route activity (Top-K style):
   - for each day, compute how many points / vehicles per route_id
   - this can later be used to find the busiest routes

Inputs
------
Silver DataFrame with at least:
- entity_id
- route_id
- event_timestamp (timestamp)
- speed
"""

from __future__ import annotations

from typing import Optional

from pyspark.sql import DataFrame, SparkSession


def compute_route_hourly_speed(df: DataFrame) -> DataFrame:
    """
    Compute hourly route metrics.

    Suggested fields:
        - route_id
        - event_hour: event_timestamp truncated to the hour
        - avg_speed: average of speed
        - points_count: number of records
        - vehicles_count: distinct count of entity_id

    Hint:
        - use functions like date_trunc("hour", ...), groupBy, agg, countDistinct.
    """
    raise NotImplementedError(
        "Implement hourly route metrics grouped by (route_id, event_hour)."
    )


def compute_daily_route_activity(df: DataFrame) -> DataFrame:
    """
    Compute daily route activity metrics.

    Suggested fields:
        - event_date: date extracted from event_timestamp
        - route_id
        - points_count
        - vehicles_count

    This can be combined later with Top-K logic to find the busiest routes
    per day.
    """
    raise NotImplementedError(
        "Implement daily route activity grouped by (event_date, route_id)."
    )


def build_gold_dataframes(
        spark: SparkSession,
        source_path: Optional[str] = None,
        fmt: Optional[str] = None,
) -> tuple[DataFrame, DataFrame]:
    """
    Helper to build both Gold-level DataFrames from Silver:
      - hourly metrics
      - daily activity

    Args:
        spark: SparkSession.
        source_path: Optional override for Bronze input path (used by the
                     underlying Silver builder).
        fmt: Optional format override ("delta" or "parquet").
    """
    raise NotImplementedError(
        "Implement Gold pipeline: build Silver from Bronze, then derive "
        "hourly and daily route metrics."
    )


if __name__ == "__main__":
    create_spark_session = _bronze_module.create_spark_session
    resolve_bronze_source_path = _bronze_module.resolve_bronze_source_path

    source_path = resolve_bronze_source_path()
    mock_format = (getenv("MOCK_DATA_FORMAT") or "parquet").lower()

    print(f"Building Gold metrics from Silver built on: {source_path} (format={mock_format})")

    spark = create_spark_session(app_name="gold_training")
    df_hourly, df_daily = build_gold_dataframes(spark, source_path, mock_format)

    print("Hourly route metrics sample:")
    df_hourly.show(10, truncate=False)

    print("Daily route activity sample:")
    df_daily.show(10, truncate=False)
