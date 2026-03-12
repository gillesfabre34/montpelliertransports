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

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from rich import print

from consumer.gold.gold_utils import get_df_silver
from utils.tools import logg, sort_by_natural_order


def calc_stats_by_route(df_silver: DataFrame) -> DataFrame:
    """
    Compute route metrics.

    Suggested fields:
        - route_id
        - avg_speed: average of speed
        - vehicles_count: distinct count of entity_id
    """
    df = (
        df_silver
        .groupby("route_id")
        .agg(
            F.round(F.avg("speed"), 1).alias("speed_avg"),
            F.count_distinct("entity_id").alias("nb_vehicles")
        )
        .orderBy(F.col("speed_avg").desc())
    )
    print(df)
    return df


def calc_stats_by_route_and_hour(df_silver: DataFrame) -> DataFrame:
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
    df = (
        df_silver
        .withColumn("date", F.to_date("event_timestamp"))
        .groupby("date", F.hour("event_timestamp").alias("hour"), "route_id")
        .agg(
            F.round(F.avg("speed"), 1).alias("speed_avg"),
            F.count_distinct("entity_id").alias("nb_vehicles")
        )
        .orderBy(
            F.col("date"),
            F.col("hour"),
            *sort_by_natural_order(F.col("route_id"))
        )
    )
    print(df)
    return df


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


def build_gold_dataframes(df_silver: DataFrame) -> tuple[DataFrame, DataFrame]:
    """
    Helper to build both Gold-level DataFrames from Silver:
      - hourly metrics
      - daily activity
    """
    df_hourly = df_silver.withColumn("hour", F.hour("event_timestamp"))
    df_daily = df_silver.withColumn("day", F.to_date("event_timestamp"))
    return df_hourly, df_daily


if __name__ == "__main__":
    df_silver: DataFrame = get_df_silver()
    logg("Stats by route")
    stats_by_route = calc_stats_by_route_and_hour(df_silver)
    # stats_by_route = calc_stats_by_route(df_silver)
    stats_by_route.show(50)

    # logg(f"Building Gold metrics from Silver")
    # df_hourly, df_daily = build_gold_dataframes(df_silver)
    #
    # logg("Hourly route metrics sample:")
    # df_hourly.show(5, truncate=False)
    #
    # logg("Daily route activity sample:")
    # df_daily.show(5, truncate=False)
