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
from pyspark.sql.window import Window as W

from consumer.gold.gold_utils import get_df_silver
from consumer.utils.spark import haversine_distance
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
    return (
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


def find_stops(df_silver: DataFrame) -> DataFrame:
    """
    Find the terminus of the different routes
    """
    window = W.partitionBy("entity_id", "route_id").orderBy("event_timestamp")
    df = (
        df_silver
        .select(["entity_id", "route_id", "event_timestamp", "latitude", "longitude", "speed"])
        .withColumn("latitude", F.round("latitude", 5))
        .withColumn("longitude", F.round("longitude", 5))
        # .filter((F.col("route_id") == "14") | (F.col("route_id") == "16"))
        .orderBy("entity_id", "route_id", "event_timestamp")
        .withColumn("is_stop", (F.col("speed") == 0).cast('int'))
        .withColumn("was_stopped", (F.lag("is_stop").over(window) == 1).cast('int'))
        .withColumn("will_move", (F.lead("is_stop").over(window) == 0).cast('int'))
        .withColumn("restarted",
                    ((F.col("is_stop") == 0) & (F.col("was_stopped") == 1) & (F.col("will_move") == 1)).cast('int'))
        .withColumn("stop_number", F.sum("restarted").over(window.rowsBetween(W.unboundedPreceding, 0)))
        .filter(F.col("speed") == 0)
        .drop("event_timestamp", "is_stop", "speed", "was_stopped", "will_move", "restarted")
        .groupBy("entity_id", "route_id", "stop_number")
        .agg(F.round(F.avg("latitude"), 5).alias("latitude"), F.round(F.avg("longitude"), 5).alias("longitude"))
        .drop("entity_id", "stop_number")
        .sort("latitude", "longitude")
    )
    w_order_by_coors = W.partitionBy("route_id").orderBy("latitude", "longitude")
    df = (
        df
        .withColumn("previous_latitude", F.lag("latitude").over(w_order_by_coors))
        .withColumn("previous_longitude", F.lag("longitude").over(w_order_by_coors))
        .withColumn(
            "distance",
            F.round(
                haversine_distance(
                    F.col("latitude"),
                    F.col("longitude"),
                    F.col("previous_latitude"),
                    F.col("previous_longitude"),
                )
            )
        )
        .withColumn("is_not_same_stop", ((F.col("distance") >= 100) & (F.isnotnull("previous_latitude"))).cast('int'))
        .withColumn("stop_number",
                    F.sum("is_not_same_stop").over(w_order_by_coors.rowsBetween(W.unboundedPreceding, 0)))
        .groupBy("route_id", "stop_number")
        .agg(F.round(F.avg("latitude"), 5).alias("latitude"), F.round(F.avg("longitude"), 5).alias("longitude"))
        .withColumn("stop_number", F.concat(F.col("route_id"), F.lit("-"), F.col("stop_number")).alias("stop_number"))
        .orderBy("route_id", "stop_number")
    )

    logg("Added cols")
    return df


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
    stats = find_stops(df_silver)
    # stats = calc_stats_by_route_and_hour(df_silver)
    # stats = calc_stats_by_route(df_silver)
    stats.show(50)

    # logg(f"Building Gold metrics from Silver")
    # df_hourly, df_daily = build_gold_dataframes(df_silver)
    #
    # logg("Hourly route metrics sample:")
    # df_hourly.show(5, truncate=False)
    #
    # logg("Daily route activity sample:")
    # df_daily.show(5, truncate=False)
