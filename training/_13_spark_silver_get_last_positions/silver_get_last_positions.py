"""
Exercise: Spark Silver – deduplication and basic data quality on vehicle positions.

Goal
----
Starting from Bronze-like vehicle positions (same schema as _12_spark_bronze_batch),
build a Silver-level DataFrame that:
- deduplicates events per (entity_id, trip_id) keeping the latest record
  by event_timestamp (D1 "KEEP LAST" pattern)
- applies simple quality rules (e.g. valid coordinates, reasonable speeds)

This mirrors:
- _03_deduplication/dedup_keep_last.py (list/dict version)
- _11_vehicle_positions_pandas/dedup_latest_by_entity_trip_pandas.py (pandas)
but implemented here with PySpark and window functions.
"""

from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window as W

from consumer.silver.silver_utils import get_df_bronze
from utils.tools import logg


def get_last_positions(df: DataFrame) -> DataFrame:
    """
    Get last vehicle positions by (entity_id, trip_id) using a Spark
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
    window_spec = W.partitionBy("entity_id", "trip_id").orderBy(F.col("event_timestamp").desc())
    df = (df
          .withColumn("event_rank", F.row_number()
                      .over(window_spec))
          .filter(F.col("event_rank") == 1)
          .drop("event_rank")
          )
    logg("df values\n")
    df.show(2)
    return df


def build_silver_dataframe(df_bronze: DataFrame) -> DataFrame:
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
    return (
        df_bronze
        .filter(F.col("entity_id").isNotNull())
        .filter(F.col("event_timestamp").between(F.lit("2026-03-11"), F.current_timestamp()))
        .withColumn(
            "speed",
            F.when(F.col("speed").between(0, 100), F.col("speed"))
        )
        .withColumn(
            "longitude",
            F.when(F.col("longitude").between(0, 360), F.col("longitude"))
        )
        .withColumn(
            "latitude",
            F.when(F.col("latitude").between(-90, 90), F.col("latitude"))
        )
        .withColumn(
            "bearing",
            F.when(F.col("bearing").between(0, 360), F.col("bearing"))
        )
    )


if __name__ == "__main__":
    df_bronze = get_df_bronze()
    df_silver = get_last_positions(df_bronze)
    # df_silver = build_silver_dataframe(df_bronze)
    logg("Silver DataFrame sample:")
    df_silver.show(5, truncate=False)
