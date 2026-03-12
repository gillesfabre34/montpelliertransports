from __future__ import annotations

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.window import Window as W

from consumer.bronze.bronze_utils import read_batch
from consumer.mocks.mocks import get_mocks_path
from consumer.utils.logg import logg
from consumer.utils.spark import create_spark_session


def get_df_bronze() -> DataFrame:
    mocks_path = get_mocks_path()
    spark = create_spark_session()
    df_bronze = read_batch(spark, mocks_path, "parquet")
    logg("df_bronze schema", df_bronze.schema)
    return df_bronze


def get_last_positions(df: DataFrame) -> DataFrame:
    """
    Get last vehicle positions by (entity_id, trip_id)
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
    Apply basic Silver-level data quality rules
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
