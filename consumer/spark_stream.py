"""Spark consumer."""

import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField
from pyspark.sql.functions import from_json, col
from delta import configure_spark_with_delta_pip

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_VEHICLE_POSITIONS_RAW,
    APP_NAME
)

load_dotenv()

AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCESS_KEY = os.getenv("AZURE_ACCESS_KEY")
def create_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(APP_NAME)
        # Packages
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2")
        # Delta Lake
        #.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        #.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # Azure Blob Storage
        .config(f"fs.azure.account.key.{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net", AZURE_ACCESS_KEY)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark




def read_spark():
    print("\n\nSTARTING SPARK\n\n")
    spark = create_spark()


    print(f"Spark version {spark.version}")

    df_raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_VEHICLE_POSITIONS_RAW)
        .option("startingOffsets", "latest")
        .load()
    )
    df_string = df_raw.selectExpr("CAST(value AS STRING) as value")

    vehicle_schema = StructType([
        StructField("entity_id", StringType()),
        StructField("trip_id", StringType()),
        StructField("route_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("bearing", DoubleType()),
        StructField("speed", DoubleType()),
        StructField("event_timestamp", LongType()),
        StructField("source", StringType()),
    ])

    df_parsed = df_string.select(
        from_json(col("value"), vehicle_schema).alias("data")
    )
    df_parsed = df_parsed.select("data.*")

    query = (
        df_parsed.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", False)
        .start()
    )
    query.awaitTermination()
    print(df_parsed)

if __name__ == "__main__":
    read_spark()



