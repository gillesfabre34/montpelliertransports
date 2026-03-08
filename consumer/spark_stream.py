"""Spark consumer."""

import os
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField
from pyspark.sql.functions import from_json, col, to_timestamp, year, month, dayofmonth
from delta import configure_spark_with_delta_pip

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_VEHICLE_POSITIONS_RAW,
)

# Load .env from project root so MOCK_DATA_PATH and Azure vars are always found
_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / ".env")

AZURE_STORAGE_ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
AZURE_ACCESS_KEY = os.getenv("AZURE_ACCESS_KEY")
AZURE_APP_NAME = os.getenv("AZURE_APP_NAME")
AZURE_CONTAINER_BRONZE = os.getenv("AZURE_CONTAINER_BRONZE")
AZURE_FOLDER_BRONZE = os.getenv("AZURE_FOLDER_BRONZE")
AZURE_FOLDER_CHECKPOINT = os.getenv("AZURE_FOLDER_CHECKPOINT")

# Mock mode: read from Parquet/Delta path (local or Azure) instead of Kafka. No Kafka required.
MOCK_DATA_PATH = os.getenv("MOCK_DATA_PATH")  # e.g. "mocks/" or "wasbs://container@account.../bronze/year=2025/month=3/day=8/"
MOCK_DATA_FORMAT = (os.getenv("MOCK_DATA_FORMAT") or "parquet").lower()  # "parquet" or "delta"

BRONZE_PATH = f"wasbs://{AZURE_CONTAINER_BRONZE}@{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{AZURE_FOLDER_BRONZE}/"
CHECKPOINT_PATH = f"wasbs://{AZURE_CONTAINER_BRONZE}@{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net/{AZURE_FOLDER_CHECKPOINT}/"

def create_spark() -> SparkSession:
    builder = (
        SparkSession.builder
        .appName(AZURE_APP_NAME)
        # Packages
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2")
        # Azure Blob Storage
        .config(f"fs.azure.account.key.{AZURE_STORAGE_ACCOUNT_NAME}.blob.core.windows.net", AZURE_ACCESS_KEY)
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_spark():
    print("\n\nSTARTING SPARK\n\n")
    spark = create_spark()

    print(f"\nSpark version {spark.version}\n")

    if MOCK_DATA_PATH:
        # Batch mode: read from Parquet or Delta (local or Azure). No Kafka.
        print(f"Mock mode: reading from {MOCK_DATA_PATH} (format={MOCK_DATA_FORMAT})\n")
        if MOCK_DATA_FORMAT == "delta":
            df_partitioned = spark.read.format("delta").load(MOCK_DATA_PATH)
        else:
            df_partitioned = spark.read.parquet(MOCK_DATA_PATH)
        print(f"Read {df_partitioned.count()} rows.")
        df_partitioned.show(5, truncate=False)
        return

    # Streaming mode: Kafka -> parse -> Delta
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
    df_parsed = df_parsed\
        .select("data.*")\
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))

    df_partitioned = df_parsed\
        .withColumn("year", year("event_timestamp"))\
        .withColumn("month", month("event_timestamp"))\
        .withColumn("day", dayofmonth("event_timestamp"))

    delta_query = (
        df_partitioned.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("year", "month", "day")
        .start(BRONZE_PATH)
    )

    delta_query.awaitTermination()

if __name__ == "__main__":
    read_spark()



