"""GTFS-RT consumer."""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType, LongType, StructField
from pyspark.sql.functions import from_json, col

from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_VEHICLE_POSITIONS_RAW,
)


def read_spark():
    spark = (
        SparkSession.builder
        .appName("montpelliertransports")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2"
        )
        .getOrCreate()
    )

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



