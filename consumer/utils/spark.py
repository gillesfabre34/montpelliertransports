import os
from pathlib import Path

from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.column import Column


def create_spark_session(use_kafka: bool = False) -> SparkSession:
    if use_kafka:
        packages = ",".join([
            "io.delta:delta-spark_2.13:4.0.1",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2",
            "org.apache.hadoop:hadoop-azure:3.3.4",
            "com.microsoft.azure:azure-storage:8.6.6",
        ])
        builder = (
            SparkSession.builder
            .appName(os.getenv("AZURE_APP_NAME"))
            .config("spark.jars.packages", packages)
            .config(
                f"fs.azure.account.key.{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net",
                os.getenv("AZURE_ACCESS_KEY"),
            )
            .config(
                "spark.sql.extensions",
                "io.delta.sql.DeltaSparkSessionExtension",
            )
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
        )
        spark = builder.getOrCreate()
    else:
        builder = (
            SparkSession.builder
            .appName("montpeliertransports-local")
            .master("local[*]")
            .config("spark.ui.showConsoleProgress", "false")
        )
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def haversine_distance(lat1: Column, lon1: Column, lat2: Column, lon2: Column) -> Column:
    """
    Returns distance between 2 points GPS (lat/lon) in meters.
    Returns NULL if lat1 or lon1 is NULL.
    """
    R = 6371000  # rayon de la Terre en mètres
    lat1_rad = F.radians(lat1)
    lon1_rad = F.radians(lon1)
    lat2_rad = F.radians(lat2)
    lon2_rad = F.radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = F.pow(F.sin(dlat / 2), 2) + F.cos(lat1_rad) * F.cos(lat2_rad) * F.pow(F.sin(dlon / 2), 2)
    c = 2 * F.asin(F.sqrt(a))

    return F.when(lat1.isNull() | lon1.isNull(), None).otherwise(R * c)


if __name__ == "__main__":
    spark = create_spark_session(use_kafka=False)
    root = Path(__file__).resolve().parents[2]
    print("\nroot", root)
    parquet_file_path = root / "consumer" / "mocks" / "bronze_2026_03_11" / "raw" / "part-00000-0a277a9b-6407-40b2-acd0-3d5e019a893f.c000.snappy.parquet"
    print("\nparquet_file_path", parquet_file_path)
    df = spark.read.parquet(str(parquet_file_path))
    df.printSchema()
    df.show(5, truncate=False)
