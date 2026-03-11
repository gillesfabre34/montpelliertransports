from pyspark.sql import SparkSession, DataFrame
from delta import configure_spark_with_delta_pip
from consumer.mocks.mocks import get_mocks_path
import os
from typing import Optional
import pyarrow.parquet as pq
from pathlib import Path


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
        # builder = (
        #     SparkSession.builder
        #     .appName(os.getenv("AZURE_APP_NAME"))
        #     .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2")
        #     .config(f"fs.azure.account.key.{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.blob.core.windows.net",
        #             os.getenv('AZURE_ACCESS_KEY'))
        # )
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


def read_batch(
        spark: SparkSession,
        path: Optional[str] = None,
        fmt: Optional[str] = "delta",
) -> DataFrame:
    """
    Read data in batch mode using Spark.

    Args:
        spark: SparkSession.
        path: Optional explicit path. If None, uses resolve_bronze_source_path().
        fmt: Optional format override. If None, uses MOCK_DATA_FORMAT env var:
             - "delta" → Spark Delta reader
             - anything else → Parquet

    Returns:
        DataFrame with the Bronze schema.
    """
    if path is None:
        path = get_mocks_path()
    df = (
        spark.read
        .format(fmt)
        .load(path)
    )
    return df

# root = Path(__file__).resolve().parents[2]
# print("root", root)
# parquet_file_path = root / "consumer" / "mocks" / "bronze_2026_3_2" / "raw" / "part-00007-2401bde3-3cd7-4895-93b6-475b2402244a-c000.snappy.parquet"
# print("parquet_file_path", parquet_file_path)
#
# table = pq.read_table(parquet_file_path)
# print("table.schema", table.schema)