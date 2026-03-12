from typing import Optional

from pyspark.sql import SparkSession, DataFrame

from consumer.mocks.mocks import get_mocks_path


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
    return (
        spark.read
        .format(fmt)
        .load(path)
    )
