"""
Exercise: Spark Bronze (batch) on real vehicle positions.

Goal
----
Use PySpark to:
- create a SparkSession (local)
- read Bronze-like data (mock Parquet/Delta) for vehicle positions
- inspect schema, partitions, and basic metrics

Data source
-----------
Same as for the pandas exercises:
- preferred: path from `MOCK_DATA_PATH` env variable
- fallback: local `consumer/mocks/` folder

Format:
- `MOCK_DATA_FORMAT=delta` → read as Delta table
- otherwise → read Parquet

Schema (Bronze-style, simplified)
---------------------------------
- entity_id: string
- trip_id: string
- route_id: string
- latitude: double
- longitude: double
- bearing: double
- speed: double
- event_timestamp: timestamp
- year: int
- month: int
- day: int
"""

from __future__ import annotations
from pyspark.sql import DataFrame
from consumer.mocks.mocks import get_mocks_path
from consumer.utils.spark import create_spark_session, read_batch


def print_bronze_overview(df: DataFrame) -> None:
    """
    Print a quick overview of the Bronze DataFrame.

    Suggested info:
        - schema (df.printSchema())
        - total number of rows
        - distinct count of entity_id, trip_id, route_id
        - sample of 5 rows
    """
    print("\nStart print_bronze_overview()\n")
    print("\nSCHEMA\n", df.schema)
    print("\nCOUNT\n", df.count())
    print("\nENTITIES\n", df.select("entity_id").distinct().count())
    print("\nTRIPS\n", df.select("trip_id").distinct().count())
    print("\nROUTES\n", df.select("route_id").distinct().count())
    print("\nSAMPLE\n", df.head(5))


if __name__ == "__main__":
    mocks_path = get_mocks_path()
    print(f"Using Bronze mocks path: {mocks_path}")

    spark = create_spark_session()
    df_bronze = read_batch(spark, path=mocks_path, fmt="parquet")

    print_bronze_overview(df_bronze)
