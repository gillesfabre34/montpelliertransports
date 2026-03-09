import os
import tempfile
from functools import reduce
from rich import print
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobProperties
from azure.core.paging import ItemPaged
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional
from pyspark.sql import SparkSession, DataFrame
from io import BytesIO


_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / ".env")
spark = SparkSession.builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


def get_partition(year: Optional[int] = None,
                  month: Optional[int] = None,
                  day: Optional[int] = None) -> str:
    root = os.getenv("AZURE_PARTITIONS_ROOT")
    if year is None:
        return root + '/'
    elif month is None:
        return root + 'year=' + str(year) + '/'
    elif day is None:
        return root + 'year=' + str(year) + '/month=' + str(month) + '/'
    else:
        return root + 'year=' + str(year) + '/month=' + str(month) + '/day=' + str(day) + '/'


def get_bronze_container() -> ContainerClient:
    blob_service_client = BlobServiceClient(os.getenv("AZURE_ACCOUNT_URL"), os.getenv("AZURE_ACCESS_KEY"))
    return blob_service_client.get_container_client(os.getenv("AZURE_CONTAINER_BRONZE"))


def get_blobs(partition_path: str) -> ItemPaged[BlobProperties]:
    return get_bronze_container().list_blobs(name_starts_with=partition_path)


def get_parquet_blobs_in_memory(partition_path: str) -> list[BytesIO]:
    list_blobs = get_blobs(partition_path)
    parquet_buffers = []
    for blob in list_blobs:
        if blob.name.endswith(".parquet"):
            content = get_bronze_container().download_blob(blob.name).readall()
            parquet_buffers.append(BytesIO(content))
    return parquet_buffers


def get_dataframe_from_blobs(partition_path: str) -> DataFrame:
    parquet_buffers = get_parquet_blobs_in_memory(partition_path)
    dfs = []
    for parquet_buffer in parquet_buffers:
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp.write(parquet_buffer.getbuffer().tobytes())
        tmp.close()
        df = spark.read.parquet(tmp.name)
        dfs.append(df)

    if dfs:
        merged_df = reduce(DataFrame.unionByName, dfs)
        return merged_df
    else:
        return spark.createDataFrame([], schema=None)


def create_mock(year: Optional[int] = None,
              month: Optional[int] = None,
              day: Optional[int] = None) -> None:
    file_name = 'bronze_' + str(year) + '_' + str(month) + '_' + str(day)
    file_path = 'mocks/' + file_name
    partition: str = get_partition(2026, 3, 2)
    df = get_dataframe_from_blobs(partition)
    df.printSchema()
    df.show(20, truncate=False)
    df.write.mode("overwrite").parquet(file_path)


if __name__ == "__main__":
    create_mock(2026, 3, 2)
    print(f"create_mock() done")