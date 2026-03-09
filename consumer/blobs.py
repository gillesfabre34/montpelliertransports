import os
from rich import print
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobProperties
from azure.core.paging import ItemPaged
from dotenv import load_dotenv
from pathlib import Path
from typing import Optional

_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / ".env")


def get_partition(year: Optional[int] = None,
                  month: Optional[int] = None,
                  day: Optional[int] = None) -> str:
    root = os.getenv("AZURE_PARTITIONS_ROOT")
    if year is None:
        return root
    elif month is None:
        return root + 'year=' + str(year)
    elif day is None:
        return root + 'year=' + str(year) + '/month=' + str(month)
    else:
        return root + 'year=' + str(year) + '/month=' + str(month) + '/day=' + str(day)


def get_blobs(partition_path: str) -> ItemPaged[BlobProperties]:
    blob_service_client = BlobServiceClient(os.getenv("AZURE_ACCOUNT_URL"), os.getenv("AZURE_ACCESS_KEY"))
    print(f"blob_service_client", blob_service_client)
    bronze_container: ContainerClient = blob_service_client.get_container_client(os.getenv("AZURE_CONTAINER_BRONZE"))
    list_blobs = bronze_container.list_blobs(name_starts_with=partition_path)
    print(f"list_blobs", list_blobs)
    for blob in list_blobs:
        print(blob)
    return list_blobs


if __name__ == "__main__":
    partition = get_partition(2026, 3, 2)
    get_blobs(partition)
    print(f"get_mocks done")