"""
Exercise: basic exploration of real vehicle positions data with pandas.

Goal
----
Read vehicle positions data (Parquet) into a pandas DataFrame and perform:
- schema inspection (dtypes, head)
- simple descriptive stats
- first aggregations by route

Data source
-----------
We reuse the Bronze-like mock data stored under `consumer/mocks/` or an
Azure/Blob path configured via environment variables.

Priority is:
1. `MOCK_DATA_PATH` env var (can be local path or Azure path).
2. Fallback to local `consumer/mocks/` folder (Parquet files downloaded from Azure).

Columns (typical schema)
------------------------
- entity_id: string
- trip_id: string
- route_id: string
- latitude: double
- longitude: double
- bearing: double
- speed: double
- event_timestamp: timestamp
- source: string (optional, depends on mock)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Optional
from rich import print
from consumer.blobs import get_dataframe_from_blobs

import pandas as pd



def get_mocks_path(location: bool) -> str:
    """
    Resolve the path to the mock Bronze data used for this exercise.

    Priority:
    1. Environment variable `MOCK_DATA_PATH` (can be local or Azure-style path).
    2. Local `consumer/mocks/` folder (project-relative).
    """
    if location:
        project_root = Path(__file__).resolve().parents[2]
        local_mocks = project_root / "consumer" / "mocks"
        return str(local_mocks)
    else:
        env_path = os.getenv("MOCK_DATA_PATH")
        if env_path:
            return env_path
        else:
            raise "Path not found"


def get_parquet_file_name(year: Optional[int] = None,
              month: Optional[int] = None,
              day: Optional[int] = None) -> str:
    return 'bronze_' + str(year) + '_' + str(month) + '_' + str(day)


def get_parquet_path(path, year: Optional[int] = None,
              month: Optional[int] = None,
              day: Optional[int] = None) -> str:
    return path + '/' + get_parquet_file_name(year, month, day)


def load_vehicle_positions_pandas(path: Optional[str] = None) -> pd.DataFrame:
    """
    Load vehicle positions from Parquet into a pandas DataFrame.

    Args:
        path: Optional explicit path. If None, uses resolve_mock_path().

    Returns:
        pandas.DataFrame with the vehicle positions schema.
    """
    final_path = path or get_mocks_path()
    # NOTE: For Azure paths, your environment must be configured so that
    # pandas/pyarrow can access the storage. For local mocks, a simple Parquet
    # folder under consumer/mocks/ is enough.
    return pd.read_parquet(final_path)


def explore_basic(df: pd.DataFrame) -> None:
    """
    Basic exploration of the vehicle positions DataFrame.

    Tasks (to implement):
    - Print the number of rows.
    - Print the list of columns and their dtypes.
    - Show the first 5 rows.
    - Compute and print simple descriptive stats for the `speed` column.
    """
    print("df.size", df.size)
    print("columns", df.columns)
    print("head", df.head(5))
    print("speed avg", round(df["speed"].mean(), 2))

    return None


def explore_by_route(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute simple aggregations per route_id.

    Expected aggregations (suggestion):
    - number of records per route_id
    - average speed per route_id

    Returns:
        A DataFrame indexed by route_id (or with a route_id column) containing
        the aggregations.
    """
    raise NotImplementedError("Group by route_id and aggregate.")


if __name__ == "__main__":
    mocks_path = get_mocks_path(True)
    parquet_path = get_parquet_path(mocks_path, 2026, 3, 2)
    df_positions = load_vehicle_positions_pandas(parquet_path)

    # 1) Basic exploration (side effects: prints)
    explore_basic(df_positions)

    # # 2) Aggregations per route (returned DataFrame)
    # route_stats = explore_by_route(df_positions)
    # print("\nRoute-level stats (sample):")
    # print(route_stats.head())


    # partition = get_partition(2026, 3, 2)
    #
    # df_positions = get_dataframe_from_blobs(partition)
    # df_positions.show(10, truncate=True)