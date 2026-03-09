"""
Exercise: time-bucketed route statistics with pandas on real vehicle data.

Goal
----
Using the real vehicle positions:
- create a time bucket (e.g. 15-minute or 1-hour windows) from event_timestamp
- compute per-(route_id, bucket) aggregations such as:
  - number of points
  - average speed

This mirrors the patterns from `09_time_series/time_bucket.py`, but applied
to the real schema with pandas.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Optional

import pandas as pd

from .exploration import load_vehicle_positions_pandas


def add_time_bucket_column(
    df: pd.DataFrame, freq: str = "15min"
) -> pd.DataFrame:
    """
    Add a time bucket column derived from event_timestamp.

    Example:
        - freq="15min" → buckets [00:00, 00:15, 00:30, ...]
        - freq="1H"    → hourly buckets

    Args:
        df: Input DataFrame with an event_timestamp column (datetime-like).
        freq: Pandas offset alias for the bucket size.

    Returns:
        A new DataFrame with an additional column, e.g. "event_bucket".
    """
    raise NotImplementedError(
        "Implement creation of a time bucket column from event_timestamp."
    )


def compute_route_time_bucket_stats(
    df: pd.DataFrame, freq: str = "15min"
) -> pd.DataFrame:
    """
    Compute per-(route_id, time bucket) statistics.

    Suggested metrics:
        - count of points
        - average speed

    Steps (suggestion):
        1. Call add_time_bucket_column(df, freq)
        2. Group by (route_id, event_bucket)
        3. Aggregate

    Returns:
        Aggregated DataFrame with one row per (route_id, event_bucket).
    """
    raise NotImplementedError(
        "Group by route_id and the time bucket column, then aggregate."
    )


def run_route_time_bucket_stats(
    path: Optional[str] = None, freq: str = "15min"
) -> pd.DataFrame:
    """
    Helper to load data and compute route time-bucketed stats.

    Args:
        path: Optional path override to the Parquet data.
        freq: bucket size passed to compute_route_time_bucket_stats.
    """
    df = load_vehicle_positions_pandas(path)
    return compute_route_time_bucket_stats(df, freq=freq)


if __name__ == "__main__":
    stats_df = run_route_time_bucket_stats(freq="15min")
    print("Sample of route time-bucketed stats:")
    print(stats_df.head())

