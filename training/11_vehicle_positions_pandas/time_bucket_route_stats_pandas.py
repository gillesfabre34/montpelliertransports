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
import pandas as pd
from pathlib import Path
from exploration import load_vehicle_positions
from geopy.distance import geodesic


def add_time_bucket_column(
    df: pd.DataFrame, freq: str = "15min"
) -> pd.DataFrame:
    """
    Add a time bucket column derived from event_timestamp.
    """
    df["event_timestamp"] = pd.to_datetime(df["event_timestamp"])
    df["event_bucket"] = df["event_timestamp"].dt.floor(freq)
    return df


def calc_stats_by_time_bucket(df: pd.DataFrame, freq: str = "10min") -> None:
    """
    Helper to load data and compute route time-bucketed stats.
        freq: bucket size passed to compute_route_time_bucket_stats.
    """
    df = add_time_bucket_column(df, freq=freq)
    stats_by_bucket = (
        df.groupby(["route_id", "event_bucket"])
        .agg(
            nb_entities=("entity_id", "nunique"),
            speed_mean=("speed", "mean")
        )
        .reset_index()
    )
    root = Path(__file__).resolve().parents[2]
    output_path = root / 'consumer/mocks/bronze_2026_3_2/outputs/time_bucket_stats.parquet'
    stats_by_bucket.to_parquet(output_path, index=False)
    print("Sample of route time-bucketed stats:\n",stats_by_bucket.head())


def calc_distances(df: pd.DataFrame) -> None:
    df["previous_lat"] = df.groupby("entity_id")["latitude"].shift(1)
    df["previous_long"] = df.groupby("entity_id")["longitude"].shift(1)

    mask = df[["previous_lat", "previous_long", "latitude", "longitude"]].notna().all(axis=1)
    df.loc[mask, "distance"] = df.loc[mask].apply(
        lambda row:
            round(
                geodesic(
                    (row["previous_lat"], row["previous_long"]),
                    (row["latitude"], row["longitude"])
                ).km
            , 3),
        axis=1
    )
    df = df.drop(columns=["previous_lat", "previous_long"])
    # print("Sample of route with distances:\n", df.head())
    print("Sample of route with distances END:\n", df.tail())


if __name__ == "__main__":
    df = load_vehicle_positions(2026,3,2)
    # calc_stats_by_time_bucket(df, freq="5min")
    calc_distances(df)

