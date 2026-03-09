"""
Exercise: D1-style deduplication on real vehicle positions with pandas.

Goal
----
For each (entity_id, trip_id) pair, keep only the latest record according
to event_timestamp.

This mirrors the "KEEP LAST" (D1) pattern already implemented in
`03_deduplication/dedup_keep_last.py`, but applied to the real vehicle
positions schema using pandas instead of pure Python lists of dicts.

Pattern (SQL-style)
-------------------
ROW_NUMBER() OVER (
  PARTITION BY entity_id, trip_id
  ORDER BY event_timestamp DESC
)
WHERE row_number = 1

Here you will implement the equivalent using pandas:
  - sort_values
  - drop_duplicates(keep="first" or "last")
"""

from __future__ import annotations

from typing import Optional

import pandas as pd

from .exploration import load_vehicle_positions_pandas


def deduplicate_keep_last_entity_trip(df: pd.DataFrame) -> pd.DataFrame:
    """
    Deduplicate vehicle positions by (entity_id, trip_id), keeping the latest
    record by event_timestamp.

    Args:
        df: Input DataFrame with at least the following columns:
            - entity_id
            - trip_id
            - event_timestamp

    Returns:
        A DataFrame where each (entity_id, trip_id) appears at most once, with
        the row having the most recent event_timestamp.
    """
    raise NotImplementedError(
        "Implement D1 deduplication with pandas (KEEP LAST per "
        "(entity_id, trip_id))."
    )


def run_dedup(path: Optional[str] = None) -> pd.DataFrame:
    """
    Helper to load data from Parquet and apply the deduplication function.

    Args:
        path: Optional explicit path to Parquet data. If None, uses the
              default resolution from load_vehicle_positions_pandas().

    Returns:
        Deduplicated DataFrame.
    """
    df = load_vehicle_positions_pandas(path)
    return deduplicate_keep_last_entity_trip(df)


if __name__ == "__main__":
    result = run_dedup()
    print("Sample of deduplicated vehicle positions:")
    print(result.head())

