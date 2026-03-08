"""
E — Time bucket (aggregate by time window)

STATEMENT
--------
Group events by a time bucket (e.g. by hour, by day): truncate or round timestamp to bucket start,
then aggregate (count, sum, etc.) per bucket. Used for: events per hour, orders per day, metrics by period.

Input:  list of events with timestamp (and optionally value to sum).
Output: list of dicts: bucket (e.g. hour or day id), count, and optionally sum of value.
"""
from rich import print

# Events with timestamps (e.g. Unix seconds or milliseconds). Bucket by "hour" (e.g. ts // 3600).
EVENTS = [
    {"event_id": "e1", "timestamp": 3600, "amount": 10},
    {"event_id": "e2", "timestamp": 3700, "amount": 20},
    {"event_id": "e3", "timestamp": 7200, "amount": 5},
    {"event_id": "e4", "timestamp": 3650, "amount": 15},
    {"event_id": "e5", "timestamp": 7300, "amount": 25},
]
# Example: bucket size 3600 (1 hour). Bucket 1: 3600,3700,3650 → count 3, sum 45. Bucket 2: 7200,7300 → count 2, sum 30.


def aggregate_by_time_bucket(
    events: list[dict],
    timestamp_key: str,
    bucket_seconds: int,
    value_key: str | None = None,
) -> list[dict]:
    """
    Group events by time bucket (timestamp // bucket_seconds). Return one row per bucket with count and optional sum.

    Args:
        events: list of dicts with timestamp_key (numeric) and optionally value_key.
        timestamp_key: e.g. "timestamp".
        bucket_seconds: bucket size in seconds (e.g. 3600 for hourly).
        value_key: if provided, also sum this field per bucket.

    Returns:
        list of dicts: bucket (start time or index), count, and sum (if value_key given).
    """
    raise NotImplementedError


if __name__ == "__main__":
    print(aggregate_by_time_bucket(EVENTS, "timestamp", 3600, "amount"))
