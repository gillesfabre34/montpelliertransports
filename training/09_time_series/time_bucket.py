"""
E — Time bucket (aggregate by time window)

STATEMENT
--------
Group events by a time bucket (e.g. by hour, by day): truncate or round timestamp to bucket start,
then aggregate (count, sum, etc.) per bucket. Used for: events per hour, orders per day, metrics by period.

Input:  list of events with timestamp (and optionally value to sum).
Output: list of dicts: bucket (e.g. hour or day id), count, and optionally sum of value.

The bucket number is: timestamp // bucket_seconds (integer division). Example: with bucket_seconds=3600,
timestamp 15000 gives bucket 15000 // 3600 = 4.
"""
from __future__ import annotations
from rich import print
from collections import defaultdict
from dataclasses import dataclass

# Events with timestamps (e.g. Unix seconds or milliseconds). Bucket by "hour" (e.g. ts // 3600).
EVENTS = [
    {"event_id": "e1", "timestamp": 3600, "amount": 10},
    {"event_id": "e2", "timestamp": 3700, "amount": 20},
    {"event_id": "e3", "timestamp": 7200, "amount": 5},
    {"event_id": "e4", "timestamp": 3650, "amount": 15},
    {"event_id": "e5", "timestamp": 7300, "amount": 25},
    {"event_id": "e6", "timestamp": 15000, "amount": 10},
]
# Example: bucket size 3600 (1 hour). Bucket 1: 3600,3700,3650 → count 3, sum 45. Bucket 2: 7200,7300 → count 2, sum 30. Bucket 4: 15000 → count 1, sum 10.
EXPECTED_TIME_BUCKET = [
    {"bucket": 1, "count": 3, "sum": 45},
    {"bucket": 2, "count": 2, "sum": 30},
    {"bucket": 4, "count": 1, "sum": 10},
]

@dataclass
class Bucket:
    count: int = 0
    sum: float = 0
    bucket: int = 0

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
    result = []
    buckets = defaultdict(Bucket)
    for event in events:
        num_bucket = event[timestamp_key] // bucket_seconds
        buckets[num_bucket].count += 1
        if value_key is not None:
            buckets[num_bucket].sum += event[value_key]
        buckets[num_bucket].bucket = num_bucket

    for bucket_id, bucket_events in buckets.items():
        bucket = {
            "bucket": bucket_id,
            "count": bucket_events.count,
        }
        if value_key != None:
            bucket["sum"] = bucket_events.sum
        result.append(bucket)
    return result


if __name__ == "__main__":
    print(aggregate_by_time_bucket(EVENTS, "timestamp", 3600, "amount"))
