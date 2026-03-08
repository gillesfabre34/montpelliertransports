"""
D1 — Event deduplication (Kafka / bronze → silver pattern)

STATEMENT
--------
In stream ingestion (Kafka, CDC), the same event_id can appear multiple times
(retries, replays, updates). We want to keep **one** record per event_id:
the **latest** by timestamp (most recent).

Equivalent SQL pattern:
  ROW_NUMBER() OVER (
    PARTITION BY event_id
    ORDER BY timestamp DESC
  )
  then keep WHERE row_number = 1

Implement in pure Python: group by event_id, sort by timestamp descending,
keep the first row of each group.

Input:  list of events with event_id, timestamp (and optionally user_id, etc.).
Output: deduplicated events (latest per event_id), stable order.
"""
from rich import print
from collections import defaultdict

EVENTS = [
    # User U1 has two distinct events: E1 (order) and E5 (click). E1 is updated twice (status changes).
    {"event_id": "E1", "user_id": "U1", "timestamp": 1000, "event_type": "order", "status": "created"},
    {"event_id": "E2", "user_id": "U2", "timestamp": 1001, "event_type": "view"},
    {"event_id": "E1", "user_id": "U1", "timestamp": 1005, "event_type": "order", "status": "paid"},
    {"event_id": "E3", "user_id": "U3", "timestamp": 1002, "event_type": "purchase"},
    {"event_id": "E2", "user_id": "U2", "timestamp": 1008, "event_type": "view"},
    {"event_id": "E1", "user_id": "U1", "timestamp": 1003, "event_type": "order", "status": "pending"},
    {"event_id": "E4", "user_id": "U4", "timestamp": 1010, "event_type": "click"},
    {"event_id": "E5", "user_id": "U1", "timestamp": 1009, "event_type": "click"},
]
# Same event_id can have different payloads over time (e.g. E1: status created → pending → paid).
# Same user can have multiple event_ids (U1 has E1 and E5). We keep one row per event_id (latest).

# Exact expected result of deduplicate_keep_last(EVENTS) (one row per event_id, latest by timestamp)
EXPECTED_RESULT = [
    {"event_id": "E1", "user_id": "U1", "timestamp": 1005, "event_type": "order", "status": "paid"},
    {"event_id": "E2", "user_id": "U2", "timestamp": 1008, "event_type": "view"},
    {"event_id": "E3", "user_id": "U3", "timestamp": 1002, "event_type": "purchase"},
    {"event_id": "E4", "user_id": "U4", "timestamp": 1010, "event_type": "click"},
    {"event_id": "E5", "user_id": "U1", "timestamp": 1009, "event_type": "click"},
]


def deduplicate_keep_last(events: list[dict]) -> list[dict]:
    """
    Keep the latest event per event_id (order: timestamp descending).

    Args:
        events: list of dicts with at least "event_id" and "timestamp".

    Returns:
        deduplicated events (one per event_id, the most recent).
    """
    events_by_id = defaultdict(list)
    for event in events:
        event_id = event["event_id"]
        events_by_id[event_id].append(event.copy())

    result = []
    for duplicates in events_by_id.values():
        last_event = sorted(duplicates, key=lambda x: x["timestamp"], reverse=True)[0]
        result.append(last_event)

    return result


print(deduplicate_keep_last(EVENTS))