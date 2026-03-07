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
from __future__ import annotations

EVENTS = [
    {"event_id": "E1", "user_id": "U1", "timestamp": 1000, "event_type": "click"},
    {"event_id": "E2", "user_id": "U2", "timestamp": 1001, "event_type": "view"},
    {"event_id": "E1", "user_id": "U1", "timestamp": 1005, "event_type": "click"},
    {"event_id": "E3", "user_id": "U3", "timestamp": 1002, "event_type": "purchase"},
    {"event_id": "E2", "user_id": "U2", "timestamp": 1008, "event_type": "view"},
    {"event_id": "E1", "user_id": "U1", "timestamp": 1003, "event_type": "click"},
    {"event_id": "E4", "user_id": "U4", "timestamp": 1010, "event_type": "click"},
]
# Expected: E1→1005, E2→1008, E3→1002, E4→1010


def deduplicate_keep_last(events: list[dict]) -> list[dict]:
    """
    Keep the latest event per event_id (order: timestamp descending).

    Args:
        events: list of dicts with at least "event_id" and "timestamp".

    Returns:
        deduplicated events (one per event_id, the most recent).
    """
    # To implement (pure Python: partition → sort → first)
    raise NotImplementedError
