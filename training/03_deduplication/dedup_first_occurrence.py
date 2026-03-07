"""
Simple deduplication: keep the first occurrence of each event_id (arrival order).
Concept: set to track seen ids, single pass.
"""
from rich import print

events = [
    {"event_id": "E1", "user_id": "U1", "timestamp": 1000, "event_type": "click"},
    {"event_id": "E2", "user_id": "U2", "timestamp": 1001, "event_type": "view"},
    {"event_id": "E1", "user_id": "U1", "timestamp": 1000, "event_type": "click"},
    {"event_id": "E3", "user_id": "U3", "timestamp": 1002, "event_type": "purchase"},
    {"event_id": "E2", "user_id": "U2", "timestamp": 1001, "event_type": "view"},
]


def deduplicate_first_occurrence(events: list[dict]) -> list[dict]:
    seen = set()
    result = []
    for event in events:
        if event["event_id"] not in seen:
            result.append(event)
            seen.add(event["event_id"])
    return result


print(deduplicate_first_occurrence(events))
