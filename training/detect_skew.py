from collections import defaultdict
from rich import print
from typing import List, Dict
import heapq

events = [
    {"event_id": "e1", "user_id": "u1"},
    {"event_id": "e2", "user_id": "u1"},
    {"event_id": "e3", "user_id": "u1"},
    {"event_id": "e4", "user_id": "u2"},
    {"event_id": "e5", "user_id": "u3"},
    {"event_id": "e6", "user_id": "u2"},
    {"event_id": "e7", "user_id": "u1"},
    {"event_id": "e8", "user_id": "u4"},
]


def detect_skew(events: list[dict], threshold: float) -> list[str]:
    total_events = 0
    events_by_user = defaultdict(int)

    for e in events:
        total_events += 1
        events_by_user[e["user_id"]] += 1

    result = [e for e in events_by_user if events_by_user[e] / total_events > threshold]
    return result


print(detect_skew(events, 0.4))