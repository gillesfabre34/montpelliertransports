from collections import defaultdict
from rich import print
from typing import List, Dict
import heapq

events = [
    {"event_id": "e1", "user_id": "u1", "event_type": "click"},
    {"event_id": "e2", "user_id": "u2", "event_type": "view"},
    {"event_id": "e3", "user_id": "u3", "event_type": "click"},
    {"event_id": "e4", "user_id": "u1", "event_type": "purchase"},
    {"event_id": "e5", "user_id": "u4", "event_type": "view"},
    {"event_id": "e6", "user_id": "u2", "event_type": "click"},
    {"event_id": "e7", "user_id": "u5", "event_type": "view"},
    {"event_id": "e8", "user_id": "u1", "event_type": "click"},
]


def partition_events(events: list[dict], num_partitions: int) -> dict:
    hashes: dict[int, list[dict]] = defaultdict(list)
    for event in events:
        user_id = event["user_id"]
        hashed = hash(user_id) % num_partitions
        hashes[hashed].append(event)
        print(f"hashed {event['user_id']}: {hashed}")
    return hashes


partitions = partition_events(events, 3)

for p in partitions:
    print(f"Partition {p}")
    for e in partitions[p]:
        print(e)

