"""
Logical partitioning: assign events to partitions by hash of the key (e.g. user_id).
Concept: hash(key) % num_partitions → same key always in the same partition.
"""
from collections import defaultdict
from rich import print

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


def partition_by_key(events: list[dict], key_field: str, num_partitions: int) -> dict[int, list[dict]]:
    partitions: dict[int, list[dict]] = defaultdict(list)
    for event in events:
        key = event[key_field]
        partition_id = hash(key) % num_partitions
        partitions[partition_id].append(event)
    return dict(partitions)


if __name__ == "__main__":
    partitions = partition_by_key(events, "user_id", 3)
    for pid, part in partitions.items():
        print(f"Partition {pid}:", part)
