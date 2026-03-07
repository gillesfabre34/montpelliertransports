"""
Salting: for keys identified as skewed, add a random suffix (#0, #1, …)
to spread load across partitions. Re-aggregate afterward (strip the #).
"""
from collections import defaultdict
from rich import print
from random import randint

from detect_skew import detect_skew

events = [
    {"event_id": "e1", "user_id": "u1"},
    {"event_id": "e2", "user_id": "u1"},
    {"event_id": "e3", "user_id": "u2"},
    {"event_id": "e4", "user_id": "u1"},
    {"event_id": "e5", "user_id": "u3"},
]


def salt_events(events: list[dict], key_field: str, threshold: float, salt_factor: int) -> list[dict]:
    skewed_keys = set(detect_skew(events, key_field, threshold))
    result = []
    for e in events:
        new_event = e.copy()
        if e[key_field] in skewed_keys:
            new_event[key_field] = f"{e[key_field]}#{randint(0, salt_factor - 1)}"
        result.append(new_event)
    return result


if __name__ == "__main__":
    print(salt_events(events, "user_id", 0.4, 3))
