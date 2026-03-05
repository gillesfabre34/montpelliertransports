from collections import defaultdict
from rich import print
from detect_skew import detect_skew
from typing import List, Dict
from random import randint


events = [
    {"event_id": "e1", "user_id": "u1"},
    {"event_id": "e2", "user_id": "u1"},
    {"event_id": "e3", "user_id": "u2"},
    {"event_id": "e4", "user_id": "u1"},
    {"event_id": "e5", "user_id": "u3"},
]


def salt_events(events: list[dict], salt_factor: int) -> list[dict]:
    result: List[Dict]  = []
    skews = detect_skew(events, 0.4)

    for e in events:
        new_event = e.copy()
        if e["user_id"] in skews:
            new_event["user_id"] += f"#{str(randint(0, salt_factor - 1))}"
        print(f"new_user_id {new_event}")
        result.append(new_event)

    return result


print(salt_events(events, 3))