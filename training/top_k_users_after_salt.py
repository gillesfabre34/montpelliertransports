import heapq
from collections import defaultdict
from rich import print
from detect_skew import detect_skew
from typing import List, Dict, Tuple
from random import randint
from salt_events import salt_events


events = [
    {"event_id": "e1", "user_id": "u1#0"},
    {"event_id": "e2", "user_id": "u1#1"},
    {"event_id": "e3", "user_id": "u2"},
    {"event_id": "e4", "user_id": "u1#2"},
    {"event_id": "e5", "user_id": "u3"},
    {"event_id": "e6", "user_id": "u2"},
    {"event_id": "e7", "user_id": "u1#1"},
]


def top_k_users_after_salt(events: List[Dict], k: int) -> list[tuple[str, int]]:
    salted_events = salt_events(events, 3)

    users = defaultdict(int)

    for event in salted_events:
        user_id = event["user_id"].split("#")[0]
        users[user_id] += 1

    heap: List[Tuple[int, str]] = []

    for user_id, nb_events in users.items():
        if len(heap) < k:
            heapq.heappush(heap, (nb_events, user_id))
        elif nb_events > heap[0][0]:
            heapq.heappushpop(heap, (nb_events, user_id))

    return sorted(
        [(user, count) for count, user in heap],
        key=lambda x: x[1],
        reverse=True
    )



print(top_k_users_after_salt(events, 2))