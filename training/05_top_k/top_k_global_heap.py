"""
Global top-K: the K records with the largest value (e.g. the K most active users).
Concept: heap of size K, heappushpop to keep only the K largest.
"""
from collections import defaultdict
from rich import print
import heapq

events = [
    {"user_id": "U1", "event_type": "click"},
    {"user_id": "U2", "event_type": "click"},
    {"user_id": "U3", "event_type": "click"},
    {"user_id": "U1", "event_type": "purchase"},
    {"user_id": "U2", "event_type": "click"},
    {"user_id": "U1", "event_type": "click"},
    {"user_id": "U4", "event_type": "click"},
    {"user_id": "U5", "event_type": "click"},
    {"user_id": "U2", "event_type": "purchase"},
    {"user_id": "U3", "event_type": "click"},
    {"user_id": "U3", "event_type": "purchase"},
    {"user_id": "U6", "event_type": "click"},
    {"user_id": "U7", "event_type": "click"},
    {"user_id": "U8", "event_type": "click"},
    {"user_id": "U1", "event_type": "click"},
    {"user_id": "U2", "event_type": "click"},
    {"user_id": "U3", "event_type": "click"},
    {"user_id": "U3", "event_type": "click"},
    {"user_id": "U4", "event_type": "purchase"},
    {"user_id": "U2", "event_type": "click"},
]


def top_k_global_heap(events: list[dict], k: int) -> list[tuple[str, int]]:
    users = defaultdict(int)
    for e in events:
        users[e["user_id"]] += 1

    heap: list[tuple[int, str]] = []
    for user_id, count in users.items():
        if len(heap) < k:
            heapq.heappush(heap, (count, user_id))
        elif count > heap[0][0]:
            heapq.heappushpop(heap, (count, user_id))

    return sorted(((u, c) for c, u in heap), key=lambda x: x[1], reverse=True)


print(top_k_global_heap(events, 2))
