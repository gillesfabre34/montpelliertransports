"""
Top-K after salting: events have salted user_ids (u1#0, u1#1, …).
Aggregate by stripping the suffix (#…) to get count per real user, then top-K with heap.
"""
import heapq
from collections import defaultdict
from rich import print

from detect_skew import detect_skew
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


def top_k_after_salt(events: list[dict], k: int, key_field: str = "user_id", threshold: float = 0.4, salt_factor: int = 3) -> list[tuple[str, int]]:
    salted = salt_events(events, key_field, threshold, salt_factor)
    # Re-aggregate by stripping the #n suffix
    counts: dict[str, int] = defaultdict(int)
    for e in salted:
        raw_key = e[key_field].split("#")[0]
        counts[raw_key] += 1

    heap: list[tuple[int, str]] = []
    for raw_key, count in counts.items():
        if len(heap) < k:
            heapq.heappush(heap, (count, raw_key))
        elif count > heap[0][0]:
            heapq.heappushpop(heap, (count, raw_key))

    return sorted(((u, c) for c, u in heap), key=lambda x: x[1], reverse=True)


print(top_k_after_salt(events, 2))
