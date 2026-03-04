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


def top_k_active_users(events: list[dict], k: int) -> list[tuple[str, int]]:
    result = []
    users = defaultdict(int)

    for e in events:
        users[e["user_id"]] += 1
    print(f"users", users)

    for user_id, count in users.items():
        print(f"user_id", user_id)
        if len(result) < k:
            heapq.heappush(result, [count, user_id])
        elif result[0][0] < count:
            heapq.heappushpop(result, [count, user_id])

        print(f"result", result)

    sorted_heap = sorted(result, key=lambda x: -x[0])
    return [(r[1], r[0]) for r in sorted_heap]


print(top_k_active_users(events, 2))