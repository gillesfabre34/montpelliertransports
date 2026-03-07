"""
Data skew detection: find keys (e.g. user_id) that account for
more than a threshold share of events (e.g. 40%).
"""
from collections import defaultdict
from rich import print

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


def detect_skew(events: list[dict], key_field: str, threshold: float) -> list[str]:
    total = 0
    counts: dict[str, int] = defaultdict(int)
    for e in events:
        total += 1
        counts[e[key_field]] += 1
    return [k for k, c in counts.items() if c / total > threshold]


if __name__ == "__main__":
    print(detect_skew(events, "user_id", 0.4))
