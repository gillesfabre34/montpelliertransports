"""
LEVEL 4 — Data Engineering Style Challenges

Exercise 12 — Detect duplicate events within time window
---------------------------------------------------------
Given a list of (user_id, timestamp) events and a time window in seconds,
return the list of user_ids that have at least two events within the window
(i.e. two events whose timestamps differ by at most window seconds).

Expected output for EXAMPLE_EVENTS and EXAMPLE_WINDOW: ["user1"]
(user1 has events at 100, 105, 111; pairs within 10s. user2 and user3 do not.)

Constraints:
- Avoid quadratic complexity (e.g. O(n) or O(n log n) per user)
"""

EXAMPLE_EVENTS = [
    ("user1", 100),
    ("user2", 101),
    ("user1", 105),
    ("user3", 110),
    ("user1", 111),
]

EXAMPLE_WINDOW = 10  # seconds

EXPECTED_OUTPUT = ["user1"]


def duplicate_events_within_time_window(
    events: list[tuple[str, int]], window_seconds: int
) -> list[str]:
    """Return user_ids that have at least two events within window_seconds."""
    pass
