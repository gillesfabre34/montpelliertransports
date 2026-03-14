from collections import defaultdict

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
    ("user2", 121),
]

EXAMPLE_WINDOW = 10  # seconds

EXPECTED_OUTPUT = ["user1"]


def duplicate_events_within_time_window(
        events: list[tuple[str, int]], window_seconds: int
) -> list[str]:
    """Return user_ids that have at least two events within window_seconds."""
    users = defaultdict(list[int])
    for user_id, t in events:
        users[user_id].append(t)

    results = []
    for user_id, ts in users.items():
        sorted_ts = sorted(ts)
        if len(ts) == 1:
            continue
        for i in range(1, len(ts)):
            if sorted_ts[i] - sorted_ts[i - 1] <= window_seconds:
                results.append(user_id)
                break

    return results


def test_real_failure_case():
    events = [
        ("user1", 0),
        ("user1", 20),
        ("user1", 5),
    ]
    window = 10
    # La paire (0,5) est dans la fenêtre, mais non consécutive dans la liste
    assert duplicate_events_within_time_window(events, window) == ["user1"]


print(duplicate_events_within_time_window(EXAMPLE_EVENTS, EXAMPLE_WINDOW))


# ----------------------
# Tests avec pytest
# ----------------------

def test_example_case():
    events = [
        ("user1", 100),
        ("user2", 101),
        ("user1", 105),
        ("user3", 110),
        ("user1", 111),
        ("user2", 121),
    ]
    window = 10
    assert duplicate_events_within_time_window(events, window) == ["user1"]


def test_unsorted_timestamps():
    events = [
        ("user1", 111),
        ("user1", 100),
        ("user1", 105),
    ]
    window = 10
    assert duplicate_events_within_time_window(events, window) == ["user1"]


def test_no_duplicates():
    events = [
        ("user1", 100),
        ("user2", 200),
        ("user3", 300),
    ]
    window = 10
    assert duplicate_events_within_time_window(events, window) == []


def test_multiple_users():
    events = [
        ("user1", 100),
        ("user2", 101),
        ("user1", 105),
        ("user2", 108),
        ("user3", 110),
        ("user1", 111),
        ("user2", 115),
    ]
    window = 10
    result = duplicate_events_within_time_window(events, window)
    # user1 et user2 ont au moins deux événements dans la fenêtre
    assert set(result) == {"user1", "user2"}


def test_single_event_per_user():
    events = [
        ("user1", 100),
        ("user2", 200),
    ]
    window = 50
    assert duplicate_events_within_time_window(events, window) == []


def test_unsorted_timestamps_edge_case():
    events = [
        ("user1", 120),
        ("user1", 100),
        ("user1", 115),
        ("user1", 105),
    ]
    window = 10
    # L'utilisateur a bien deux événements à 100 et 105 → diff 5 ≤ 10
    # Si ta fonction ne trie pas, ts = [120,100,115,105], paires consécutives: (120,100)=20, (100,115)=15, (115,105)=10
    # Aucun diff consécutif ≤ 10 → ton code raterait l'utilisateur
    assert duplicate_events_within_time_window(events, window) == ["user1"]


def test_unsorted_timestamps_failure_case():
    events = [
        ("user1", 120),
        ("user1", 100),
        ("user1", 111),
        ("user1", 105),
    ]
    window = 10
    # La paire valide est (100,105) diff=5
    # Sans trier les timestamps, la fonction actuelle échoue et ne détecte pas user1
    assert duplicate_events_within_time_window(events, window) == ["user1"]


def test_missing_sorted_exposes_bug():
    """
    Fails when timestamps are not sorted: without sorted(), the code uses
    consecutive list order. Here events are (100, 89); diff 89-100 = -11 <= 10
    so the buggy code wrongly adds user1. But sorted order is (89, 100), gap 11 > 10,
    so the user must NOT be in the result.
    """
    events = [
        ("user1", 100),
        ("user1", 89),
    ]
    window = 10
    assert duplicate_events_within_time_window(events, window) == []
