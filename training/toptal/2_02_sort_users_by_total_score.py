from collections import defaultdict

"""
LEVEL 2 — Data Transformation

Exercise 5 — Sort users by total score
---------------------------------------
Given a list of (username, score) tuples, aggregate scores per user
and return a list of (username, total_score) sorted by total_score descending.

Steps:
1) Aggregate scores per user
2) Sort by total score descending

Expected output for EXAMPLE_INPUT: [("alice", 80), ("carol", 70), ("bob", 30)]

Constraints:
- Return list of (name, total) tuples, descending by total
"""

EXAMPLE_INPUT = [
    ("alice", 50),
    ("bob", 20),
    ("alice", 30),
    ("bob", 10),
    ("carol", 70),
]

EXPECTED_OUTPUT = [
    ("alice", 80),
    ("carol", 70),
    ("bob", 30),
]


def sort_users_by_total_score(
        scores: list[tuple[str, int]]
) -> list[tuple[str, int]]:
    """Return (username, total_score) list sorted by total_score descending."""
    users_scores = defaultdict(int)
    for name, score in scores:
        users_scores[name] += score
    output = sorted(users_scores.items(), key=lambda x: -x[1])
    return output


print(sort_users_by_total_score(EXAMPLE_INPUT))
