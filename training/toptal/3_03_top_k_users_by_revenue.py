from collections import defaultdict
from heapq import heappush, heappushpop

"""
LEVEL 3 — Top K Problems

Exercise 9 — Top K users by revenue
------------------------------------
Given a list of (username, amount) transactions and k, return the top k users
by total revenue as a list of (username, total_revenue) in descending order.

Steps:
1) Aggregate revenue per user
2) Use a heap to get top k

Expected output for EXAMPLE_TRANSACTIONS and EXAMPLE_K:
[("carol", 200), ("alice", 140)]

Constraints:
- Use a heap for the top-k selection after aggregation
"""

EXAMPLE_TRANSACTIONS = [
    ("alice", 100),
    ("bob", 50),
    ("alice", 40),
    ("carol", 200),
    ("bob", 80),
]

EXAMPLE_K = 2

EXPECTED_OUTPUT = [
    ("carol", 200),
    ("alice", 140),
]


def top_k_users_by_revenue(
        transactions: list[tuple[str, int]], k: int
) -> list[tuple[str, int]]:
    """Return top k users by total revenue as (username, total) descending."""
    users = defaultdict(int)
    for name, value in transactions:
        users[name] += value
    heaps = []
    for name, value in users.items():
        if len(heaps) < k:
            heappush(heaps, (value, name))
        elif value > heaps[0][0]:
            heappushpop(heaps, (value, name))
    return sorted([(name, value) for value, name in heaps], key=lambda x: x[1], reverse=True)


print(top_k_users_by_revenue(EXAMPLE_TRANSACTIONS, EXAMPLE_K))
