from __future__ import annotations
from __future__ import annotations

from collections import defaultdict

"""
LEVEL 2 — Data Transformation

Exercise 4 — Join two datasets
-------------------------------
Given two lists of dicts: users (id, name) and orders (user_id, amount).
Return total spending per user name (not per user id).

Expected output for EXAMPLE_USERS and EXAMPLE_ORDERS: {"Alice": 80, "Bob": 20}

Constraints:
- Avoid nested loops if possible
"""

EXAMPLE_USERS = [
    {"id": 1, "name": "Alice"},
    {"id": 2, "name": "Bob"},
]

EXAMPLE_ORDERS = [
    {"user_id": 1, "amount": 50},
    {"user_id": 1, "amount": 30},
    {"user_id": 2, "amount": 20},
]

EXPECTED_OUTPUT = {"Alice": 80, "Bob": 20}


def total_spending_per_user_name(
        users: list[dict], orders: list[dict]
) -> dict[str, int | float]:
    """Return total spending per user name (key = name, value = total amount)."""
    output = defaultdict(int)
    users_dict: dict[str, str] = {u["id"]: u["name"] for u in users}
    # users_dict = defaultdict(str)
    # for u in users:
    #     users_dict[u["id"]] = u["name"]

    for o in orders:
        user_name = users_dict[o["user_id"]]
        output[user_name] += o["amount"]

    return dict(output)


print(total_spending_per_user_name(EXAMPLE_USERS, EXAMPLE_ORDERS))
