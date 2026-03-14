from collections import defaultdict

from rich import print

"""
LEVEL 1 — Basic Python Data Manipulation

Exercise 1 — Average per group
-------------------------------
You are given a list of tuples (user_id, value).
Compute the average value per user.

Expected output for EXAMPLE_INPUT: {"u1": 15, "u2": 10, "u3": 7}

Constraints:
- O(n) time if possible
- Use a dictionary
"""

EXAMPLE_INPUT = [
    ("u1", 10),
    ("u2", 5),
    ("u1", 20),
    ("u2", 15),
    ("u3", 7),
]

EXPECTED_OUTPUT = {"u1": 15, "u2": 10, "u3": 7}


def average_per_group(data: list[tuple[str, int]]) -> dict[str, float]:
    """Return the average value per user_id."""
    sums = defaultdict(int)
    counts = defaultdict(int)
    for user_id, value in data:
        sums[user_id] += value
        counts[user_id] += 1

    output = {}
    for user_id, value in sums.items():
        output[user_id] = value / counts[user_id]

    return dict(output)


print(average_per_group(EXAMPLE_INPUT))
