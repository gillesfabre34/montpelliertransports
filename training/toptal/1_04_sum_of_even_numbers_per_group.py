from collections import defaultdict

"""
LEVEL 1 — Basic Python Data Manipulation

Exercise 4 — Sum of even numbers per group
------------------------------------------
You are given a list of tuples (user_id, value).
Compute the sum of **even** numbers per user. Ignore odd values.

Expected output for EXAMPLE_INPUT: {"u1": 22, "u2": 14, "u3": 8}
(u1: 10 + 12, u2: 14, u3: 8)

Constraints:
- Use a dictionary
- Ignore odd numbers
- O(n) time

Hint: Check value % 2 before adding.
"""

EXAMPLE_INPUT = [
    ("u1", 10),
    ("u2", 5),
    ("u1", 21),
    ("u2", 14),
    ("u3", 8),
    ("u1", 12),
    ("u4", 7),
]

EXPECTED_OUTPUT = {
    "u1": 22,  # 10 + 12
    "u2": 14,  # 14
    "u3": 8,  # 8
    "u4": 0
}


def sum_even_per_group(data: list[tuple[str, int]]) -> dict[str, int]:
    """Return the sum of even values per user_id."""
    output = defaultdict(int)
    for (user_id, value) in data:
        if value % 2 == 0:
            output[user_id] += value
    return dict(output)


print(sum_even_per_group(EXAMPLE_INPUT))
