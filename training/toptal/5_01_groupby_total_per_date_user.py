from __future__ import annotations

from itertools import groupby

from rich import print

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 1 — Total amount per (date, user_id) with groupby
----------------------------------------------------------
You are given a list of tuples (date, user_id, amount) **already sorted**
by (date, user_id). Use itertools.groupby to return the total amount
per (date, user_id) in a single pass (streaming style).

Expected output for EXAMPLE_INPUT: {("2024-01-01", "u1"): 30, ("2024-01-01", "u2"): 10,
("2024-01-02", "u1"): 5}

Constraints:
- Use itertools.groupby
- Input is pre-sorted by (date, user_id)
- Return a dict: key = (date, user_id), value = total amount
- O(n) single pass
"""

EXAMPLE_INPUT = [
    ("2024-01-01", "u1", 10),
    ("2024-01-01", "u1", 20),
    ("2024-01-01", "u2", 10),
    ("2024-01-02", "u1", 5),
]

EXPECTED_OUTPUT = {
    ("2024-01-01", "u1"): 30,
    ("2024-01-01", "u2"): 10,
    ("2024-01-02", "u1"): 5,
}


def total_per_date_user(
        data: list[tuple[str, str, int | float]]
) -> dict[tuple[str, str], int | float]:
    """Return total amount per (date, user_id) using groupby. Input must be sorted by (date, user_id)."""
    result = {}
    for (date, user_id), group in groupby(data, key=lambda x: (x[0], x[1])):
        total = sum(value for _, _, value in group)
        result[(date, user_id)] = total
    return result


print(total_per_date_user(EXAMPLE_INPUT))


# def total_per_date_user_without_itertools(
#         data: list[tuple[str, str, int | float]]
# ) -> dict[tuple[str, str], int | float]:
#     """Return total amount per (date, user_id) using groupby. Input must be sorted by (date, user_id)."""
#     result: defaultdict[tuple[str, str], int | float] = defaultdict(int)
#     for date, user_id, value in data:
#         result[(date, user_id)] += value
#     return dict(result)
#
#
# print(total_per_date_user_without_itertools(EXAMPLE_INPUT))

# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert total_per_date_user(EXAMPLE_INPUT) == EXPECTED_OUTPUT


def test_single_row():
    assert total_per_date_user([("2024-01-01", "u1", 100)]) == {("2024-01-01", "u1"): 100}


def test_empty():
    assert total_per_date_user([]) == {}


def test_multiple_dates_same_user():
    data = [
        ("2024-01-01", "u1", 1),
        ("2024-01-02", "u1", 2),
        ("2024-01-03", "u1", 3),
    ]
    assert total_per_date_user(data) == {
        ("2024-01-01", "u1"): 1,
        ("2024-01-02", "u1"): 2,
        ("2024-01-03", "u1"): 3,
    }


def test_one_group_many_rows():
    data = [("2024-01-01", "u1", x) for x in (1, 2, 3, 4, 5)]
    assert total_per_date_user(data) == {("2024-01-01", "u1"): 15}
