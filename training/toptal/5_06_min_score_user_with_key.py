from __future__ import annotations

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 6 — User with minimum score using min(..., key=...)
----------------------------------------------------------
You are given a list of (user_id, score) tuples. Return the user_id that
has the **minimum** score. Use the built-in min() with the key= parameter.

If there is a tie (same minimum score), return the user_id that is
**largest** (e.g. lexicographically for strings, or numerically for ints).

Example: [("alice", 50), ("bob", 30), ("carol", 30)] -> "carol" (30 is min; tie -> max user_id)
Example: [("u1", 10), ("u2", 20)] -> "u1"

Constraints:
- Use min() with key= (and possibly a tuple for tie-break)
"""

EXAMPLE_INPUT = [("alice", 50), ("bob", 30), ("carol", 30)]
EXPECTED_OUTPUT = "carol"  # min score 30; tie between bob and carol -> max user_id -> "carol"


def min_score_user(scores: list[tuple[str, int | float]]) -> str:
    """Return user_id with minimum score; on tie, return the largest user_id. Use min(..., key=...)."""
    ...


print(min_score_user(EXAMPLE_INPUT))


# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert min_score_user(EXAMPLE_INPUT) == EXPECTED_OUTPUT


def test_single_user():
    assert min_score_user([("alice", 100)]) == "alice"


def test_no_tie():
    assert min_score_user([("a", 10), ("b", 20), ("c", 5)]) == "c"


def test_all_same_score():
    # tie -> max user_id
    assert min_score_user([("a", 0), ("b", 0), ("c", 0)]) == "c"


def test_tie_two():
    assert min_score_user([("alice", 10), ("bob", 10)]) == "bob"


def test_numeric_user_ids():
    # If user_id were int: (1, 10), (2, 10) -> max user_id = 2
    data = [("1", 10), ("2", 10)]
    assert min_score_user(data) == "2"
