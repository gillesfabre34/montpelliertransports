from __future__ import annotations

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 12 — Sort by multiple criteria (sorted with key tuple)
--------------------------------------------------------------
You are given a list of tuples (name, score, date). Sort by:
  1) score descending (highest first)
  2) date ascending (earliest first)
  3) name ascending (alphabetical)

Use a single sorted(..., key=...) with a tuple key. Remember: descending
can be done with negative for numbers, or with a wrapper.

Example: [("alice", 10, "2024-02-01"), ("bob", 10, "2024-01-01"), ("carol", 20, "2024-01-15")]
Expected: [("carol", 20, "2024-01-15"), ("alice", 10, "2024-02-01"), ("bob", 10, "2024-01-01")]
(score 20 first; then score 10 tie -> date asc -> bob before alice)

Constraints:
- Use sorted() with key= returning a tuple
- score desc, date asc, name asc
"""

EXAMPLE_INPUT = [
    ("alice", 10, "2024-02-01"),
    ("bob", 10, "2024-01-01"),
    ("carol", 20, "2024-01-15"),
]
EXPECTED_OUTPUT = [
    ("carol", 20, "2024-01-15"),
    ("bob", 10, "2024-01-01"),
    ("alice", 10, "2024-02-01"),
]


def sort_multi_criteria(
    data: list[tuple[str, int, str]]
) -> list[tuple[str, int, str]]:
    """Sort by score desc, then date asc, then name asc. Use sorted(..., key=...)."""
    ...


print(sort_multi_criteria(EXAMPLE_INPUT))


# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert sort_multi_criteria(EXAMPLE_INPUT) == EXPECTED_OUTPUT


def test_empty():
    assert sort_multi_criteria([]) == []


def test_single():
    assert sort_multi_criteria([("a", 1, "2024-01-01")]) == [("a", 1, "2024-01-01")]


def test_score_only_tie():
    data = [("b", 5, "2024-01-01"), ("a", 5, "2024-01-01")]
    # score tie, date tie -> name asc -> a before b
    assert sort_multi_criteria(data) == [("a", 5, "2024-01-01"), ("b", 5, "2024-01-01")]


def test_date_break_tie():
    data = [("x", 10, "2024-02-01"), ("y", 10, "2024-01-01")]
    assert sort_multi_criteria(data) == [("y", 10, "2024-01-01"), ("x", 10, "2024-02-01")]


def test_all_different_scores():
    data = [("a", 1, "2024-01-01"), ("b", 3, "2024-01-01"), ("c", 2, "2024-01-01")]
    assert sort_multi_criteria(data) == [("b", 3, "2024-01-01"), ("c", 2, "2024-01-01"), ("a", 1, "2024-01-01")]
