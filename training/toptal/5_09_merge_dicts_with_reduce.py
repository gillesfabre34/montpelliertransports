from __future__ import annotations

from functools import reduce

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 9 — Merge list of dicts into one with reduce (sum values per key)
-------------------------------------------------------------------------
You are given a list of dictionaries, each mapping string keys to numeric values.
Return a single dict that merges all of them: for each key, the value is the
**sum** of all values for that key across all dicts. Use functools.reduce.

Example: [{"a": 1, "b": 2}, {"b": 3, "c": 4}, {"a": 10}]
Expected: {"a": 11, "b": 5, "c": 4}

Constraints:
- Use functools.reduce
- Empty list -> return {}
"""

EXAMPLE_INPUT = [
    {"a": 1, "b": 2},
    {"b": 3, "c": 4},
    {"a": 10},
]
EXPECTED_OUTPUT = {"a": 11, "b": 5, "c": 4}


def merge_dicts_with_reduce(
    dicts: list[dict[str, int | float]]
) -> dict[str, int | float]:
    """Merge all dicts: for each key, sum values. Use reduce."""
    ...


print(merge_dicts_with_reduce(EXAMPLE_INPUT))


# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert merge_dicts_with_reduce(EXAMPLE_INPUT) == EXPECTED_OUTPUT


def test_empty_list():
    assert merge_dicts_with_reduce([]) == {}


def test_single_dict():
    assert merge_dicts_with_reduce([{"x": 1, "y": 2}]) == {"x": 1, "y": 2}


def test_no_common_keys():
    assert merge_dicts_with_reduce([{"a": 1}, {"b": 2}, {"c": 3}]) == {"a": 1, "b": 2, "c": 3}


def test_all_same_keys():
    assert merge_dicts_with_reduce([{"a": 1}, {"a": 2}, {"a": 3}]) == {"a": 6}


def test_empty_dict_in_list():
    assert merge_dicts_with_reduce([{"a": 1}, {}, {"a": 2}]) == {"a": 3}
