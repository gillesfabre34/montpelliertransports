from __future__ import annotations

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 5 — Merge two dicts, sum values for common keys (get / setdefault)
---------------------------------------------------------------------------
Given two dictionaries mapping string keys to numeric values, return a new
dict that merges them: if a key exists in both, the value is the sum of the
two values; otherwise it is the single value. Use dict.get(key, default) or
dict.setdefault(key, default) in your implementation.

Example: d1 = {"a": 1, "b": 2}, d2 = {"b": 3, "c": 4}
Expected: {"a": 1, "b": 5, "c": 4}

Constraints:
- Use .get() or .setdefault() (no KeyError handling by hand)
- Do not modify d1 or d2; return a new dict
"""

EXAMPLE_D1 = {"a": 1, "b": 2}
EXAMPLE_D2 = {"b": 3, "c": 4}
EXPECTED_OUTPUT = {"a": 1, "b": 5, "c": 4}


def merge_dicts_sum_values(d1: dict[str, int | float], d2: dict[str, int | float]) -> dict[str, int | float]:
    """Merge d1 and d2; for common keys, sum the values. Use .get() or .setdefault()."""
    ...


print(merge_dicts_sum_values(EXAMPLE_D1, EXAMPLE_D2))


# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert merge_dicts_sum_values(EXAMPLE_D1, EXAMPLE_D2) == EXPECTED_OUTPUT


def test_empty_first():
    assert merge_dicts_sum_values({}, {"x": 1}) == {"x": 1}


def test_empty_second():
    assert merge_dicts_sum_values({"x": 1}, {}) == {"x": 1}


def test_both_empty():
    assert merge_dicts_sum_values({}, {}) == {}


def test_no_common_keys():
    assert merge_dicts_sum_values({"a": 1}, {"b": 2}) == {"a": 1, "b": 2}


def test_all_common_keys():
    assert merge_dicts_sum_values({"a": 10, "b": 20}, {"a": 1, "b": 2}) == {"a": 11, "b": 22}


def test_input_unchanged():
    d1, d2 = {"a": 1}, {"a": 2}
    merge_dicts_sum_values(d1, d2)
    assert d1 == {"a": 1} and d2 == {"a": 2}
