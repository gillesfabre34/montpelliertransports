from __future__ import annotations

from functools import reduce

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 8 — Product of a list using reduce
--------------------------------------------
Given a list of numbers, return their product (1 if list is empty).
Use functools.reduce.

Example: [2, 3, 4] -> 24
Example: [] -> 1

Constraints:
- Use functools.reduce
"""

EXAMPLE_INPUT = [2, 3, 4]
EXPECTED_OUTPUT = 24


def product_with_reduce(nums: list[int | float]) -> int | float:
    """Return the product of nums; return 1 if empty. Use reduce."""
    ...


print(product_with_reduce(EXAMPLE_INPUT))


# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert product_with_reduce(EXAMPLE_INPUT) == EXPECTED_OUTPUT


def test_empty():
    assert product_with_reduce([]) == 1


def test_single():
    assert product_with_reduce([7]) == 7


def test_with_zero():
    assert product_with_reduce([1, 2, 0, 3]) == 0


def test_negative():
    assert product_with_reduce([-1, 2, 3]) == -6


def test_one_element_zero():
    assert product_with_reduce([0]) == 0
