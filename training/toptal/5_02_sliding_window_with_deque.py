from __future__ import annotations

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 2 — Sliding window average using deque
-----------------------------------------------
Given a list of numbers and a window size, return the list of averages
for each sliding window of that size, using a collections.deque with
maxlen=window_size (so that each append automatically drops the oldest
element).

Expected output for EXAMPLE_NUMS and EXAMPLE_WINDOW: [3.0, 5.0, 7.0]
Windows: (1+3+5)/3=3, (3+5+7)/3=5, (5+7+9)/3=7

Constraints:
- Use collections.deque(maxlen=window_size)
- O(n) time
"""

EXAMPLE_NUMS = [1, 3, 5, 7, 9]
EXAMPLE_WINDOW = 3
EXPECTED_OUTPUT = [3.0, 5.0, 7.0]


def sliding_window_average_deque(
    nums: list[float], window_size: int
) -> list[float]:
    """Return the average of each sliding window using a deque(maxlen=window_size)."""
    ...


print(sliding_window_average_deque(EXAMPLE_NUMS, EXAMPLE_WINDOW))


# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert sliding_window_average_deque(EXAMPLE_NUMS, EXAMPLE_WINDOW) == EXPECTED_OUTPUT


def test_window_larger_than_list():
    assert sliding_window_average_deque([1.0, 2.0], 5) == []


def test_window_equals_list_length():
    assert sliding_window_average_deque([1.0, 2.0, 3.0], 3) == [2.0]


def test_single_element_window():
    assert sliding_window_average_deque([10.0, 20.0, 30.0], 1) == [10.0, 20.0, 30.0]


def test_empty_list():
    assert sliding_window_average_deque([], 3) == []


def test_window_size_one():
    assert sliding_window_average_deque([1.0, 2.0, 3.0], 1) == [1.0, 2.0, 3.0]
