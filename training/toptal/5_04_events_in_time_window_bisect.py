from __future__ import annotations

from collections import deque

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 4 — Count events in time window using bisect
-----------------------------------------------------
You are given a **sorted** list of timestamps (integers, e.g. seconds)
and a window size T. For each timestamp t in the list, count how many
events fall in the closed interval [t - T, t]. Use bisect (bisect_left
and/or bisect_right) to avoid a naive O(n) scan per t.

Example: timestamps = [10, 20, 25, 30], T = 10
For t=10: [0, 10] -> 1 event (10)
For t=20: [10, 20] -> 2 events (10, 20)
For t=25: [15, 25] -> 2 events (20, 25)
For t=30: [20, 30] -> 3 events (20, 25, 30)
Expected: [1, 2, 2, 3]

Constraints:
- Use bisect module (bisect_left / bisect_right)
- Input timestamps are sorted
- Window is [t - T, t] (inclusive on both ends)
"""

EXAMPLE_TIMESTAMPS = [10, 20, 25, 30]
EXAMPLE_WINDOW_T = 10
EXPECTED_OUTPUT = [1, 2, 2, 3]


def count_in_window(timestamps: list[int], window_t: int) -> list[int]:
    """Complexity : O(n)"""
    """For each t in timestamps, return count of events in [t - window_t, t]. Use bisect."""
    result = []
    if len(timestamps) > 0:
        deq = deque()
        for t in timestamps:
            while deq and deq[0] < t - window_t:
                deq.popleft()
            deq.append(t)
            result.append(len(deq))
    return result


# def count_in_window(timestamps: list[int], window_t: int) -> list[int]:
#     """Complexity : O(n log n)"""
#     """For each t in timestamps, return count of events in [t - window_t, t]. Use bisect."""
#     result = []
#     for t in timestamps:
#         previous_t = B.bisect_left(timestamps, t - window_t)
#         next_t = B.bisect_right(timestamps, t)
#         logg("t / previous_t / next_t / bucket", f"{t} / {previous_t} / {next_t}")
#         result.append(next_t - previous_t)
#     return result


print(count_in_window(EXAMPLE_TIMESTAMPS, EXAMPLE_WINDOW_T))

# ----------------------
# Corner case tests
# ----------------------


# def test_example():
#     assert count_in_window(EXAMPLE_TIMESTAMPS, EXAMPLE_WINDOW_T) == EXPECTED_OUTPUT
#
#
# def test_empty():
#     assert count_in_window([], 10) == []
#
#
# def test_single_event():
#     assert count_in_window([5], 10) == [1]
#
#
# def test_window_zero():
#     # [t, t] only t itself
#     assert count_in_window([1, 2, 3], 0) == [1, 1, 1]
#
#
# def test_all_in_same_window():
#     assert count_in_window([1, 2, 3], 5) == [1, 2, 3]
#
#
# def test_no_overlap():
#     # timestamps 0, 10, 20 with T=5: each window contains only one event
#     assert count_in_window([0, 10, 20], 5) == [1, 1, 1]
