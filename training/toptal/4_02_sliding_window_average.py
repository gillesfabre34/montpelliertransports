"""
LEVEL 4 — Data Engineering Style Challenges

Exercise 11 — Sliding window average
-------------------------------------
Given a list of numbers and a window size, return the list of averages
for each sliding window of that size.

Expected output for EXAMPLE_NUMS and EXAMPLE_WINDOW_SIZE: [3, 5, 7]
Windows: (1+3+5)/3=3, (3+5+7)/3=5, (5+7+9)/3=7

Constraints:
- O(n) time
"""

EXAMPLE_NUMS = [1, 3, 5, 7, 9]

EXAMPLE_WINDOW_SIZE = 3

EXPECTED_OUTPUT = [3, 5, 7]


def sliding_window_average(nums: list[float], window_size: int) -> list[float]:
    """Return the average of each sliding window of size window_size."""
    if len(nums) < window_size:
        return []
    s = sum(nums[0:window_size])
    result = [s / window_size]
    for i, n in enumerate(nums[window_size: len(nums)], window_size):
        s += n
        s -= nums[i - window_size]
        result.append(s / window_size)
    return result


print(sliding_window_average(EXAMPLE_NUMS, EXAMPLE_WINDOW_SIZE))
