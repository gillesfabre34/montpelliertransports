from collections import Counter
from heapq import heappush, heappushpop

"""
LEVEL 3 — Top K Problems

Exercise 8 — Top K frequent elements
------------------------------------
Given a list of numbers and k, return the k most frequent elements.
If frequency is tied, order does not matter for this exercise.

Expected output for EXAMPLE_NUMS and EXAMPLE_K: [1, 2]

Constraints:
- Time complexity better than O(n log n) if possible (e.g. bucket-based)
"""

EXAMPLE_NUMS = [1, 1, 1, 2, 2, 3]

EXAMPLE_K = 2

EXPECTED_OUTPUT = [1, 2]


def top_k_frequent(nums: list[int], k: int) -> list[int]:
    """Return the k most frequent elements."""
    counts = Counter(nums)
    heaps = []
    for key, v in counts.items():
        if len(heaps) < k:
            heappush(heaps, (v, key))
        elif v > heaps[0][0]:
            heappushpop(heaps, (v, key))
    return [v for key, v in heaps]


print(top_k_frequent(EXAMPLE_NUMS, EXAMPLE_K))
