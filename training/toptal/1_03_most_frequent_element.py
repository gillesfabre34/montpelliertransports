from collections import Counter

"""
LEVEL 1 — Basic Python Data Manipulation

Exercise 3 — Find most frequent element
----------------------------------------
Return the element that appears most often in the list.
If there is a tie, return the smallest value.

Expected output for EXAMPLE_INPUT: 3

Constraints:
- If tie, return the smallest value
"""

EXAMPLE_INPUT = [1, 2, 3, 2, 4, 2, 5, 3, 3, 3, 2]

EXPECTED_OUTPUT = 3


def most_frequent_element(items: list[int]) -> int:
    """Return the most frequent element; on tie, return the smallest."""
    counts = Counter(items)
    max_count = 0
    output = None
    for v, count in counts.items():
        if count > max_count or (count == max_count and v < output):
            max_count = count
            output = v
    return output


print("max", most_frequent_element(EXAMPLE_INPUT))
