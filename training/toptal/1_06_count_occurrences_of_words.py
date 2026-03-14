"""
LEVEL 1 — Basic Python Data Manipulation

Exercise 6 — Count occurrences of words
--------------------------------------
Given a list of words, return a dictionary mapping each word to its number of occurrences.

Expected output for EXAMPLE_INPUT: {"apple": 3, "banana": 2, "orange": 1}

Constraints:
- Use a dictionary or defaultdict
- O(n) time

Hint: One pass over the list; increment count for each word.
"""

EXAMPLE_INPUT = ["apple", "banana", "apple", "orange", "banana", "apple"]

EXPECTED_OUTPUT = {
    "apple": 3,
    "banana": 2,
    "orange": 1,
}


def count_word_occurrences(words: list[str]) -> dict[str, int]:
    """Return a mapping of each word to its occurrence count."""
    pass
