"""
LEVEL 1 — Basic Python Data Manipulation

Exercise 5 — Unique characters in a string
------------------------------------------
Given a string, return a list of unique characters in the order they first appear.

Expected output for EXAMPLE_INPUT: ["a", "b", "r", "c", "d"]

Constraints:
- Preserve order of first occurrence
- Use a set for efficiency

Hint: Iterate and track “already seen” with a set; append to result only if new.
"""

EXAMPLE_INPUT = "abracadabra"

EXPECTED_OUTPUT = ["a", "b", "r", "c", "d"]


def unique_characters_in_order(s: str) -> list[str]:
    """Return unique characters in order of first appearance."""
    chars = set()
    output = []
    for char in s:
        if char not in chars:
            chars.add(char)
            output.append(char)
    return output


print(unique_characters_in_order(EXAMPLE_INPUT))
