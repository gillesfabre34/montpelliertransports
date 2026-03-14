"""
LEVEL 4 — Data Engineering Style Challenges

Exercise 10 — First non-repeating event
----------------------------------------
Given a list of event labels in order, return the first element that
appears exactly once (no duplicate anywhere in the list).
If all repeat or list is empty, you may return None or raise.

Expected output for EXAMPLE_INPUT: "C"
(A and B repeat; C is the first with no duplicate.)

Constraint:
- Single pass if possible
"""

EXAMPLE_INPUT = ["A", "B", "C", "A", "B", "D"]

EXPECTED_OUTPUT = "C"


def first_non_repeating_event(events: list[str]) -> str | None:
    """Return the first event that appears exactly once in the list."""
    pass
