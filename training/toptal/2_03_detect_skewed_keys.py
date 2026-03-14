"""
LEVEL 2 — Data Transformation

Exercise 6 — Detect skewed keys
--------------------------------
Given a list of (key, value) tuples, return the keys that represent
more than a given percentage (e.g. 40%) of the total number of records.

Expected output for EXAMPLE_INPUT with EXAMPLE_THRESHOLD: ["A"]
(A appears 4 times out of 7 ~57%; B and C are below 40%.)

Constraints:
- threshold_pct is between 0 and 1
- Return keys that strictly exceed the threshold proportion
"""

EXAMPLE_INPUT = [
    ("A", 1),
    ("A", 2),
    ("A", 3),
    ("B", 4),
    ("C", 5),
    ("A", 6),
    ("B", 7),
]

EXAMPLE_THRESHOLD = 0.4  # 40%

EXPECTED_OUTPUT = ["A"]


def detect_skewed_keys(
    data: list[tuple[str, int]], threshold_pct: float = 0.4
) -> list[str]:
    """Return keys whose count exceeds threshold_pct of total records."""
    pass
