from __future__ import annotations
from collections import defaultdict
from datetime import datetime
from utils.tools import logg

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 10 — Parse a log line into a dict (string methods)
-----------------------------------------------------------
You are given a log line string in the format:
  "YYYY-MM-DD HH:MM:SS | user_id | LEVEL | message"
(separator " | " between fields). Return a dict with keys: "date", "time",
"user_id", "level", "message". Use string methods (split, strip); no regex required.

Example: "2024-01-15 10:30:00 | user_123 | ERROR | Connection refused"
Expected: {"date": "2024-01-15", "time": "10:30:00", "user_id": "user_123", "level": "ERROR", "message": "Connection refused"}

Constraints:
- Use str.split() and str.strip()
- Assume exactly 4 parts after splitting by " | "
- date is the first token of the first part (before space), time is the second token
"""

EXAMPLE_LINE = "2024-01-15 10:30:00 | user_123 | ERROR | Connection refused"
EXPECTED_OUTPUT = {
    "date": "2024-01-15",
    "time": "10:30:00",
    "user_id": "user_123",
    "level": "ERROR",
    "message": "Connection refused",
}


def parse_log_line(line: str) -> dict[str, str]:
    """Parse log line into dict with keys date, time, user_id, level, message. Use split/strip."""
    date_str, user_id, level, message = [part.strip() for part in line.split('|')]
    date, time = date_str.split(' ')
    return {
        "date": date,
        "time": time,
        "user_id": user_id,
        "level": level,
        "message": message
    }


print(parse_log_line(EXAMPLE_LINE))


# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert parse_log_line(EXAMPLE_LINE) == EXPECTED_OUTPUT


def test_info_level():
    line = "2024-02-01 00:00:00 | admin | INFO | Startup"
    assert parse_log_line(line) == {
        "date": "2024-02-01",
        "time": "00:00:00",
        "user_id": "admin",
        "level": "INFO",
        "message": "Startup",
    }


def test_message_with_spaces():
    line = "2024-01-01 12:00:00 | u1 | WARN | Something went wrong here"
    assert parse_log_line(line)["message"] == "Something went wrong here"


def test_empty_message():
    line = "2024-01-01 12:00:00 | u1 | DEBUG | "
    result = parse_log_line(line)
    assert result["message"] == "" and result["level"] == "DEBUG"
