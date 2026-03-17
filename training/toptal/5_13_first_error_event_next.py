from __future__ import annotations
from collections.abc import Iterable, Iterator

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 13 — First event with type "ERROR" using next() and default
---------------------------------------------------------------------
You are given a list of event dicts; each has at least a "type" key.
Return the **first** event dict whose type equals "ERROR". If none,
return None. Use the built-in next() with a generator expression (or
filter) and a default value.

Example: [{"type": "INFO", "id": 1}, {"type": "ERROR", "id": 2}, {"type": "ERROR", "id": 3}]
Expected: {"type": "ERROR", "id": 2}

Constraints:
- Use next(iterator, default)
- Return the full dict (not just a field)
"""

EXAMPLE_EVENTS = [
    {"type": "INFO", "id": 1},
    {"type": "ERROR", "id": 2},
    {"type": "ERROR", "id": 3},
]
EXPECTED_OUTPUT = {"type": "ERROR", "id": 2}


def first_error_event(events: list[dict]) -> dict | None:
    """Return the first event with type == 'ERROR', or None. Use next(..., default)."""
    return next((event for event in events if event['type'] == 'ERROR'), None)


# def first_error_event(events: list[dict]) -> dict | None:
#     """Return the first event with type == 'ERROR', or None. Use next(..., default)."""
#
#     def it(events: Iterable[dict]) -> Iterator:
#         for event in events:
#             if event["type"] == 'ERROR':
#                 yield event
#
#     return next(it(events), None)


print(first_error_event(EXAMPLE_EVENTS))


# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert first_error_event(EXAMPLE_EVENTS) == EXPECTED_OUTPUT


def test_no_error():
    assert first_error_event([{"type": "INFO"}, {"type": "WARN"}]) is None


def test_empty_list():
    assert first_error_event([]) is None


def test_first_is_error():
    assert first_error_event([{"type": "ERROR", "id": 1}]) == {"type": "ERROR", "id": 1}


def test_last_is_error():
    events = [{"type": "INFO"}, {"type": "ERROR", "code": 500}]
    assert first_error_event(events) == {"type": "ERROR", "code": 500}


def test_only_one_event_error():
    assert first_error_event([{"type": "ERROR", "msg": "fail"}]) == {"type": "ERROR", "msg": "fail"}
