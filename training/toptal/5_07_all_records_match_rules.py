from __future__ import annotations
from utils.tools import logg
from typing import Callable

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 7 — All records match all rules (any / all)
----------------------------------------------------
You are given a list of records (each record is a dict with string keys and
values) and a list of rules. Each rule is a tuple (key, predicate) where
predicate is a function that takes a value and returns True/False.

Return True if **all** records satisfy **all** rules (i.e. for every record
and every rule, predicate(record[key]) is True). Otherwise return False.

Use the built-ins any() and/or all() in your implementation.

Example: records = [{"a": 1, "b": 2}, {"a": 2, "b": 3}]
         rules = [("a", lambda v: v > 0), ("b", lambda v: v < 10)]
         -> True (all records pass all rules)

Constraints:
- Use any() and/or all()
- If a key is missing in a record, consider the record as failing that rule (or define your contract)
"""

EXAMPLE_RECORDS = [
    {"a": 1, "b": 2},
    {"a": 2, "b": 3},
]
EXAMPLE_RULES = [
    ("a", lambda v: v > 0),
    ("b", lambda v: v < 3),
]
EXPECTED_OUTPUT = True


def all_records_match_rules(
        records: list[dict], rules: list[tuple[str, Callable]]
) -> bool:
    """Return True iff every record satisfies every rule (key -> predicate(value)). Use any/all."""
    return all(
        [all([(key in rec and f(rec[key])) for key, f in rules]) for rec in records]
    )


print(all_records_match_rules(EXAMPLE_RECORDS, EXAMPLE_RULES))


# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert all_records_match_rules(EXAMPLE_RECORDS, EXAMPLE_RULES) is False


def test_one_record_fails():
    records = [{"a": 1}, {"a": -1}]
    rules = [("a", lambda v: v > 0)]
    assert all_records_match_rules(records, rules) is False


def test_one_rule_fails():
    records = [{"a": 1, "b": 2}]
    rules = [("a", lambda v: v > 0), ("b", lambda v: v > 10)]
    assert all_records_match_rules(records, rules) is False


def test_empty_records():
    # No records -> vacuously true
    assert all_records_match_rules([], [("a", lambda v: False)]) is True


def test_empty_rules():
    assert all_records_match_rules([{"a": 1}], []) is True


def test_missing_key_fails():
    records = [{"a": 1}]  # no "b"
    rules = [("b", lambda v: True)]
    # Contract: missing key -> fail (KeyError or explicit check)
    try:
        result = all_records_match_rules(records, rules)
        assert result is False
    except KeyError:
        pass  # also acceptable
