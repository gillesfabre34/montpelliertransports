from __future__ import annotations

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 11 — Build dict from keys and values using zip
-------------------------------------------------------
You are given a list of keys and a list of values of the same length.
Return the dictionary mapping each key to the corresponding value.
Use the built-in zip() and dict() (or a dict comprehension with zip).

Example: keys = ["a", "b", "c"], values = [1, 2, 3]
Expected: {"a": 1, "b": 2, "c": 3}

Constraints:
- Use zip()
- Assume keys and values have the same length
"""

EXAMPLE_KEYS = ["a", "b", "c"]
EXAMPLE_VALUES = [1, 2, 3]
EXPECTED_OUTPUT = {"a": 1, "b": 2, "c": 3}


def dict_from_zip(keys: list[str], values: list[int]) -> dict[str, int]:
    """Return dict mapping keys[i] to values[i]. Use zip()."""
    return dict(zip(keys, values))


print(dict_from_zip(EXAMPLE_KEYS, EXAMPLE_VALUES))


# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert dict_from_zip(EXAMPLE_KEYS, EXAMPLE_VALUES) == EXPECTED_OUTPUT


def test_empty():
    assert dict_from_zip([], []) == {}


def test_single():
    assert dict_from_zip(["x"], [42]) == {"x": 42}


def test_duplicate_keys():
    # dict(zip(...)): last value wins for duplicate keys
    assert dict_from_zip(["a", "a"], [1, 2]) == {"a": 2}


def test_values_any_type():
    # If signature is generic, values could be list[Any]; here we use int per spec
    keys = ["name", "age"]
    values = [1, 2]  # placeholder ints
    assert dict_from_zip(keys, values) == {"name": 1, "age": 2}
