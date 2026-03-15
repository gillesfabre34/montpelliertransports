from __future__ import annotations

"""
LEVEL 5 — itertools, deque, generators, bisect, dict/sequence tricks

Exercise 3 — CSV chunks generator (yield)
-----------------------------------------
You are given an iterable of lines (strings), e.g. as read from a file.
Yield successive chunks of lines, each chunk being a list of at most
chunk_size lines. Use a generator (yield); do not load all lines into
a single list.

Example: lines = ["a", "b", "c", "d", "e"], chunk_size=2
Expected: first yield ["a","b"], then ["c","d"], then ["e"]

Constraints:
- Use yield (generator)
- Each yielded chunk is a list of at most chunk_size lines
- Last chunk may have fewer than chunk_size lines
"""

EXAMPLE_LINES = ["header", "row1", "row2", "row3", "row4", "row5"]
EXAMPLE_CHUNK_SIZE = 2
EXPECTED_CHUNKS = [["header", "row1"], ["row2", "row3"], ["row4", "row5"]]


def line_chunks(lines: list[str], chunk_size: int):
    """Yield chunks of at most chunk_size lines. Generator (use yield)."""
    ...


print(list(line_chunks(EXAMPLE_LINES, EXAMPLE_CHUNK_SIZE)))


# ----------------------
# Corner case tests
# ----------------------


def test_example():
    assert list(line_chunks(EXAMPLE_LINES, EXAMPLE_CHUNK_SIZE)) == EXPECTED_CHUNKS


def test_chunk_size_larger_than_list():
    assert list(line_chunks(["a", "b"], 10)) == [["a", "b"]]


def test_chunk_size_one():
    assert list(line_chunks(["a", "b", "c"], 1)) == [["a"], ["b"], ["c"]]


def test_empty_lines():
    assert list(line_chunks([], 2)) == []


def test_exact_multiple():
    assert list(line_chunks(["a", "b", "c", "d"], 2)) == [["a", "b"], ["c", "d"]]


def test_generator_consumed_once():
    gen = line_chunks(["a", "b"], 1)
    assert next(gen) == ["a"]
    assert next(gen) == ["b"]
    try:
        next(gen)
        assert False, "StopIteration expected"
    except StopIteration:
        pass
