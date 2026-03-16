from __future__ import annotations
import shutil
from collections.abc import Iterable, Iterator
from itertools import count
from pathlib import Path
from utils.tools import logg

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

EXAMPLE_LINES = ["header", "row1", "row2", "row3", "row4", "row5", "row6"]
EXAMPLE_CHUNK_SIZE = 2
EXPECTED_CHUNKS = [["header", "row1"], ["row2", "row3"], ["row4", "row5"], ['row6']]


def line_chunks(lines: Iterable[str], chunk_size: int) -> Iterator[list[str]]:
    """Yield chunks of at most chunk_size lines. Generator (use yield)."""
    if not lines:
        return
    bucket = []
    for line in lines:
        if len(bucket) < chunk_size:
            bucket.append(line)
        else:
            yield bucket
            bucket = [line]
    if bucket:
        yield bucket


"""CASE WHEN THE ITERABLE IS A LIST"""


# def line_chunks(lines: list[str], chunk_size: int):
#     """Yield chunks of at most chunk_size lines. Generator (use yield)."""
#     if not lines:
#         return
#     elif len(lines) <= chunk_size:
#         yield lines
#     else:
#         start_chunk = 0
#         while start_chunk < len(lines):
#             yield lines[start_chunk:start_chunk + chunk_size]
#             start_chunk += chunk_size


# print(list(line_chunks(gen(), EXAMPLE_CHUNK_SIZE)))


# print(list(line_chunks(("a", "b", "c", "d", "e"), EXAMPLE_CHUNK_SIZE)))
# print(list(line_chunks(EXAMPLE_LINES, EXAMPLE_CHUNK_SIZE)))


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


def test_iterable_is_tuple():
    lines = ("a", "b", "c", "d", "e")
    assert list(line_chunks(lines, 2)) == [["a", "b"], ["c", "d"], ["e"]]


def test_iterable_is_generator():
    def gen():
        for item in ["a", "b", "c", "d", "e"]:
            yield item

    assert list(line_chunks(gen(), 3)) == [["a", "b", "c"], ["d", "e"]]


def test_iterable_is_file_object(tmp_path):
    # Copy CSV mocks in tmp_path to isolate the test
    project_root = Path(__file__).resolve().parents[2]
    src_csv = project_root / "consumer" / "mocks" / "vehicles_sample.csv"
    dst_csv = tmp_path / "vehicles_sample.csv"
    shutil.copy(src_csv, dst_csv)

    with dst_csv.open("r", encoding="utf-8") as f:
        chunks = list(line_chunks(f, 3))

    # The example CSV has 4 lines: 1 header + 3 lines of data
    assert chunks[0][0].startswith("entity_id,trip_id,route_id")
    assert len(chunks[0]) == 3  # header + 2 first lines
    assert len(chunks[1]) == 1  # last line alone
