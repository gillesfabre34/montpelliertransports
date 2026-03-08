"""
F1 — Distributed top-K (local top-K per partition, then merge to global top-K)

STATEMENT
--------
Simulate a distributed setting: data is split into partitions (e.g. by country or by shard).
First compute the top-K within each partition (local top-K). Then merge all local top-K lists
and compute the final global top-K. Pattern used in Spark, MapReduce, Flink.

Input:  list of records (e.g. (key, count)) and a partition function; or pre-partitioned dict.
Output: global top-K after merging local top-Ks.
"""
from rich import print

# Events per user, already grouped by "partition" (e.g. shard or region). Each partition has (user_id, count).
# Simulate 3 partitions; we want global top-2 users by event count.
PARTITION_A = [("u1", 10), ("u2", 30), ("u3", 5)]
PARTITION_B = [("u4", 25), ("u5", 8)]
PARTITION_C = [("u6", 40), ("u7", 12), ("u8", 3)]

PARTITIONS = {"A": PARTITION_A, "B": PARTITION_B, "C": PARTITION_C}

# Expected: local top-2 per partition → A: u2(30), u1(10); B: u4(25), u5(8); C: u6(40), u7(12).
# Merge: (30,25,40,10,8,12) → global top-2: u6(40), u2(30).
EXPECTED_TOP_K_DISTRIBUTED = [("u6", 40), ("u2", 30)]


def top_k_distributed(
    partitions: dict[str, list[tuple[str, int]]],
    k: int,
) -> list[tuple[str, int]]:
    """
    From each partition, take local top-K by value (second element of tuple); then merge and return global top-K.

    Args:
        partitions: dict mapping partition_id to list of (key, value) e.g. (user_id, count).
        k: number of top items to return globally.

    Returns:
        list of (key, value) for global top-K, ordered by value descending.
    """
    raise NotImplementedError


if __name__ == "__main__":
    print(top_k_distributed(PARTITIONS, 2))
