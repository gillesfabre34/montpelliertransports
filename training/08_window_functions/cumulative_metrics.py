"""
D2 — Cumulative metrics (running sum per partition)

STATEMENT
--------
Compute a running total per key (e.g. per user): for each row, the cumulative sum
of a numeric column (e.g. amount) ordered by timestamp within the partition.

Equivalent SQL: SUM(amount) OVER (PARTITION BY user_id ORDER BY timestamp)

Used for: dashboards, spend tracking, running balances.

Input:  list of records with partition key (e.g. user_id), order key (timestamp), value (amount).
Output: same records with an extra field for cumulative sum per partition (or list of dicts with cumsum).
"""
from collections import defaultdict
from rich import print

# Transactions per user, ordered by time: compute running total spent per user.
TRANSACTIONS = [
    {"user_id": "U1", "timestamp": 1000, "amount": 10},
    {"user_id": "U2", "timestamp": 1001, "amount": 25},
    {"user_id": "U1", "timestamp": 1002, "amount": 15},
    {"user_id": "U1", "timestamp": 1005, "amount": 20},
    {"user_id": "U2", "timestamp": 1006, "amount": 5},
]

# Expected: each row gets a cumulative sum within its user_id (order by timestamp).
# U1: 10 → 10+15=25 → 25+20=45.  U2: 25 → 25+5=30.
EXPECTED_CUMULATIVE_METRICS = [
    {"user_id": "U1", "timestamp": 1000, "amount": 10, "cumsum": 10},
    {"user_id": "U2", "timestamp": 1001, "amount": 25, "cumsum": 25},
    {"user_id": "U1", "timestamp": 1002, "amount": 15, "cumsum": 25},
    {"user_id": "U1", "timestamp": 1005, "amount": 20, "cumsum": 45},
    {"user_id": "U2", "timestamp": 1006, "amount": 5, "cumsum": 30},
]


def cumulative_sum_by_partition(
    rows: list[dict],
    partition_key: str,
    order_key: str,
    value_key: str,
) -> list[dict]:
    """
    Add a cumulative sum of value_key per partition_key, ordered by order_key.

    Args:
        rows: list of dicts with partition_key, order_key, value_key.
        partition_key: e.g. "user_id".
        order_key: e.g. "timestamp".
        value_key: e.g. "amount".

    Returns:
        list of dicts with same fields plus "cumsum" (or similar) per partition.
    """
    users = defaultdict(list)
    sums = defaultdict(int)
    for r in rows:
        partition_value = r[partition_key]
        sums[partition_value] += r[value_key]
        t_copy = r.copy()
        t_copy["cumsum"] = sums[partition_value]
        users[partition_value].append(t_copy)

    result = []
    for user_id, events in users.items():
        for event in events:
            result.append(event)

    result = sorted(result, key=lambda x: x[order_key])
    return result



if __name__ == "__main__":
    print(cumulative_sum_by_partition(TRANSACTIONS, "user_id", "timestamp", "amount"))
