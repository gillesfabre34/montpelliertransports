"""
D3 — Lag / Lead (compare row with previous or next)

STATEMENT
--------
For each row, get the value of a column from the previous (lag) or next (lead) row
within a partition, ordered by a key (e.g. timestamp). Used for: deltas, growth, anomaly detection.

Equivalent SQL: LAG(price) OVER (PARTITION BY product ORDER BY timestamp)

Input:  list of records with partition key, order key, and value column.
Output: same records with extra columns e.g. value_prev (lag), value_next (lead); first row has no prev, last has no next.
"""
from rich import print
from collections import defaultdict

# Price history per product: for each row, get previous and next price (for delta / trend).
PRICE_HISTORY = [
    {"product_id": "P1", "timestamp": 1000, "price": 100},
    {"product_id": "P1", "timestamp": 1002, "price": 105},
    {"product_id": "P1", "timestamp": 1005, "price": 102},
    {"product_id": "P2", "timestamp": 1001, "price": 50},
    {"product_id": "P2", "timestamp": 1004, "price": 55},
]

# Expected: each row gets e.g. price_prev (lag) and price_next (lead) within product_id.
# P1 first row: price_prev=None, price_next=105.  P1 second: price_prev=100, price_next=102.  etc.
EXPECTED_LAG_LEAD = [
    {"product_id": "P1", "timestamp": 1000, "price": 100, "price_prev": None, "price_next": 105},
    {"product_id": "P1", "timestamp": 1002, "price": 105, "price_prev": 100, "price_next": 102},
    {"product_id": "P1", "timestamp": 1005, "price": 102, "price_prev": 105, "price_next": None},
    {"product_id": "P2", "timestamp": 1001, "price": 50, "price_prev": None, "price_next": 55},
    {"product_id": "P2", "timestamp": 1004, "price": 55, "price_prev": 50, "price_next": None},
]


def lag_lead(
    rows: list[dict],
    partition_key: str,
    order_key: str,
    value_key: str,
) -> list[dict]:
    """
    Add value_prev (lag) and value_next (lead) per partition, ordered by order_key.

    Args:
        rows: list of dicts with partition_key, order_key, value_key.
        partition_key: e.g. "product_id".
        order_key: e.g. "timestamp".
        value_key: e.g. "price".

    Returns:
        list of dicts with same fields plus value_prev and value_next (None when no previous/next).
    """

    partitions = defaultdict(list)
    for r in rows:
        partitions[r[partition_key]].append(r.copy())
    print("partitions", partitions)

    result = []
    for p_key, r_copy in partitions.items():
        sorted_r_copy = sorted(r_copy, key=lambda x: x[order_key])
        print("sorted_r_copy", sorted_r_copy)
        print("len(sorted_r_copy)", len(sorted_r_copy))
        i = 0
        for element in sorted_r_copy:
            augmented_element = element
            nb_elts = len(sorted_r_copy)
            if i == 0:
                augmented_element["prev"] = None
                augmented_element["next"] = sorted_r_copy[1][value_key] if nb_elts > 1 else None
            elif i == nb_elts - 1:
                augmented_element["prev"] = sorted_r_copy[i - 1][value_key]
                augmented_element["next"] = None
            else:
                augmented_element["prev"] = sorted_r_copy[i - 1][value_key]
                augmented_element["next"] = sorted_r_copy[i + 1][value_key]
            i += 1
            result.append(augmented_element)

    return result


if __name__ == "__main__":
    print(lag_lead(PRICE_HISTORY, "product_id", "timestamp", "price"))
