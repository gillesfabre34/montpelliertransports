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
    raise NotImplementedError


if __name__ == "__main__":
    print(lag_lead(PRICE_HISTORY, "product_id", "timestamp", "price"))
