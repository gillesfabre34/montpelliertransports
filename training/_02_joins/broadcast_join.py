"""
F3 — Broadcast join (small table in memory, scan big table once)

STATEMENT
--------
When one table is small and the other large: load the small one into a dict (broadcast),
then scan the big table once and lookup the small table for each row. Avoids shuffling the big table.
In Spark: broadcast(small_df); join with big_df.

Input:  small list (e.g. dimension table: id → attributes), large list (e.g. facts with id).
Output: large list enriched with attributes from the small table (same as hash join, but emphasis on small vs large).
"""
from rich import print

# Small dimension: product_id → name, category.
PRODUCTS = [
    {"product_id": "P1", "name": "Widget", "category": "A"},
    {"product_id": "P2", "name": "Gadget", "category": "B"},
    {"product_id": "P3", "name": "Gizmo", "category": "A"},
]

# Large fact table: many orders with product_id (repeated many times).
ORDERS = [
    {"order_id": "O1", "product_id": "P1", "quantity": 2},
    {"order_id": "O2", "product_id": "P2", "quantity": 1},
    {"order_id": "O3", "product_id": "P1", "quantity": 3},
    {"order_id": "O4", "product_id": "P3", "quantity": 1},
    {"order_id": "O5", "product_id": "P2", "quantity": 2},
]

# Expected: each order row enriched with name and category from PRODUCTS (broadcast side).


def broadcast_join(
    small: list[dict],
    large: list[dict],
    join_key: str,
    small_key: str | None = None,
) -> list[dict]:
    """
    Enrich each row of large with fields from small, joined on join_key. Small is the "broadcast" (build a dict once).

    Args:
        small: list of dicts (dimension / lookup table).
        large: list of dicts (fact table).
        join_key: key to join on (e.g. "product_id").
        small_key: if different from join_key in small, specify; else assumed same as join_key.

    Returns:
        list of dicts: each row of large with added fields from the matching row in small.
    """
    raise NotImplementedError


if __name__ == "__main__":
    print(broadcast_join(PRODUCTS, ORDERS, "product_id"))
