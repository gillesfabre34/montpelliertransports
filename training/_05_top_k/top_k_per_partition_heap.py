"""
Top-K per partition: for each country, the K most ordered products.
Concept: join user→country, groupby (country, product), heap per country.
"""
from collections import defaultdict
from rich import print
import heapq

users = [
    {"user_id": "U1", "country": "FR"},
    {"user_id": "U2", "country": "US"},
    {"user_id": "U3", "country": "FR"},
    {"user_id": "U4", "country": "US"},
    {"user_id": "U5", "country": "DE"},
]
orders = [
    {"order_id": "O1", "user_id": "U1", "product": "P1"},
    {"order_id": "O2", "user_id": "U2", "product": "P2"},
    {"order_id": "O3", "user_id": "U1", "product": "P3"},
    {"order_id": "O4", "user_id": "U3", "product": "P1"},
    {"order_id": "O5", "user_id": "U4", "product": "P2"},
    {"order_id": "O6", "user_id": "U2", "product": "P3"},
    {"order_id": "O7", "user_id": "U5", "product": "P1"},
    {"order_id": "O8", "user_id": "U1", "product": "P1"},
]


def top_k_products_by_country(users: list[dict], orders: list[dict], k: int) -> dict:
    users_by_country = {u["user_id"]: u["country"] for u in users}
    products_by_country: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for o in orders:
        country = users_by_country.get(o["user_id"])
        if not country:
            continue
        products_by_country[country][o["product"]] += 1

    result = {}
    for country, product_counts in products_by_country.items():
        heap: list[tuple[int, str]] = []
        for product, count in product_counts.items():
            if len(heap) < k:
                heapq.heappush(heap, (count, product))
            elif count > heap[0][0]:
                heapq.heappushpop(heap, (count, product))
        result[country] = [(p, c) for c, p in sorted(heap, reverse=True)]
    return result


print(top_k_products_by_country(users, orders, 2))
