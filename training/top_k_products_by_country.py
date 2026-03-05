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
    users_by_country = {}

    for user in users:
        users_by_country[user["user_id"]] = user["country"]

    result = {}
    products_by_country = defaultdict(lambda: defaultdict(int))
    for o in orders:
        product = o["product"]
        country = users_by_country.get(o["user_id"])
        if not country:
            continue
        products_by_country[country][product] += 1

    for country in products_by_country:
        print(f"{country}: ")
        heap = []
        for product, count in products_by_country[country].items():
            print(f"{product} : {count}")
            if len(heap) < k:
                heapq.heappush(heap, [count, product])
            elif count > heap[0][0]:
                heapq.heappushpop(heap, [count, product])

        print(f"heap", heap)
        sorted_heap = sorted(heap, reverse=True)
        result[country] = [(h[1], h[0]) for h in sorted_heap]

    return result


print(top_k_products_by_country(users, orders, 2))