"""
Hash join / enrichment: join orders with users to add name and country.
Concept: dict as index (user_id → info), then enrich each order.
"""
from rich import print

users = [
    {"user_id": "U1", "name": "Alice", "country": "FR"},
    {"user_id": "U2", "name": "Bob", "country": "DE"},
    {"user_id": "U3", "name": "Charlie", "country": "FR"},
]
orders = [
    {"order_id": "O1", "user_id": "U1", "amount": 100},
    {"order_id": "O2", "user_id": "U3", "amount": 200},
    {"order_id": "O3", "user_id": "U2", "amount": 150},
]


def enrich_orders_with_users(orders: list[dict], users: list[dict]) -> list[dict]:
    result = []
    user_dict = {u["user_id"]: {"name": u["name"], "country": u["country"]} for u in users}
    for order in orders:
        user_info = user_dict.get(order["user_id"])
        result.append({
            "user_id": order["user_id"],
            "order_id": order["order_id"],
            "name": user_info["name"] if user_info else None,
            "country": user_info["country"] if user_info else None,
            "amount": order["amount"]
        })
    return result


print(enrich_orders_with_users(orders, users))
