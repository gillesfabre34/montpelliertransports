"""
Hash join: join orders and users on user_id to produce rows (order + name).
Concept: dict as index, scan orders with lookup.
"""
from rich import print

users = [
    {"user_id": "U1", "name": "Alice"},
    {"user_id": "U2", "name": "Bob"},
    {"user_id": "U3", "name": "Charlie"},
    {"user_id": "U4", "name": "David"},
]
orders = [
    {"order_id": "O1", "user_id": "U2", "amount": 100},
    {"order_id": "O2", "user_id": "U1", "amount": 200},
    {"order_id": "O3", "user_id": "U3", "amount": 50},
    {"order_id": "O4", "user_id": "U2", "amount": 75},
]


def join_orders_with_users(users: list[dict], orders: list[dict]) -> list[dict]:
    result = []
    user_dict = {u["user_id"]: u["name"] for u in users}
    for order in orders:
        name = user_dict.get(order["user_id"])
        result.append({
            "user_id": order["user_id"],
            "order_id": order["order_id"],
            "name": name,
            "amount": order["amount"]
        })
    return result


print(join_orders_with_users(users, orders))
