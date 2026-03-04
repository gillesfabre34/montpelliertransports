from rich import print
from collections import defaultdict

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


def attach_user_info(orders: list[dict], users: list[dict]) -> list[dict]:
    result = []
    user_dict = {}
    for user in users:
        user_dict[user["user_id"]] = {
            "name": user["name"],
            "country": user["country"]
        }
    for order in orders:
        user_info = user_dict.get(order["user_id"])
        result.append({
            "user_id": order["user_id"],
            "order_id": order["order_id"],
            "name": user_dict[order["user_id"]]["name"] if user_info else None,
            "country": user_dict[order["user_id"]]["country"] if user_info else None,
            "amount": order["amount"]
        })
    return result


print(attach_user_info(orders, users))
