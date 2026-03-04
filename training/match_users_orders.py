from collections import defaultdict
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


def match_users_orders(users: list[dict], orders: list[dict]) -> list[dict]:
    result = []
    user_dict = {}
    for user in users:
        user_dict[user["user_id"]] = {
            "name": user["name"],
        }
    for order in orders:
        user_info = user_dict.get(order["user_id"])
        result.append({
            "user_id": order["user_id"],
            "order_id": order["order_id"],
            "name": user_dict[order["user_id"]]["name"] if user_info else None,
            "amount": order["amount"]
        })
    return result


print(match_users_orders(users, orders))
