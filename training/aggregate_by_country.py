from collections import defaultdict
from rich import print

orders = [
    {"order_id": "A1", "customer_id": "C1", "amount": 100.0, "country": "FR"},
    {"order_id": "A2", "customer_id": "C2", "amount": 200.0, "country": "FR"},
    {"order_id": "A3", "customer_id": "C1", "amount": 50.0, "country": "DE"},
    {"order_id": "A4", "customer_id": "C3", "amount": 300.0, "country": "FR"},
]


def aggregate_by_country(orders: list[dict]) -> dict:
    result = defaultdict(lambda: {
        "customers": set(),
        "order_count": 0,
        "total_amount": 0
    })
    for order in orders:
        country = order["country"]
        result[country]["order_count"] += 1
        result[country]["total_amount"] += order["amount"]
        result[country]["customers"].add(order["customer_id"])

    final_result = {}
    for country, data in result.items():
        final_result[country] = {
            "unique_customers": len(data["customers"]),
            "order_count": data["order_count"],
            "total_amount": data["total_amount"]
        }
    return final_result

print(f"Result:", aggregate_by_country(orders))