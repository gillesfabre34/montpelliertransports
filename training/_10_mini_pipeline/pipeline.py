"""
H — Mini analytical pipeline

STATEMENT
--------
Using the shared dataset below (users, orders, events, products), implement the following analytics:
- Top products (by order count or revenue).
- Top users (by order count or event count).
- Revenue by country.
- Optional: cumulative revenue per user over time; event deduplication (keep latest per event_id).

Each function has its signature; implement as needed. Data is shared across all functions.
"""
from rich import print

# Shared dataset
USERS = [
    {"user_id": "u1", "country": "FR"},
    {"user_id": "u2", "country": "FR"},
    {"user_id": "u3", "country": "US"},
    {"user_id": "u4", "country": "DE"},
]

ORDERS = [
    {"order_id": "o1", "user_id": "u1", "product_id": "p1", "amount": 100},
    {"order_id": "o2", "user_id": "u2", "product_id": "p2", "amount": 50},
    {"order_id": "o3", "user_id": "u1", "product_id": "p1", "amount": 70},
    {"order_id": "o4", "user_id": "u3", "product_id": "p3", "amount": 200},
    {"order_id": "o5", "user_id": "u4", "product_id": "p2", "amount": 30},
]

EVENTS = [
    {"event_id": "e1", "user_id": "u1", "timestamp": 1000, "type": "click"},
    {"event_id": "e2", "user_id": "u2", "timestamp": 1001, "type": "view"},
    {"event_id": "e3", "user_id": "u1", "timestamp": 1002, "type": "purchase"},
    {"event_id": "e4", "user_id": "u1", "timestamp": 1005, "type": "click"},
]

PRODUCTS = [
    {"product_id": "p1", "name": "Widget"},
    {"product_id": "p2", "name": "Gadget"},
    {"product_id": "p3", "name": "Gizmo"},
]

# Expected outputs (for k=2 where applicable)
EXPECTED_TOP_PRODUCTS = [("p1", 2), ("p2", 2)]  # p1: 2 orders, p2: 2, p3: 1
EXPECTED_TOP_USERS_BY_REVENUE = [("u3", 200.0), ("u1", 170.0)]  # u3: 200, u1: 170, u2: 50, u4: 30
EXPECTED_REVENUE_BY_COUNTRY = [
    {"country": "FR", "total_revenue": 220},   # u1 + u2
    {"country": "US", "total_revenue": 200},   # u3
    {"country": "DE", "total_revenue": 30},    # u4
]
EXPECTED_TOP_USERS_BY_EVENT_COUNT = [("u1", 3), ("u2", 1)]  # u1: 3 events, u2: 1


def top_products(orders: list[dict], k: int) -> list[tuple[str, int]]:
    """
    Return the top-k products by order count: list of (product_id, count), ordered by count descending.
    """
    raise NotImplementedError


def top_users_by_revenue(orders: list[dict], k: int) -> list[tuple[str, float]]:
    """
    Return the top-k users by total order amount: list of (user_id, total_amount), ordered by total_amount descending.
    """
    raise NotImplementedError


def revenue_by_country(users: list[dict], orders: list[dict]) -> list[dict]:
    """
    Return one row per country with total revenue (sum of order amounts). Keys e.g. country, total_revenue.
    """
    raise NotImplementedError


def top_users_by_event_count(events: list[dict], k: int) -> list[tuple[str, int]]:
    """
    Return the top-k users by event count: list of (user_id, count), ordered by count descending.
    """
    raise NotImplementedError


if __name__ == "__main__":
    print("top_products:", top_products(ORDERS, 2))
    print("top_users_by_revenue:", top_users_by_revenue(ORDERS, 2))
    print("revenue_by_country:", revenue_by_country(USERS, ORDERS))
    print("top_users_by_event_count:", top_users_by_event_count(EVENTS, 2))
