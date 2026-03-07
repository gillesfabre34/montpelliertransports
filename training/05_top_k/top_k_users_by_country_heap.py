"""
Top-K per partition: in each country, the K users who spent the most.
Concept: groupby (country, user_id) → sum, then heap per country.
"""
from collections import defaultdict
from rich import print
import heapq

orders = [
    {"user_id": 1, "country": "FR", "amount": 120},
    {"user_id": 2, "country": "FR", "amount": 80},
    {"user_id": 1, "country": "FR", "amount": 30},
    {"user_id": 3, "country": "FR", "amount": 200},
    {"user_id": 4, "country": "US", "amount": 50},
    {"user_id": 5, "country": "US", "amount": 300},
    {"user_id": 4, "country": "US", "amount": 70},
    {"user_id": 6, "country": "US", "amount": 20},
]


def top_k_users_by_country(orders: list[dict], k: int = 2) -> list[dict]:
    """
    Return the k users who spent the most in each country.
    """
    totals: dict[tuple[str, int], int] = defaultdict(int)
    for o in orders:
        totals[(o["country"], o["user_id"])] += o["amount"]

    heaps: dict[str, list[tuple[int, int]]] = defaultdict(list)
    for (country, user_id), total_amount in totals.items():
        heapq.heappush(heaps[country], (total_amount, user_id))
        if len(heaps[country]) > k:
            heapq.heappop(heaps[country])

    result = []
    for country, heap in heaps.items():
        for rank, (total_amount, user_id) in enumerate(
            sorted(heap, key=lambda x: x[0], reverse=True), start=1
        ):
            result.append({
                "country": country,
                "user_id": user_id,
                "total_amount": total_amount,
                "rank": rank
            })
    return result


print(top_k_users_by_country(orders, 2))
