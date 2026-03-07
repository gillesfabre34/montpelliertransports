from collections import defaultdict
from rich import print
import heapq
import pandas as pd
from pandas.api.typing import Rolling

users = [
    {"user_id": "u1", "country": "FR"},
    {"user_id": "u2", "country": "FR"},
    {"user_id": "u3", "country": "US"},
    {"user_id": "u4", "country": "DE"},
]

orders = [
    {"order_id": "o1", "user_id": "u1", "product": "p1"},
    {"order_id": "o2", "user_id": "u2", "product": "p2"},
    {"order_id": "o3", "user_id": "u1", "product": "p1"},
    {"order_id": "o4", "user_id": "u3", "product": "p3"},
    {"order_id": "o5", "user_id": "u4", "product": "p2"},
    {"order_id": "o6", "user_id": "u1", "product": "p2"},
    {"order_id": "o7", "user_id": "u2", "product": "p1"},
]


def top_k_products_by_country(users: list[dict], orders: list[dict], k: int) -> pd.DataFrame:
    df_users = pd.DataFrame(users)
    df_orders = pd.DataFrame(orders)

    return (
        df_users
        .merge(df_orders, how="outer", on="user_id")
        .groupby(["country", "product"])
        .size()
        .rename("order_count")
        .reset_index()
        .sort_values(["country", "order_count"], ascending=[True, False])\
        .groupby("country")
        .head(k)
        .reset_index(drop=True)
    )


print(top_k_products_by_country(users, orders, 2))