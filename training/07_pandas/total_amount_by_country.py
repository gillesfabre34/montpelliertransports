"""
Pandas: join (merge) + groupby + aggregation (sum) + sort.
Same goal as country aggregation, with the DataFrame API.
"""
from rich import print
import pandas as pd

users = [
    {"user_id": "u1", "country": "FR"},
    {"user_id": "u2", "country": "FR"},
    {"user_id": "u3", "country": "US"},
    {"user_id": "u4", "country": "DE"},
]
orders = [
    {"order_id": "o1", "user_id": "u1", "amount": 100},
    {"order_id": "o2", "user_id": "u2", "amount": 50},
    {"order_id": "o3", "user_id": "u1", "amount": 70},
    {"order_id": "o4", "user_id": "u3", "amount": 200},
    {"order_id": "o5", "user_id": "u4", "amount": 30},
]


def total_amount_by_country(users: list[dict], orders: list[dict]) -> pd.DataFrame:
    df_users = pd.DataFrame(users)
    df_orders = pd.DataFrame(orders)
    return (
        df_users.merge(df_orders, how="inner", on="user_id")
        .groupby("country", as_index=False)
        .agg(total_amount=("amount", "sum"))
        .sort_values(by="total_amount", ascending=False)
    )


print(total_amount_by_country(users, orders))
