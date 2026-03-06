import pandas as pd
from rich import print

events = [
    {"event_id": "e1", "user_id": "u1", "event_type": "click"},
    {"event_id": "e2", "user_id": "u2", "event_type": "view"},
    {"event_id": "e3", "user_id": "u1", "event_type": "view"},
    {"event_id": "e4", "user_id": "u3", "event_type": "click"},
    {"event_id": "e5", "user_id": "u1", "event_type": "click"},
    {"event_id": "e6", "user_id": "u2", "event_type": "view"},
    {"event_id": "e7", "user_id": "u4", "event_type": "click"},
    {"event_id": "e8", "user_id": "u2", "event_type": "click"},
]


def top_k_active_users(events: list[dict], k: int) -> pd.DataFrame:
    df = pd.DataFrame(events)\
        .groupby("user_id")\
        .size()\
        .reset_index(name="event_count")\
        .sort_values(by="event_count", ascending=False)\
        .head(k)
    return df


print(top_k_active_users(events, 2))