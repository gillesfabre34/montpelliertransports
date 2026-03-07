"""
Filtering: exclude records whose key is in a list (blacklist).
Concept: set for O(1) lookup, loop, filter.
"""
from rich import print

blacklist = ["U1", "U5", "U20"]

users = [
    {"user_id": "U1", "name": "Alice", "country": "FR"},
    {"user_id": "U2", "name": "Bob", "country": "DE"},
    {"user_id": "U3", "name": "Charlie", "country": "FR"},
    {"user_id": "U4", "name": "David", "country": "ES"},
    {"user_id": "U5", "name": "Eve", "country": "IT"},
]


def filter_blacklisted_users(users: list[dict], blacklist: list[str]) -> list[dict]:
    result = []
    set_blacklist = set(blacklist)
    for user in users:
        if user["user_id"] not in set_blacklist:
            result.append(user)
    return result


print(filter_blacklisted_users(users, blacklist))
