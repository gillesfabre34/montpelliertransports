from rich import print


events = [
    {"event_id": "E1", "user_id": "U1", "timestamp": 1000, "event_type": "click"},
    {"event_id": "E2", "user_id": "U2", "timestamp": 1001, "event_type": "view"},
    {"event_id": "E1", "user_id": "U1", "timestamp": 1000, "event_type": "click"},
    {"event_id": "E3", "user_id": "U3", "timestamp": 1002, "event_type": "purchase"},
    {"event_id": "E2", "user_id": "U2", "timestamp": 1001, "event_type": "view"},
]


def deduplicate_events(events: list[dict]) -> list[dict]:
    event_ids = set()
    result = []
    for event in events:
        if event["event_id"] not in event_ids:
            result.append(event)
            event_ids.add(event["event_id"])
    return result

print(deduplicate_events(events))