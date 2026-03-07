"""
Basics: filtering, manual groupby, defaultdict, Counter, sort, max per group.
Several small patterns on the same dataset (vehicles).
"""
from statistics import mean
from rich import print
from collections import defaultdict, Counter

vehicle_positions = [
    {"vehicle_id": "V1", "route": "L1", "speed": 32, "delay": 5,  "type": "bus"},
    {"vehicle_id": "V2", "route": "L1", "speed": 28, "delay": -2, "type": "tram"},
    {"vehicle_id": "V3", "route": "L2", "speed": 41, "delay": 0,  "type": "bus"},
    {"vehicle_id": "V4", "route": "L3", "speed": 25, "delay": 12, "type": "metro"},
    {"vehicle_id": "V5", "route": "L2", "speed": 38, "delay": 3,  "type": "tram"},
]


def get_late_vehicles(vehicles):
    return [v for v in vehicles if v["delay"] > 0]


def average_speed_by_route(vehicles: list):
    routes = set([v["route"] for v in vehicles])
    averages = {}
    for route in routes:
        averages[route] = mean([v["speed"] for v in vehicles if v["route"] == route])
    return averages


def categorize_delay(vehicle):
    if vehicle["delay"] <= 0:
        return "on_time"
    elif vehicle["delay"] <= 5:
        return "small_delay"
    else:
        return "major_delay"


def vehicles_by_route(vehicles):
    route_vehicles = defaultdict(list)
    for v in vehicles:
        route_vehicles[v["route"]].append(v)
    return list(route_vehicles.values())


def flatten_vehicles(nested_list):
    return [v for sublist in nested_list for v in sublist]


def count_vehicles_by_route(vehicles):
    return dict(Counter(v["route"] for v in vehicles))


def delayed_vehicles(vehicles):
    return [v for v in vehicles if v["delay"] > 0]


def vehicles_sorted_by_speed(vehicles: list):
    return sorted(vehicles, key=lambda v: v["speed"], reverse=True)


def vehicles_grouped_by_type(vehicles: list):
    dict_types = defaultdict(list)
    for v in vehicles:
        dict_types[v["type"]].append(v)
    return dict(dict_types)


def fastest_vehicle_by_route(vehicles: list):
    fastest = {}
    for v in vehicles:
        route = v["route"]
        if route not in fastest or fastest[route]["speed"] < v["speed"]:
            fastest[route] = v
    return fastest


if __name__ == "__main__":
    print("Fastest vehicles by route:", fastest_vehicle_by_route(vehicle_positions))
