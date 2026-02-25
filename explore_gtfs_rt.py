import requests
from typing import Dict, Any
from google.transit import gtfs_realtime_pb2
from pprint import pprint

URBAIN_VEHICLE_POSITIONS_URL = (
    "https://data.montpellier3m.fr/GTFS/Urbain/VehiclePosition.pb"
)


def fetch_feed(url: str) -> gtfs_realtime_pb2.FeedMessage:
    feed = gtfs_realtime_pb2.FeedMessage()
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    feed.ParseFromString(resp.content)
    return feed


def vehicle_to_dict(
    entity: gtfs_realtime_pb2.FeedEntity,
    source: str = "Montpellier_Urbain",
) -> Dict[str, Any]:
    """Convert a GTFS-RT vehicle entity to a flat dict for Kafka."""
    if not entity.HasField("vehicle"):
        return {}

    vehicle = entity.vehicle
    trip = vehicle.trip
    position = vehicle.position

    return {
        "entity_id": entity.id,
        "trip_id": trip.trip_id,
        "route_id": trip.route_id,
        "latitude": float(position.latitude) if position.HasField("latitude") else None,
        "longitude": float(position.longitude) if position.HasField("longitude") else None,
        "bearing": float(position.bearing) if position.HasField("bearing") else None,
        "speed": float(position.speed) if position.HasField("speed") else None,
        "event_timestamp": int(vehicle.timestamp) if vehicle.timestamp else None,
        "source": source,
    }

def main() -> None:
    feed = fetch_feed(URBAIN_VEHICLE_POSITIONS_URL)
    print(f"Number of entities in feed: {len(feed.entity)}")

    for entity in list(feed.entity)[:5]:
        if not entity.HasField("vehicle"):
            continue

        vehicle = entity.vehicle
        trip = vehicle.trip
        position = vehicle.position

        print("-" * 60)
        print(f"entity_id      : {entity.id}")
        print(f"trip_id        : {trip.trip_id}")
        print(f"route_id       : {trip.route_id}")
        print(f"start_time     : {trip.start_time}")
        print(f"start_date     : {trip.start_date}")
        print(f"latitude       : {position.latitude}")
        print(f"longitude      : {position.longitude}")
        print(f"bearing        : {position.bearing}")
        print(f"speed          : {position.speed}")
        print(f"timestamp      : {vehicle.timestamp}")

        d = vehicle_to_dict(entity)
        if not d:
            continue
        pprint(d)


if __name__ == "__main__":
    main()

