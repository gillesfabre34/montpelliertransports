"""GTFS-RT producer: fetch vehicle positions, convert to JSON, publish to Kafka."""

import json
import logging
import time
from typing import Any, Dict

import requests
from confluent_kafka import Producer
from google.transit import gtfs_realtime_pb2

from config import (
    GTFS_RT_SOURCE,
    GTFS_RT_VEHICLE_POSITIONS_URL,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_VEHICLE_POSITIONS_RAW,
    POLL_INTERVAL_SECONDS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def vehicle_to_dict(
    entity: gtfs_realtime_pb2.FeedEntity,
    source: str = GTFS_RT_SOURCE,
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


def fetch_feed(url: str) -> gtfs_realtime_pb2.FeedMessage:
    """Fetch and parse GTFS-RT feed from URL."""
    feed = gtfs_realtime_pb2.FeedMessage()
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    feed.ParseFromString(resp.content)
    return feed


def run_producer() -> None:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    }
    producer = Producer(conf)
    topic = KAFKA_TOPIC_VEHICLE_POSITIONS_RAW

    def delivery_callback(err, msg):
        if err:
            logger.error("Delivery failed: %s", err)
        elif msg:
            logger.debug("Produced to %s [%d]", msg.topic(), msg.partition())

    while True:
        try:
            feed = fetch_feed(GTFS_RT_VEHICLE_POSITIONS_URL)
            count = 0
            routes = set()
            for entity in feed.entity:
                payload = vehicle_to_dict(entity)
                routes.add(entity.vehicle.trip.route_id)
                if not payload:
                    continue
                value = json.dumps(payload).encode("utf-8")
                producer.produce(
                    topic,
                    value=value,
                    key=payload.get("entity_id", "").encode("utf-8"),
                    callback=delivery_callback,
                )
                count += 1
            producer.flush(timeout=10)
            print("Routes actives:", routes)
            logger.info("Produced %d vehicle positions to %s", count, topic)
        except Exception as e:
            logger.exception("Error fetching or producing: %s", e)

        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    run_producer()