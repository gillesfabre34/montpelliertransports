"""Producer configuration: Kafka and GTFS-RT endpoints."""

# Kafka
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC_VEHICLE_POSITIONS_RAW = "vehicle_positions_raw"

# GTFS-RT Montpellier (urbain)
GTFS_RT_VEHICLE_POSITIONS_URL = (
    "https://data.montpellier3m.fr/GTFS/Urbain/VehiclePosition.pb"
)
GTFS_RT_SOURCE = "montpellier_Urbain"

# Polling
POLL_INTERVAL_SECONDS = 30
