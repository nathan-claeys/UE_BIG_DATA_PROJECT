import json
import logging
import requests
from kafka import KafkaProducer

# Configuration and constants
KAFKA_CONFIG = {
    "bootstrap_servers": "kafka1:9092",  # Update with your Kafka broker
}
BASE_URL = "https://data.nantesmetropole.fr"
DATASET_PATH = (
    "/api/explore/v2.1/catalog/datasets/"
    "244400404_disponibilite-temps-reel-"
    "velos-libre-service-naolib-nantes-metropole/records"
)
TOPIC = "bike_stations"

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def build_query_params(position: tuple, distance_in_kilometers: int) -> dict:
    """
    Build query parameters for the API request.

    :param position: Tuple containing (longitude, latitude).
    :param distance_in_kilometers: Search radius in kilometers.
    :return: Dictionary with query parameters.
    """
    # Construct the position string as required by the API.
    position_str = f"geom'POINT({position[0]} {position[1]})'"
    distance_str = f"{distance_in_kilometers}km"

    return {
        "where": f"within_distance(position, {position_str}, {distance_str})",
        "order_by": f"distance(position, {position_str})",
        "limit": 5,
        "timezone": "Europe/Paris",
    }


def get_nearby_bike_stations(
    position: tuple, distance_in_kilometers: int
) -> list:
    """
    Fetch nearby bike stations from the API.

    :param position: Tuple containing (longitude, latitude).
    :param distance_in_kilometers: Search radius in kilometers.
    :return: List of bike station records.
    """
    params = build_query_params(position, distance_in_kilometers)
    url = f"{BASE_URL}{DATASET_PATH}"

    try:
        response = requests.get(url, params=params, timeout=5)
        response.raise_for_status()  # Raises an HTTPError for bad responses
        data = response.json()
        return data.get("results", [])
    except requests.RequestException as e:
        logger.error(f"Failed to fetch data: {e}")
        return []


def send_bike_stations(position: tuple, radius: int):
    """
    Collect bike station data and send each record to a Kafka topic.

    :param position: Tuple containing (longitude, latitude).
    :param radius: Search radius in kilometers.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG["bootstrap_servers"],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
    except Exception as e:
        logger.error(f"Failed to initialize KafkaProducer: {e}")
        return

    logger.info("Starting to collect bike station data...")
    bike_stations = get_nearby_bike_stations(position, radius)

    if not bike_stations:
        logger.warning("No bike stations found.")
        return

    records_sent = 0
    for station in bike_stations:
        try:
            producer.send(TOPIC, value=station)
            records_sent += 1
        except Exception as e:
            logger.error(f"Error sending station data: {e}")

    producer.flush()
    producer.close()
    logger.info(f"Sent {records_sent} records.")


if __name__ == "__main__":
    # IMT Atlantique coordinates
    sample_position = (-1.520754797081473, 47.282105501965894)
    sample_radius = 10
    send_bike_stations(sample_position, sample_radius)
