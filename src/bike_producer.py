import json
import requests
from kafka import KafkaProducer

kafka_config = {
    "bootstrap_servers": "kafka1:9092",  # Update with your Kafka broker
}


def get_nearby_bike_stations(
    position: tuple, distance_in_kilometers: int
) -> list:
    base_url = "https://data.nantesmetropole.fr"
    bike_stations_url = (
        "/api/explore/v2.1/catalog/datasets/"
        "244400404_disponibilite-temps-reel-"
        "velos-libre-service-naolib-nantes-metropole/records"
    )
    position_str = f"geom%27POINT({position[0]}%20{position[1]})%27"
    distance_str = f"{distance_in_kilometers}km"
    filter_query = (
        f"?where=within_distance(position%2C%20{position_str}"
        f"%2C%20{distance_str})"
    )
    order_query = f"&order_by=distance(position%2C%20{position_str})"

    limit_query = "&limit=20"
    timezone_query = "&timezone=Europe%2FParis"

    response = requests.get(
        f"{base_url}{bike_stations_url}{filter_query}"
        f"{order_query}{limit_query}{timezone_query}",
        timeout=5,
    )
    if response.status_code == 200:
        data = response.json()
        return data["results"]
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return []


def send_bike_stations(position, radius):
    topic = "bike_stations"

    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Starting to collect bike station data...")

    records = 0

    bike_stations = get_nearby_bike_stations(position, radius)

    for station in bike_stations:
        producer.send(topic, value=station)
        records += 1

    producer.flush()
    print(f"Sent {records} records.")
