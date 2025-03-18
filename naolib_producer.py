import requests
from kafka import KafkaProducer
import json
from kafka_setup import create_topic_if_not_exists
import threading
from constants import kafka_config

stops_information = None


def get_stops_information():
    global stops_information
    if stops_information is None:
        response = requests.get("https://open.tan.fr/ewp/arrets.json")
        if response.status_code == 200:
            stops_information = response.json()
        else:
            print(f"Failed to fetch data: {response.status_code}")
    return stops_information


def get_stops_of_line(line_name):
    stops = []
    for stop in get_stops_information():
        for ligne in stop["ligne"]:
            if ligne["numLigne"] == line_name:
                stops.append(
                    {"codeLieu": stop["codeLieu"], "libelle": stop["libelle"]}
                )
                break

    return stops


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


def send_bus_position(line_name):
    topic = "bus_position"

    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Starting to collect bus position data...")

    records = 0

    for stop in get_stops_of_line(line_name):

        url = (
            "https://open.tan.fr/ewp/tempsattentelieu.json/"
            f"{stop['codeLieu']}/"
            f"1/{line_name}"
        )

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()

            # Publish each entry to Kafka
            for info in data:
                info["stop"] = stop["codeLieu"]
                info["codeArret"] = info["arret"]["codeArret"]
                info["numLigne"] = info["ligne"]["numLigne"]
                del info["ligne"]
                del info["arret"]
                producer.send(topic, value=info)
                records += 1

        else:
            print(f"Failed to fetch data: {response.status_code}")

    producer.flush()
    print(f"Sent {records} records.")


def run_periodic(interval_sec, stop_event, task, task_args=()):
    while not stop_event.is_set():
        task(*task_args)
        # Wait for the interval or until stop is requested
        stop_event.wait(interval_sec)


if __name__ == "__main__":

    create_topic_if_not_exists(
        kafka_config["bootstrap_servers"],
        "bus_position",
    )
    create_topic_if_not_exists(
        kafka_config["bootstrap_servers"],
        "bike_stations",
    )

    TEST_POSITION = (-1.520754797081473, 47.282105501965894)
    TEST_RADIUS = 10
    print(get_nearby_bike_stations(TEST_POSITION, TEST_RADIUS))
    stop_event = threading.Event()

    # Start the periodic thread
    periodic_thread = threading.Thread(
        target=run_periodic, args=(60, stop_event, send_bus_position, ("C6",))
    )
    periodic_thread.start()

    try:
        # Keep the main thread alive while the periodic thread is running
        while periodic_thread.is_alive():
            periodic_thread.join(timeout=1)
    except KeyboardInterrupt:
        print("Interrupt received, shutting down gracefully...")
        stop_event.set()
        periodic_thread.join()
