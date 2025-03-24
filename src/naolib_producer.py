from datetime import datetime
import json
import threading
import requests
from kafka import KafkaProducer
from topics import create_topic_if_not_exists
from bike_producer import get_nearby_bike_stations, send_bike_stations

# Kafka configuration
kafka_config = {
    "bootstrap_servers": "kafka1:9092",  # Update with your Kafka broker
}


stops_information = None


TEST_POSITION = (-1.520754797081473, 47.282105501965894)
TEST_RADIUS = 10


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
    stops = {}
    for stop in get_stops_information():
        for ligne in stop["ligne"]:
            if ligne["numLigne"] == line_name:
                stops[stop["codeLieu"]] = stop["libelle"]
                break

    return stops


def get_bus_airport():
    url = "https://open.tan.fr/ewp/horairesarret.json/AEPO1"  # aeroport
    response_38 = requests.get(url + "/38/1/2025-03-17")  # ligne 38
    response_98 = requests.get(url + "/98/1/2025-03-17")  # ligne 98
    try:
        if response_38.status_code == 200 and response_98.status_code == 200:
            data_38 = response_38.json()
            data_98 = response_98.json()
            return (
                data_38.get("horaires"),
                data_98.get("horaires"),
            )  # "horaires":[{"heure":"4h","passages":["50d"]},{"heure":"5h","passages":["12","32","52"]}]
        else:
            print(
                f"Failed to fetch data: {response_38.status_code},"
                f" {response_98.status_code}"
            )
            return [], []
    except Exception as e:
        print(f"Failed to fetch data: {e}")


def send_bus_position(line_name):
    topic = "bus_position"

    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Starting to collect bus position data...")

    records = 0

    time = datetime.now().isoformat()

    for stop in get_stops_of_line(line_name).keys():

        url = f"https://open.tan.fr/ewp/tempsattentelieu.json/{stop}/1/{line_name}"

        try:
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()

                # Publish each entry to Kafka
                for info in data:
                    info["stop"] = stop
                    info["codeArret"] = info["arret"]["codeArret"]
                    info["numLigne"] = info["ligne"]["numLigne"]
                    del info["ligne"]
                    del info["arret"]
                    info["created_at"] = time
                    producer.send(topic, value=info)
                    records += 1

            else:
                print(f"Failed to fetch data: {response.status_code}")
        except Exception as e:
            print(f"Failed to fetch data: {e}")

    producer.flush()
    print(f"Sent {records} records.")


def send_bus_airport():
    topic = "bus_airport"

    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Starting to collect bus data...")

    records = 0

    data1, data2 = get_bus_airport()

    for info in data1:
        info["bus"] = 38
        producer.send(topic, value=info)
        records += 1

    for info in data2:
        info["bus"] = 98
        producer.send(topic, value=info)
        records += 1

    producer.flush()
    print(f"Sent {records} records.")


def get_lines_from_stop(stop_name):
    lines = []
    for stop in get_stops_information():
        if stop["codeLieu"] == stop_name:
            for line in stop["ligne"]:
                lines.append(line["numLigne"])
    return lines


def send_bus_affluence(stop_name):
    lines = get_lines_from_stop(stop_name[:4])

    print(lines)

    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Starting to collect bus affluence data...")

    records = 0

    for line in lines:
        url = f"https://open.tan.fr/ewp/horairesarret.json/{stop_name}/{line}/1"

        print(url)
        print("line : ")
        print(line)

        try:
            response = requests.get(url)

            if response.status_code == 200:
                data = response.json()
                print("data :")
                print(data)

                num_ligne = line
                stop = stop_name

                print("records :")
                print(records)

                # Process each schedule
                for horaire in data.get("horaires", []):
                    heure = horaire.get("heure")
                    for passage in horaire.get("passages", []):
                        message = {
                            "numLigne": num_ligne,
                            "stop": stop,
                            "heure": heure,
                            "passage": passage,
                        }
                        producer.send("bus_affluence_horaire", value=message)
                        records += 1

            else:
                print(f"Failed to fetch data: {response.status_code}")

        except Exception as e:
            print(f"Error fetching data for {stop_name}, line {line}: {e}")

    producer.flush()
    print(f"Sent {records} records.")


def run_periodic(interval_sec, stop_event, task, task_args=()):
    while not stop_event.is_set():
        task(*task_args)
        # Wait for the interval or until stop is requested
        stop_event.wait(interval_sec)


def main(selected_bike_coordinates):
    create_topic_if_not_exists(
        kafka_config["bootstrap_servers"],
        "bus_position",
    )
    create_topic_if_not_exists(
        kafka_config["bootstrap_servers"],
        "bus_airport",
    )
    create_topic_if_not_exists(
        kafka_config["bootstrap_servers"],
        "bike_stations",
    )
    create_topic_if_not_exists(
        kafka_config["bootstrap_servers"], "bus_affluence_horaire"
    )

    # Non periodic task
    send_bus_airport()

    stop_event = threading.Event()

    send_bus_affluence("CRQU4")

    # Create the periodic thread for bus position data
    bus_thread = threading.Thread(
        target=run_periodic, args=(60, stop_event, send_bus_position, ("C6",))
    )

    # Create another periodic thread for bike station data
    bike_thread = threading.Thread(
        target=run_periodic,
        args=(
            30,
            stop_event,
            send_bike_stations,
            (selected_bike_coordinates, TEST_RADIUS),
        ),
    )

    # Start both threads
    bus_thread.start()
    bike_thread.start()

    try:
        # Keep the main thread alive while the periodic threads are running
        while bus_thread.is_alive() or bike_thread.is_alive():
            bus_thread.join(timeout=1)
            bike_thread.join(timeout=1)
    except KeyboardInterrupt:
        print("Interrupt received, shutting down gracefully...")
        stop_event.set()
        bus_thread.join()
        bike_thread.join()


if __name__ == "__main__":
    main()
