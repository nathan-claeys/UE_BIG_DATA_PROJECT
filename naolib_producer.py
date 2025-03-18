import requests
from kafka import KafkaProducer
import json
from kafka_setup import create_topic_if_not_exists
import threading

# Kafka configuration
kafka_config = {
    "bootstrap_servers": "localhost:9095",  # Update with your Kafka broker
}
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
                stops.append({"codeLieu": stop["codeLieu"], "libelle": stop["libelle"]})
                break

    return stops

def get_trams_gare_nord():
    url = "https://open.tan.fr/ewp/horairesarret.json/GSNO" 
    response_1_1 = requests.get(url + "1/1/2") # tram 1 sens 1
    response_1_2 = requests.get(url + "2/1/1") # tram 1 sens 2
    if response_1_1.status_code == 200 and response_1_2.status_code == 200 :
        data_1_1 = response_1_1.json()
        data_1_2 = response_1_2.json()
        return data_1_1.horaires, data_1_2.horaires # "horaires":[{"heure":"4h","passages":["50d"]},{"heure":"5h","passages":["12","32","52"]}]
    else:
        print(f"Failed to fetch data: {response_1_1.status_code}, {response_1_2.status_code}")
        return [],[]

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

        url = f"https://open.tan.fr/ewp/tempsattentelieu.json/{stop['codeLieu']}/1/{line_name}"

        response = requests.get(url)

        if response.status_code == 200:
            data = response.json()

            # Publish each entry to Kafka
            for info in data:
                info["stop"] = stop["codeLieu"]
                producer.send(topic, value=info)
                records += 1

        else:
            print(f"Failed to fetch data: {response.status_code}")

    producer.flush()
    print(f"Sent {records} records.")

def send_trams_gare_nord():
    topic = "trams_gare_nord"

    # Initialize Kafka Producer
    producer = KafkaProducer(
        bootstrap_servers=kafka_config["bootstrap_servers"],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )

    print("Starting to collect tram data...")

    records = 0

    for horaire in get_trams_gare_nord():
        producer.send(topic, value=horaire)
        records += 1
    
    producer.flush()
    print(f"Sent {records} records.")


def run_periodic(interval_sec, stop_event, task, task_args=()):
    while not stop_event.is_set():
        task(*task_args)
        # Wait for the interval or until stop is requested
        stop_event.wait(interval_sec)


if __name__ == "__main__":

    create_topic_if_not_exists(kafka_config["bootstrap_servers"], "bus_position")
    create_topic_if_not_exists(kafka_config["bootstrap_servers"], "trams_gare_nord")

    # Non periodic task
    send_trams_gare_nord()

    stop_event = threading.Event()

    # Start the periodic thread
    thread = threading.Thread(
        target=run_periodic, args=(60, stop_event, send_bus_position, ("C6",))
    )
    thread.start()
