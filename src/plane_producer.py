import requests
from kafka import KafkaProducer
import json
from src.topics import create_topic_if_not_exists

API_PLANE_URL = "https://opensky-network.org/api/flights/arrival?airport=LFRS&begin=1742166015&end=1742252385"

# Kafka configuration
kafka_config = {
    "bootstrap_servers": "localhost:9095",  # Update with your Kafka broker
}
stops_information = None

def get_plane_arrival():
    response = requests.get(API_PLANE_URL) #[{"icao24":"39ca84","firstSeen":1742251213,"estDepartureAirport":"LFRS","lastSeen":1742252046,"estArrivalAirport":"LFRS","callsign":"SAMU44  ","estDepartureAirportHorizDistance":6603,"estDepartureAirportVertDistance":239,"estArrivalAirportHorizDistance":3326,"estArrivalAirportVertDistance":155,"departureAirportCandidatesCount":0,"arrivalAirportCandidatesCount":0}]
    if response.status_code == 200:
        return response.json()
    else:
        print({response.status_code}, {response.text})
        return []

def send_plane_arrival():
    print("Starting to collect plane arrival data...")
    producer = KafkaProducer(bootstrap_servers=kafka_config["bootstrap_servers"],
                             value_serializer=lambda v: json.dumps(v).encode("utf-8"))
    plane_arrival = get_plane_arrival()
    
    records = 0

    for plane in plane_arrival:
        producer.send("plane_arrival", plane)
        records += 1

    print(f"Sent {records} records to Kafka")
    producer.flush()


if __name__ == "__main__":
    create_topic_if_not_exists(kafka_config["bootstrap_servers"],"plane_arrival")

    # Non periodic producer
    send_plane_arrival()