import requests
from kafka import KafkaProducer
import json
import logging
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_topic_if_not_exists(
    bootstrap_servers: str,
    topic_name: str,
    num_partitions: int = 1,
    replication_factor: int = 1,
) -> bool:
    admin_client = None
    try:
        # Initialize Kafka Admin Client
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

        # Fetch current topics
        existing_topics = admin_client.list_topics()

        if topic_name in existing_topics:
            logger.info(
                "Topic '%s' already exists.",
                topic_name,
            )
            return

        # Define new topic
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )

        # Create topic
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        logger.info("Topic '%s' created successfully.", topic_name)
        return True

    except TopicAlreadyExistsError:
        logger.warning(
            "Topic '%s' already exists (caught exception).",
            topic_name,
        )
        return False

    except NoBrokersAvailable:
        logger.error(
            "No Kafka brokers available."
            " Check 'bootstrap_servers'configuration."
        )
        return False

    except Exception as e:  # pylint: disable=broad-except
        logger.error(
            "Failed to create topic %s: %s",
            topic_name,
            e,
        )
        return False

    finally:
        if admin_client is not None:
            admin_client.close()

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