from kafka.admin import KafkaAdminClient, NewTopic
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_topic_if_not_exists(
    bootstrap_servers, topic_name, num_partitions=1, replication_factor=1
):
    try:
        # Initialize Kafka Admin Client
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

        # Fetch current topics
        existing_topics = admin_client.list_topics()

        if topic_name in existing_topics:
            logger.info(f"Topic '{topic_name}' already exists.")
            return

        # Define new topic
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )

        # Create topic
        admin_client.create_topics(new_topics=[topic], validate_only=False)
        logger.info(f"Topic '{topic_name}' created successfully.")

    except TopicAlreadyExistsError:
        logger.warning(f"Topic '{topic_name}' already exists. (Caught Exception)")
    except NoBrokersAvailable:
        logger.error(
            "No Kafka brokers available. Check 'bootstrap_servers' configuration."
        )
    except Exception as e:
        logger.error(f"Failed to create topic '{topic_name}': {e}")
    finally:
        admin_client.close()
