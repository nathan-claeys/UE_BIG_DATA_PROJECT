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
            return True

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
