import os
from typing import Dict, Any

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

from src.db import query_by_puuid


KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL")

def get_consumer() -> Consumer:
    """
    Creates and returns a Kafka consumer configured with the specified settings.

    :return: Kafka consumer object
    :raises KafkaException: If there is an error creating the Kafka consumer
    """
    try:
        consumer_conf: Dict[str, Any] = {
            'bootstrap.servers': KAFKA_BROKER_URL,
            'group.id': 'my_group',
            'auto.offset.reset': 'earliest'
        }
        consumer: Consumer = Consumer(**consumer_conf)
        return consumer
    except KafkaException as e:
        print(f"Failed to create Kafka consumer: {e}")
        raise

def subscribe_topic(consumer: Consumer, topic_name: str) -> None:
    """
    Subscribes the Kafka consumer to the specified topic.

    :param topic_name: Name of the Kafka topic to subscribe to
    :raises KafkaException: If there is an error subscribing to the topic
    """
    try:
        consumer.subscribe([topic_name])
    except KafkaException as e:
        print(f"Failed to subscribe to topic {topic_name}: {e}")
        raise

