import json
import os
from typing import Dict, Any

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException, Message
from pandas import DataFrame

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
        topic_name = 'riot_account_search'
        consumer.subscribe([topic_name])
        return consumer
    except KafkaException as e:
        print(f"Failed to create Kafka consumer: {e}")
        raise

def subscribe_topic(consumer: Consumer, topic_name: str) -> Consumer:
    """
    Subscribes the Kafka consumer to the specified topic.

    :param topic_name: Name of the Kafka topic to subscribe to
    :raises KafkaException: If there is an error subscribing to the topic
    """
    try:
        consumer.subscribe([topic_name])
        return consumer
    except KafkaException as e:
        print(f"Failed to subscribe to topic {topic_name}: {e}")
        raise

def parse_json_from_message(message: str) -> json:
    """
    Parses the message from the Kafka topic and returns a DataFrame with the specified features.

    :param message: Message from the Kafka topic
    :return: DataFrame with the specified features
    """
    inner_str = message.strip('\\')
    data = json.loads(json.loads(inner_str))
    return data

def parse_feature_from_json(json_data: json) -> DataFrame:
    """
    Parses the message from the Kafka topic and returns a DataFrame with the specified features.

    :param message: Message from the Kafka topic
    :return: DataFrame with the specified features
    """
    df = DataFrame([json_data])
    df = df[['champLevel', 'damagesPerMinute', 'damageTakenOnTeamPercentage', 'goldPerMinute', 'teamDamagePercentage', 'kda']]
    return df