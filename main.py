import json
import sys
import os

import pandas as pd
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

from src.ml import inference_by_puuid
from src.kafka import  parse_feature_from_json, parse_json_from_message
from src.db import get_collection, get_db, insert_data_to_ml_inference, query_by_puuid

if __name__ == "__main__":

    # Get "data_source" collection
    data_source_collection =  get_collection("data_source")

    # # Subscribe to "data_source" topic
    # consumer = get_consumer()
    # consumer = subscribe_topic(consumer, "riot_account_search")

    consumer_conf = {
        'bootstrap.servers': '3.38.212.52:9092',
        'group.id': 'my_group',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(**consumer_conf)
    topic_name = 'riot_match_rating'
    consumer.subscribe([topic_name])

    # Consuming loop
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            print(msg)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('파티션 끝 도달 {0}/{1}'.format(msg.topic(), msg.partition()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                kafka_message_string = msg.value().decode('utf-8')
                print('Received Kafka Message: {0}'.format(kafka_message_string))
                json_data = parse_json_from_message(kafka_message_string) 
                print(json_data)  
                puuid = json_data["puuid"]
                df = parse_feature_from_json(json_data)
                lp_teer = inference_by_puuid(df)
                print(df.shape,lp_teer)
                insert_data_to_ml_inference(puuid, lp_teer)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
