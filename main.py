import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException

from src.ml import inference_by_puuid
from src.kafka import get_consumer, subscribe_topic
from src.db import get_collection, get_db, query_by_puuid

if __name__ == "__main__":

    # Get "data_source" collection
    data_source_collection =  get_collection("data_source")

    # Subscribe to "data_source" topic
    consumer = get_consumer()
    subscribe_topic(consumer, "data_source")

    # Consuming loop
    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print('파티션 끝 도달 {0}/{1}'.format(msg.topic(), msg.partition()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                print('받은 메시지: {0}'.format(msg.value().decode('utf-8')))
                puuid = msg.value().decode('utf-8')
                data = query_by_puuid(puuid, data_source_collection)
                ml_inference_document = {
                    # key-value 추가
                    "lp_teer": inference_by_puuid(data)
                }
                print(ml_inference_document)
                break
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
