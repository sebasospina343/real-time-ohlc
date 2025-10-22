from quixstreams import Application
from loguru import logger
import json
from src.hopsworks_api import push_data_to_feature_store
from src.config import config

def kafka_to_feature_store(
    kafka_topic: str,
    kafka_broker_address: str,
    feature_group_name: str,
    feature_group_version: int,
) -> None:
    """
    Converts Kafka messages to feature store.
    """
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="kafka_to_feature_store"
    )
    
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])

        while True:
            msg = consumer.poll(1)


            if msg is None:
                continue

            if msg.error():
                logger.error(f"kafka_to_feature_store Error: {msg.error()}")
                continue

            ohlc_candle = json.loads(msg.value().decode('utf-8'))
            logger.info(f"Message received from Kafka: {ohlc_candle}")


            push_data_to_feature_store(
                feature_group_name=feature_group_name,
                feature_group_version=feature_group_version,
                data=ohlc_candle,
            )

            consumer.store_offsets(message=msg)

if __name__ == "__main__":
    kafka_to_feature_store(
        kafka_topic=config.kafka_topic,
        kafka_broker_address=config.kafka_broker_address,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
    )