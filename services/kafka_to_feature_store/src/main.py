from quixstreams import Application
from loguru import logger
import json
from src.hopsworks_api import push_data_to_feature_store
from src.config import config
from typing import Optional
from datetime import datetime, timezone

def get_current_utc_seconds() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def kafka_to_feature_store(
    kafka_topic: str,
    kafka_broker_address: str,
    feature_group_name: str,
    feature_group_version: int,
    buffer_size: Optional[int] = None,
    live_or_historical: Optional[str] = 'live',
) -> None:
    """
    Converts Kafka messages to feature store.
    """
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="kafka_to_feature_store",
        auto_offset_reset="earliest"
    )

    last_saved_to_feature_ts = get_current_utc_seconds()

    buffer = []
    
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])

        while True:
            msg = consumer.poll(1)


            if msg is None:
                n_sec = 10
                if(get_current_utc_seconds() - last_saved_to_feature_ts) > n_sec:
                    logger.info(f"Time exceeded. Pushing data to feature store: {kafka_topic}")
                    push_data_to_feature_store(
                        feature_group_name=feature_group_name,
                        feature_group_version=feature_group_version,
                        data=buffer,
                        online_or_offline='online' if live_or_historical == 'live' else 'offline',
                    )
                    buffer = []
                    last_saved_to_feature_ts = get_current_utc_seconds()
                else:
                    continue

            if msg.error():
                logger.error(f"kafka_to_feature_store Error: {msg.error()}")
                continue

            ohlc_candle = json.loads(msg.value().decode('utf-8'))

            buffer.append(ohlc_candle)

            logger.info(f"Message received from Kafka: {ohlc_candle}")

            if len(buffer) >= buffer_size:
                logger.debug(buffer)
                push_data_to_feature_store(
                    feature_group_name=feature_group_name,
                    feature_group_version=feature_group_version,
                    data=buffer,
                    online_or_offline='online' if live_or_historical == 'live' else 'offline',
                )

                buffer = []
                last_saved_to_feature_ts = get_current_utc_seconds()

            consumer.store_offsets(message=msg)

if __name__ == "__main__":
    try:
        kafka_to_feature_store(
            kafka_topic=config.kafka_topic,
            kafka_broker_address=config.kafka_broker_address,
            feature_group_name=config.feature_group_name,
            feature_group_version=config.feature_group_version,
            buffer_size=config.buffer_size,
            live_or_historical=config.live_or_historical,
        )
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received, exiting...")