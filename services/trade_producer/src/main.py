from quixstreams import Application
from src.kraken_api import KrakenWebsocketTradeAPI
from typing import List, Dict
from loguru import logger
from src.config import config


def produce_trades(
    kafka_broker_address: str,
    kafka_topic_name: str,
    product_id: str,
) -> None:
    """
    Reads trades from a Kraken API.

    Args:
        kafka_broker_address: The address of the Kafka broker.
        kafka_topic: The topic to produce the trades to.

    Returns:
        None
    """
    app = Application(broker_address=kafka_broker_address)

    topic = app.topic(name=kafka_topic_name, value_serializer='json')

    kraken_api = KrakenWebsocketTradeAPI(product_id=product_id)

    with app.get_producer() as producer:
        while True:
            trades: List[Dict] = kraken_api.get_trades()

            for trade in trades:
                message = topic.serialize(key=trade['product_id'], value=trade)
                producer.produce(
                    topic=kafka_topic_name, value=message.value, key=message.key
                )
                logger.info(f'Message sent to Kafka: {trade}')

            from time import sleep

            sleep(1)


if __name__ == '__main__':
    produce_trades(
        kafka_broker_address=config.kafka_broker_address,
        kafka_topic_name=config.kafka_topic_name,
        product_id=config.product_id,
    )
