from quixstreams import Application
from kraken_api.websocket import KrakenWebsocketTradeAPI
from kraken_api.rest import KrakenRestAPI
from typing import List, Dict
from loguru import logger
from config import config
import time

def produce_trades(
    kafka_broker_address: str,
    kafka_topic_name: str,
    product_id: str,
    live_or_historical: str,
    last_n_days: int,
) -> None:
    """
    Reads trades from a Kraken API.

    Args:
        kafka_broker_address: The address of the Kafka broker.
        kafka_topic: The topic to produce the trades to.
        product_id: The product ID to produce the trades for.
        live_or_historical: Whether to produce live or historical trades.
        las_n_days: The number of days to produce historical trades for.
    Returns:
        None
    """
    assert live_or_historical in ['live', 'historical'], f'Invalid live_or_historical value: {live_or_historical}'

    app = Application(broker_address=kafka_broker_address)

    topic = app.topic(name=kafka_topic_name, value_serializer='json')

    if live_or_historical == 'live':
        kraken_api = KrakenWebsocketTradeAPI(product_id=product_id)
    else:
        # Milliseconds in a day: 24 hours * 60 minutes * 60 seconds * 1000 milliseconds
        ms_per_day = 24 * 60 * 60 * 1000
        to_ms = int(time.time() * 1000)
        from_ms = to_ms - (last_n_days * ms_per_day)

        kraken_api = KrakenRestAPI(product_id=product_id, from_ms=from_ms, to_ms=to_ms)

    with app.get_producer() as producer:
        while True:
            if kraken_api.is_done():
                logger.info('All historical data produced')
                break


            trades: List[Dict] = kraken_api.get_trades()

            for trade in trades:
                message = topic.serialize(key=trade['product_id'], value=trade)
                producer.produce(
                    topic=kafka_topic_name, value=message.value, key=message.key
                )
                logger.info(f'Message sent to Kafka: {trade}')


if __name__ == '__main__':
    produce_trades(
        kafka_broker_address=config.kafka_broker_address,
        kafka_topic_name=config.kafka_topic_name,
        product_id=config.product_id,
        live_or_historical=config.live_or_historical,
        last_n_days=config.last_n_days,
    )
