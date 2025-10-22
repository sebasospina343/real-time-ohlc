from datetime import timedelta
from config import config
from loguru import logger
from typing import Dict

def init_ohlc_candle(value: Dict) -> Dict:
    return {
        'open': value['price'],
        'high': value['price'],
        'low': value['price'],
        'close': value['price'],
        'product_id': value['product_id'],
    }

def update_ohlc_candle(ohlc_candle: Dict, trade: Dict) -> Dict:
    return {
        'open': ohlc_candle['open'],
        'high': max(ohlc_candle['high'], trade['price']),
        'low': min(ohlc_candle['low'], trade['price']),
        'close': trade['price'],
        'product_id': trade['product_id'],
    }

def trade_to_ohlc(
    kafka_input_topic: str,
    kafka_output_topic: str,
    kafka_broker_address: str,
    ohlc_windows_seconds: int,
) -> None:
    """
    Converts trades to OHLCs.
    """

    from quixstreams import Application

    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="trade_to_ohlc",
    )

    input_topic = app.topic(name=kafka_input_topic, value_deserializer='json')
    output_topic = app.topic(name=kafka_output_topic, value_serializer='json')

    sdf = app.dataframe(input_topic)

    # Apply transformation
    sdf = sdf.tumbling_window(duration_ms=timedelta(seconds=ohlc_windows_seconds))
    sdf = sdf.reduce(reducer=update_ohlc_candle, initializer=init_ohlc_candle).current()

    sdf['open'] = sdf['value']['open']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['product_id'] = sdf['value']['product_id']
    sdf['timestamp'] = sdf['end']

    # keep only relevant values
    sdf = sdf[['timestamp','open', 'high', 'low', 'close', 'product_id']]

    # Print the result
    sdf = sdf.update(logger.info)
    
    sdf = sdf.to_topic(output_topic)

    app.run(sdf)

if __name__ == '__main__':
    trade_to_ohlc(
        kafka_input_topic=config.kafka_input_topic,
        kafka_output_topic=config.kafka_output_topic,
        kafka_broker_address=config.kafka_broker_address,
        ohlc_windows_seconds=config.ohlc_windows_seconds,
    )
