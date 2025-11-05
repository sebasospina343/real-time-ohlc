from typing import List, Dict
from websocket import create_connection
import json
from loguru import logger
from datetime import datetime
from .Trade import Trade


class KrakenWebsocketTradeAPI:
    URL = 'wss://ws.kraken.com/v2'

    def __init__(self, product_id: str):
        self.product_id = product_id

        # establish connection
        self._ws = create_connection(self.URL)
        logger.info(f'Connected to Kraken API: {self.URL}')

        # subscribe to the product
        self._subscribe(product_id)

    def _subscribe(self, product_id: str) -> None:
        logger.info(f'Subscribing for Symbol: {product_id}')

        self._ws.send(
            json.dumps(
                {
                    'method': 'subscribe',
                    'params': {
                        'channel': 'trade',
                        'symbol': [product_id],
                        'snapshot': False,
                    },
                }
            )
        )

        logger.info('Subscription worked!!')

        # dumpping the first 2 messages
        _ = self._ws.recv()
        _ = self._ws.recv()

    def get_trades(self) -> List[Trade]:
        message = self._ws.recv()

        if 'heartbeat' in message:
            return []

        message = json.loads(message)  #

        trades = []
        for trade in message['data']:
            trades.append(
                # {
                #     'product_id': self.product_id,
                #     'price': trade['price'],
                #     'volume': trade['qty'],
                #     'timestamp': int(datetime.strptime(trade['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp() * 1000),
                # }
                Trade(
                    product_id=self.product_id,
                    price=trade['price'],
                    volume=trade['qty'],
                    timestamp_ms=int(datetime.strptime(trade['timestamp'], '%Y-%m-%dT%H:%M:%S.%fZ').timestamp() * 1000),
                )
            )

        return trades

    def is_done(self) -> bool:
        """
        The websocket never stops.
        """
        return False