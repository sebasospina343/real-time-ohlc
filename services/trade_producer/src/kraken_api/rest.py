import requests
from typing import List, Dict
import json
from datetime import datetime, timezone
from typing import Tuple
from loguru import logger
from time import sleep
from .Trade import Trade

class KrakenRestAPI:
    URL = 'https://api.kraken.com/0/public/Trades?pair={product_id}&since={since_sec}'

    def __init__(
        self,
        product_id: str,
        last_n_days: int,
    ) -> None:
        self.product_id = product_id
        self.from_ms, self.to_ms = self._init_from_to_ms(last_n_days)
        self._is_done = False
        self.last_trade_ms = self.from_ms
        self.last_n_days = last_n_days

    @staticmethod
    def _init_from_to_ms(last_n_days: int)->Tuple[int, int]:
       # Milliseconds in a day: 24 hours * 60 minutes * 60 seconds * 1000 milliseconds
        today_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        to_ms = int(today_date.timestamp() * 1000)
        from_ms = to_ms - (last_n_days * 24 * 60 * 60 * 1000)

        return from_ms, to_ms

    def get_trades(self) -> List[Trade]:
        """
        Fetches a batch of trades from the Kraken API.
        """
        payload = {}
        headers = {'accept': 'application/json'}

        since_sec = self.last_trade_ms // 1000
        url = self.URL.format(product_id=self.product_id, since_sec=since_sec) #convert from milliseconds to seconds

        response = requests.get(url, headers=headers, data=payload)

        data = json.loads(response.text)

        if ('error' in data and data['error'] != []):
            logger.info("Too many requests. Sleeping for 30 seconds.")
            sleep(30)

        
        pair = list(data['result'].keys())[0]

        trades = [Trade(
            product_id=self.product_id,
            price=float(trade[0]),
            volume=float(trade[1]),
            timestamp_ms=int(trade[2]) * 1000,
        ) for trade in data['result'][pair]]

        if trades [-1].timestamp_ms > self.last_trade_ms:
            self.last_trade_ms = trades[-1].timestamp_ms + 1
        else:
            self.last_trade_ms = trades[-1].timestamp_ms

        trades = [trade for trade in trades if trade.timestamp_ms <= self.to_ms]

        last_ts_in_ns = int(data['result']['last'])
        self.last_trade_ms = last_ts_in_ns // 1_000_000 #convert from nanoseconds to milliseconds
        self._is_done = self.last_trade_ms >= self.to_ms

        # slow down the rate
        sleep(1)
        return trades

    def is_done(self) -> bool:
        """
        Checks if all historical data has been produced.
        """
        return self._is_done
