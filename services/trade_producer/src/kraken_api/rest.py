import requests
from typing import List, Dict
import json

class KrakenRestAPI:
    URL = 'https://api.kraken.com/0/public/Trades?pair={product_id}&since={from_ms}'

    def __init__(
        self,
        product_id: str,
        from_ms: int,
        to_ms: int,
    ) -> None:
        self.product_id = product_id
        self.from_ms = from_ms
        self.to_ms = to_ms
        self._is_done = False

    def get_trades(self) -> List[Dict]:
        """
        Fetches a batch of trades from the Kraken API.
        """
        payload = {}
        headers = {'accept': 'application/json'}
        url = self.URL.format(product_id=self.product_id, from_ms=int(self.from_ms // 1000)) #convert from milliseconds to seconds

        # breakpoint()

        response = requests.get(url, headers=headers, data=payload)

        data = json.loads(response.text)
        # extract the first key from the result
        pair = list(data['result'].keys())[0]

        if data['error']:
            raise Exception(data['error'])
        
        trades = [
            {
                'product_id': self.product_id,
                'price': float(trade[0]),
                'volume': float(trade[1]),
                'timestamp': int(trade[2]),
            } for trade in data['result'][pair]
        ]

        if int(data['result']['last']) >= self.to_ms:
            self._is_done = True

        return trades

    def is_done(self) -> bool:
        """
        Checks if all historical data has been produced.
        """
        return self._is_done
