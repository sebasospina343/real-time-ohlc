from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

class Config(BaseSettings):
    product_id: str = 'BTC/USD'
    kafka_broker_address: str = os.environ.get('KAFKA_BROKER_ADDRESS')
    kafka_topic_name: str = 'trade'
    ohlc_windows_seconds: int = os.environ.get('OHLC_WINDOWS_SECONDS')
    live_or_historical: str = os.environ.get('LIVE_OR_HISTORICAL')
    last_n_days: int = 7

config = Config()