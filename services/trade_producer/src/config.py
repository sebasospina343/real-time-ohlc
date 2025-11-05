from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv, find_dotenv
from pydantic import field_validator

load_dotenv(find_dotenv())

class Config(BaseSettings):
    product_id: str = 'BTC/USD'
    kafka_broker_address: str = os.environ.get('KAFKA_BROKER_ADDRESS')
    kafka_topic_name: str = os.environ.get('KAFKA_TOPIC')
    ohlc_windows_seconds: int = os.environ.get('OHLC_WINDOWS_SECONDS')
    live_or_historical: str = os.environ.get('LIVE_OR_HISTORICAL')
    last_n_days: int = os.environ.get('LAST_N_DAYS')

    @field_validator('live_or_historical')
    @classmethod
    def validate_live_or_historical(cls, v: str) -> str:
        assert v in ['live', 'historical'], f'Invalid live_or_historical value: {v}'
        return v

config = Config()