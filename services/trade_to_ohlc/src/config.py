from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

class Config(BaseSettings):
    kafka_input_topic: str = "trade"
    kafka_output_topic: str = "ohlc"
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    ohlc_windows_seconds: int = os.environ['OHLC_WINDOWS_SECONDS']


config = Config()