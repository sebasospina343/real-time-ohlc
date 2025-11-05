from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv, find_dotenv
from typing import Optional

load_dotenv(find_dotenv())

class Config(BaseSettings):
    kafka_input_topic: str = os.environ.get('KAFKA_INPUT_TOPIC')
    kafka_output_topic: str = os.environ.get('KAFKA_OUTPUT_TOPIC')
    kafka_broker_address:Optional[str] = None
    ohlc_windows_seconds: int = os.environ['OHLC_WINDOWS_SECONDS']

config = Config()