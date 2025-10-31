from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

class Config(BaseSettings):
    kafka_topic: str = "ohlc"
    kafka_broker_address: str = os.environ['KAFKA_BROKER_ADDRESS']
    feature_group_name: str = "ohlc_feature_group"
    feature_group_version: int = 1
    project_name: str = os.environ['HOPSWORKS_PROJECT_NAME']
    api_key: str = os.environ['HOPSWORKS_API_KEY']
    buffer_size: int = 1

config = Config()