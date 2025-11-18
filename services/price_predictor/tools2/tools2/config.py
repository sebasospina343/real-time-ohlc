from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

class Config(BaseSettings):
    project_name: str = os.environ.get('HOPSWORKS_PROJECT_NAME')
    api_key: str = os.environ.get('HOPSWORKS_API_KEY')

config = Config()