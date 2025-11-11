from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())

class Config(BaseSettings):
    feature_group_name: str = os.environ.get('FEATURE_GROUP_NAME')
    feature_group_version: int = os.environ.get('FEATURE_GROUP_VERSION')
    feature_view_name: str = os.environ.get('FEATURE_VIEW_NAME')
    feature_view_version: int = os.environ.get('FEATURE_VIEW_VERSION')
    project_name: str = os.environ.get('HOPSWORKS_PROJECT_NAME')
    api_key: str = os.environ.get('HOPSWORKS_API_KEY')

config = Config()