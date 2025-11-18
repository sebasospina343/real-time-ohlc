from pydantic_settings import BaseSettings
import os
from dotenv import load_dotenv, find_dotenv
from typing import Optional

load_dotenv(find_dotenv())

OHLC_WINDOW_SEC = 60

class Config(BaseSettings):
    feature_view_name: str = os.environ.get('FEATURE_VIEW_NAME')
    feature_view_version: int = os.environ.get('FEATURE_VIEW_VERSION')
    feature_group_name: str = os.environ.get('FEATURE_GROUP_NAME')
    feature_group_version: int = os.environ.get('FEATURE_GROUP_VERSION')
    last_n_minutes: int = os.environ.get('LAST_N_MINUTES')
    ohlc_window_sec: int = OHLC_WINDOW_SEC
    prediction_window_sec: int = 60 * 5
    comet_project_name: str = os.environ.get('COMET_PROJECT_NAME')
    comet_api_key: str = os.environ.get('COMET_API_KEY')
    comet_workspace: str = os.environ.get('COMET_WORKSPACE')

config = Config()