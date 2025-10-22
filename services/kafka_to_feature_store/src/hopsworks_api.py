import hopsworks
from typing import Dict
import pandas as pd
from src.config import config
from loguru import logger

def push_data_to_feature_store(
    feature_group_name: str,
    feature_group_version: str,
    data: Dict,
) -> None:
    """
    Pushes data to feature store.
    """
    project = hopsworks.login(
        project=config.project_name,
        api_key_value=config.api_key,
    )

    fs = project.get_feature_store()

    try:
        ohlc_feature_group = fs.get_or_create_feature_group(
            name=feature_group_name,
            version=feature_group_version,
            description="OHLC feature group",
            primary_key=["product_id","timestamp"],
            event_time="timestamp",
            online_enabled=True,
        )
    except Exception as e:
        logger.error(f"Error creating feature group: {e}")
        raise e

    data = pd.DataFrame([data])

    ohlc_feature_group.insert(data)