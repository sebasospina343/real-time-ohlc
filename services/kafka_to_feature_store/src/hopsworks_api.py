import hopsworks
from typing import Dict, List
import pandas as pd
from config import config
from loguru import logger

def push_data_to_feature_store(
    feature_group_name: str,
    feature_group_version: int,
    data: List[Dict],
    online_or_offline: str,
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

    if not data:
        logger.warning("No data to push to feature store, skipping insert")
        return
    
    df = pd.DataFrame(data)
    
    if df.empty:
        logger.warning("DataFrame is empty, skipping insert")
        return
    
    logger.info(f"Pushing {len(df)} records with columns: {list(df.columns)}")
    ohlc_feature_group.insert(df, write_options={"start_offline_materialization": True if online_or_offline == 'offline' else False})