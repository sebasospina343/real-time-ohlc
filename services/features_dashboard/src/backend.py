import hopsworks
import pandas as pd
from src.config import config
from typing import List, Dict

def get_primary_keys(last_n_minutes: int) -> List[Dict]:
    import time
    current_utc = int(time.time() * 1000)
    current_utc = current_utc - (current_utc % 60000)

    timestamps = [current_utc - i * 60000 for i in range(last_n_minutes)]
    primary_keys = [
        {
            "product_id": "BTC/USD",
            "timestamp": timestamp
        } for timestamp in timestamps
    ]

    return primary_keys

def get_feature_view():
    project = hopsworks.login(
        project=config.project_name,
        api_key_value=config.api_key,
    )

    fs = project.get_feature_store()

    feature_group = fs.get_feature_group(name=config.feature_group_name, version=config.feature_group_version)

    feature_view = fs.get_or_create_feature_view(
        name=config.feature_view_name,
        version=config.feature_view_version,
        query=feature_group.select_all()
    )

    return feature_view


def get_features_from_fs(online_or_offline: str) -> pd.DataFrame:
    """
    Get features from feature store.
    """
    feature_view = get_feature_view()

    if online_or_offline == 'online':
        features: pd.DataFrame = feature_view.get_batch_data()
    else :
        features: pd.DataFrame = feature_view.get_feature_vectors(
            entry=get_primary_keys(last_n_minutes=24*60*2),
            return_type="pandas"
        )

    #sort by timestamp
    features = features.sort_values(by='timestamp')

    return features