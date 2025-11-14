import hopsworks
from .config import config
from typing import List, Dict
import time

class OhlcDataReader:
    def __init__(
        self,
        ohlc_window_sec:int,
        feature_view_name:str,
        feature_view_version:int,
        feature_group_name:str,
        feature_group_version:int,
        last_n_minutes:int,
    ):
        self.ohlc_window_sec = ohlc_window_sec
        self.feature_view_name = feature_view_name
        self.feature_view_version = feature_view_version
        self.feature_group_name = feature_group_name
        self.feature_group_version = feature_group_version
        self.last_n_minutes = last_n_minutes

    def _get_primary_keys(self, last_n_minutes: int) -> List[Dict]:
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

    def get_feature_view(
        self,
        feature_group_name:str,
        feature_group_version:int,
        feature_view_name:str,
        feature_view_version:int,
    ):
        project = hopsworks.login(
            project=config.project_name,
            api_key_value=config.api_key,
        )

        fs = project.get_feature_store()

        feature_group = fs.get_feature_group(name=feature_group_name, version=feature_group_version)

        feature_view = fs.get_or_create_feature_view(
            name=feature_view_name,
            version=feature_view_version,
            query=feature_group.select_all()
        )

        return feature_view

    def read_from_online_store(self):
        feature_view = self.get_feature_view(
            feature_group_name=self.feature_group_name,
            feature_group_version=self.feature_group_version,
            feature_view_name=self.feature_view_name,
            feature_view_version=self.feature_view_version,
        )
        data = feature_view.get_batch_data()
        #sort by timestamp
        data = data.sort_values(by='timestamp')

        return data

    def read_from_offline_store(self, last_n_days_to_fetch_from_store:int):
        feature_view = self.get_feature_view(
            feature_group_name=self.feature_group_name,
            feature_group_version=self.feature_group_version,
            feature_view_name=self.feature_view_name,
            feature_view_version=self.feature_view_version,
        )
        data = feature_view.get_feature_vectors(
            entry=self._get_primary_keys(last_n_minutes=last_n_days_to_fetch_from_store * 24 * 60),
            return_type="pandas"
        )
        #sort by timestamp ascending
        data = data.sort_values(by='timestamp', ascending=True)
        return data