import pickle
from pydantic import BaseModel
from tools2.ohlc_data_reader import OhlcDataReader
from src.config import config
from loguru import logger
from comet_ml import Model

class PredictorOutput(BaseModel):
    prediction: float
    product_id: str
    predicted_timestamp: int
    predicted_timestamp_str: str

    def to_dict(self) -> dict:
        return {
            "prediction": self.prediction,
            "product_id": self.product_id,
            "predicted_timestamp": self.predicted_timestamp,
            "predicted_timestamp_str": self.predicted_timestamp_str,
        }


class Predictor:
    def __init__(self, model_path: str, ohlc_window_sec: int, feature_view_name: str, feature_view_version: int, feature_group_name: str, feature_group_version: int, last_n_minutes: int, prediction_window_sec: int, last_n_days_to_fetch_from_store: int, last_n_days_to_test_model: int):
        self.model_path = model_path
        self.ohlc_data_reader = OhlcDataReader(
            ohlc_window_sec=config.ohlc_window_sec,
            feature_view_name=config.feature_view_name,
            feature_view_version=config.feature_view_version,
            feature_group_name=config.feature_group_name,
            feature_group_version=config.feature_group_version,
            last_n_minutes=config.last_n_minutes,
        )
        self.model = self._load_model_pickle()

    @classmethod
    def from_model_registry(cls, model_name: str) -> 'Predictor':
        from comet_ml import API
        api = API(api_key=config.comet_api_key)
        model = api.get_model(workspace=config.comet_workspace, model_name=model_name)
        model_versions = model.find_versions(status='production')
        model_versions = sorted(model_versions, reverse=True)
        model_version = model_versions[0]

        model.download(version=model_version, output_folder='./')
        model_path = "./lasso_model.pkl"

        # Step 2: fetch metadata
        experiment_key = model.get_details(version=model_version)['experimentKey']

        experiment = api.get_experiment_by_key(experiment_key=experiment_key)
        # get parameters from the experiment
        ohlc_window_sec = experiment.get_parameters_summary('ohlc_window_sec')['valueCurrent']
        feature_view_name = experiment.get_parameters_summary('feature_view_name')['valueCurrent']
        feature_view_version = experiment.get_parameters_summary('feature_view_version')['valueCurrent']
        feature_group_name = experiment.get_parameters_summary('feature_group_name')['valueCurrent']
        feature_group_version = experiment.get_parameters_summary('feature_group_version')['valueCurrent']
        last_n_minutes = experiment.get_parameters_summary('last_n_minutes')['valueCurrent']
        prediction_window_sec = experiment.get_parameters_summary('prediction_window_sec')['valueCurrent']
        last_n_days_to_fetch_from_store = experiment.get_parameters_summary('last_n_days_to_fetch_from_store')['valueCurrent']
        last_n_days_to_test_model = experiment.get_parameters_summary('last_n_days_to_test_model')['valueCurrent']

        return cls(
            model_path=model_path, 
            ohlc_window_sec=ohlc_window_sec, 
            feature_view_name=feature_view_name, 
            feature_view_version=feature_view_version, 
            feature_group_name=feature_group_name, 
            feature_group_version=feature_group_version, 
            last_n_minutes=last_n_minutes, 
            prediction_window_sec=prediction_window_sec, 
            last_n_days_to_fetch_from_store=last_n_days_to_fetch_from_store, 
            last_n_days_to_test_model=last_n_days_to_test_model
        )

    
    def predict(self) -> PredictorOutput:
        # Step 1: Read the latest ohlc data from the online store
        ohlc_data = self.ohlc_data_reader.read_from_online_store()

        # Step 2: Preprocess data
        from src.training import interpolate_missing_candles
        ohlc_data = interpolate_missing_candles(ohlc_data)

        # Step 3: add features
        from src.feature_engineering import add_features
        ohlc_data = add_features(
            data=ohlc_data,
            timeperiod=14,
            n_candles_into_future=config.prediction_window_sec // config.ohlc_window_sec,
        )

        # Step 4: Extract metadata before selecting numeric columns
        # Get the last row's metadata for the prediction output
        last_row = ohlc_data.iloc[-1]
        product_id = str(last_row['product_id'])
        predicted_timestamp = int(last_row['timestamp'])
        predicted_timestamp_str = last_row['datetime'].strftime('%Y-%m-%d %H:%M:%S') if 'datetime' in last_row else str(predicted_timestamp)

        # Step 5: Select only numeric columns (same as training)
        ohlc_data_numeric = ohlc_data.select_dtypes(include=['number'])
        
        # Step 6: Handle NaN values - use the last row for prediction
        ohlc_data_numeric = ohlc_data_numeric.ffill().bfill()
        last_row_numeric = ohlc_data_numeric.iloc[-1:]

        # Step 7: model predict
        prediction = self.model.predict(last_row_numeric)[0]

        # Step 8: Return PredictorOutput
        return PredictorOutput(
            prediction=float(prediction),
            product_id=product_id,
            predicted_timestamp=predicted_timestamp,
            predicted_timestamp_str=predicted_timestamp_str,
        )

    def _load_model_pickle(self):
        with open(self.model_path, 'rb') as f:
            model = pickle.load(f)
            return model

if __name__ == "__main__":
    # predictor = Predictor(model_path='./lasso_model.pkl')
    # prediction = predictor.predict()
    # logger.info(f"Prediction: {prediction}")

    predictor = Predictor.from_model_registry(model_name='btc_usd_price_predictor_lasso')
    prediction = predictor.predict()
    logger.info(f"Prediction: {prediction}")
        