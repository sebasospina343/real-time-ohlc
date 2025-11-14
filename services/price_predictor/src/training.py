from tools2.ohlc_data_reader import OhlcDataReader
from config import config
import pandas as pd
from typing import List
from loguru import logger
from typing import Tuple
from baseline_model import BaselineModel
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix
from sklearn.metrics import classification_report

def split_data_into_train_and_test(
    data: pd.DataFrame,
    last_n_days_to_test_model:int,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cutoff_date = data['datetime'].max() - pd.Timedelta(days=last_n_days_to_test_model)
    
    ohlc_train = data[data['datetime'] < cutoff_date]
    ohlc_test = data[data['datetime'] >= cutoff_date]

    return ohlc_train, ohlc_test


def create_target_metric(
    data: pd.DataFrame,
    ohlc_window_sec:int,
    discretization_tresholds:List[float],
    prediction_window_sec:int,
) -> pd.DataFrame:

    assert prediction_window_sec % ohlc_window_sec == 0, "Prediction window must be a multiple of the OHLC window"

    n_candles_into_future = prediction_window_sec // ohlc_window_sec

    data['close_pct_change'] = data['close'].pct_change(n_candles_into_future)

    def discretize_value(x:float) -> int:
        if x < discretization_tresholds[0]:
            return 0
        elif x < discretization_tresholds[1]:
            return 1
        elif x >= discretization_tresholds[1]:
            return 2
        else:
            return None
    
    data['target_metric'] = data['close_pct_change'].apply(discretize_value)
    data['target_metric'] = data['target_metric'].shift(-n_candles_into_future)
    data.drop(columns=['close_pct_change'], inplace=True)

    data.dropna(subset=['target_metric'],inplace=True)

    return data

def train(
    feature_view_name:str,
    feature_view_version:int,
    feature_group_name:str,
    feature_group_version:int,
    last_n_minutes:int,
    ohlc_window_sec:int,
    discretization_tresholds:List[float],
    prediction_window_sec:int,
    last_n_days_to_fetch_from_store:int,
    last_n_days_to_test_model:int,
):
    ohlc_data_reader = OhlcDataReader(
        ohlc_window_sec=ohlc_window_sec,
        feature_view_name=feature_view_name,
        feature_view_version=feature_view_version,
        feature_group_name=feature_group_name,
        feature_group_version=feature_group_version,
        last_n_minutes=last_n_minutes,
    )

    # fetch data from feature store
    # data = ohlc_data_reader.read_from_online_store()
    ohlc_data = ohlc_data_reader.read_from_offline_store(last_n_days_to_fetch_from_store=last_n_days_to_fetch_from_store)
    ohlc_data['datetime'] = pd.to_datetime(ohlc_data['timestamp'], unit='ms')

    logger.info(f"Splitting data into train and test")
    ohlc_train, ohlc_test = split_data_into_train_and_test(data=ohlc_data, last_n_days_to_test_model=last_n_days_to_test_model)

    # missing candles populated
    logger.info(f"Interpolating missing candles for train data")
    ohlc_train = interpolate_missing_candles(ohlc_train)
    logger.info(f"Interpolating missing candles for test data")
    ohlc_test = interpolate_missing_candles(ohlc_test)

    # create the target metric
    ohlc_train = create_target_metric(
        data=ohlc_train,
        ohlc_window_sec=ohlc_window_sec,
        discretization_tresholds=discretization_tresholds,
        prediction_window_sec=prediction_window_sec,
    )
    ohlc_test = create_target_metric(
        data=ohlc_test,
        ohlc_window_sec=ohlc_window_sec,
        discretization_tresholds=discretization_tresholds,
        prediction_window_sec=prediction_window_sec,
    )

    # Plot distribution of the target metric
    logger.info(f"Distribution of the target metric for train data")
    logger.debug(ohlc_train['target_metric'].value_counts())
    logger.info(f"Distribution of the target metric for test data")
    logger.debug(ohlc_test['target_metric'].value_counts())

    X_train = ohlc_train.drop(columns=['target_metric'])
    y_train = ohlc_train['target_metric']
    X_test = ohlc_test.drop(columns=['target_metric'])
    y_test = ohlc_test['target_metric']

    baseline_model = BaselineModel(
        n_candles_into_future=prediction_window_sec // ohlc_window_sec,
        discretization_tresholds=discretization_tresholds,
    )

    y_test_predictions = baseline_model.predict(X_test)

    # get the accuracy
    accuracy = accuracy_score(y_test, y_test_predictions)
    logger.info(f"Accuracy: {accuracy}")

    # confusion matrix
    confusion_matrix_ = confusion_matrix(y_test, y_test_predictions)
    logger.info(f"Confusion matrix: {confusion_matrix_}")

    # classification report
    classification_report_ = classification_report(y_test, y_test_predictions)
    logger.info(f"Classification report: {classification_report_}")

    return ohlc_data

def interpolate_missing_candles(data: pd.DataFrame) -> pd.DataFrame:

    data.set_index('timestamp', inplace=True)
    from_ms = int(data.index.min())
    to_ms = int(data.index.max())
    labels = range(from_ms, to_ms, 60000)
    data = data.reindex(labels)

    # forward fill
    data['close'].ffill(inplace=True)
    data['product_id'].ffill(inplace=True)
    # take the last value
    data['open'].fillna(data['close'], inplace=True)
    data['high'].fillna(data['close'], inplace=True)
    data['low'].fillna(data['close'], inplace=True)

    data.reset_index(inplace=True)

    data['datetime'] = pd.to_datetime(data['timestamp'], unit='ms')

    # save it as a csv
    # data.to_csv('data.csv', index=False)

    return data

if __name__ == "__main__":
    train(
        feature_view_name=config.feature_view_name,
        feature_view_version=config.feature_view_version,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
        last_n_minutes=config.last_n_minutes,
        ohlc_window_sec=config.ohlc_window_sec,
        discretization_tresholds=[-0.0001, 0.0001],
        prediction_window_sec=60 * 5,
        last_n_days_to_fetch_from_store=5,
        last_n_days_to_test_model=1,
    )