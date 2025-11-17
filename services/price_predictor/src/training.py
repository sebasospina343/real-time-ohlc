from tools2.ohlc_data_reader import OhlcDataReader
from config import config
import pandas as pd
from loguru import logger
from typing import Tuple
from baseline_model import BaselineModel
from model_factory import fit_lasso_regressor, fit_xgboost_regressor
from feature_engineering import add_features
import comet_ml
import matplotlib.pyplot as plt

def evaluate_model(
    predictions: pd.Series,
    actuals: pd.Series,
    description: str,
) -> float:
    logger.info(f"{description}")
    from sklearn.metrics import mean_absolute_error
    mae = mean_absolute_error(actuals, predictions)
    logger.info(f"Mean Absolute Error: {mae:.4f}")
    return mae

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
    prediction_window_sec:int,
) -> pd.DataFrame:

    assert prediction_window_sec % ohlc_window_sec == 0, "Prediction window must be a multiple of the OHLC window"

    n_candles_into_future = prediction_window_sec // ohlc_window_sec

    data['close_pct_change'] = data['close'].pct_change(n_candles_into_future)
    data['target_metric'] = data['close_pct_change'].shift(-n_candles_into_future)
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

    prediction_window_sec:int,
    last_n_days_to_fetch_from_store:int,
    last_n_days_to_test_model:int,
):
    # Initialize Comet ML
    comet_ml.login(api_key=config.comet_api_key)
    experiment = comet_ml.Experiment(project_name=config.comet_project_name)

    experiment.log_parameters({
        "feature_view_name": feature_view_name, 
        "feature_view_version": feature_view_version, 
        "feature_group_name": feature_group_name,
        "feature_group_version": feature_group_version,
        "last_n_minutes": last_n_minutes,
        "ohlc_window_sec": ohlc_window_sec,
        "prediction_window_sec": prediction_window_sec, 
        "last_n_days_to_fetch_from_store": last_n_days_to_fetch_from_store, 
        "last_n_days_to_test_model": last_n_days_to_test_model
    })

    # Step 1: fetch data from feature store
    ohlc_data_reader = OhlcDataReader(
        ohlc_window_sec=ohlc_window_sec,
        feature_view_name=feature_view_name,
        feature_view_version=feature_view_version,
        feature_group_name=feature_group_name,
        feature_group_version=feature_group_version,
        last_n_minutes=last_n_minutes,
    )

    ohlc_data = ohlc_data_reader.read_from_offline_store(last_n_days_to_fetch_from_store=last_n_days_to_fetch_from_store)
    ohlc_data['datetime'] = pd.to_datetime(ohlc_data['timestamp'], unit='ms')

    # Log the dataset hash
    experiment.log_dataset_hash(ohlc_data)

    # Step 2: split data into train and test
    logger.info(f"Splitting data into train and test")
    ohlc_train, ohlc_test = split_data_into_train_and_test(data=ohlc_data, last_n_days_to_test_model=last_n_days_to_test_model)
    experiment.log_metric("n_rows_train", ohlc_train.shape[0])
    experiment.log_metric("n_rows_test", ohlc_test.shape[0])

    # Step 3: missing candles populated
    n_missing_rows_train = ohlc_train['close'].isna().sum()
    n_missing_rows_test = ohlc_test['close'].isna().sum()
    experiment.log_metric("n_missing_rows_train", n_missing_rows_train)
    experiment.log_metric("n_missing_rows_test", n_missing_rows_test)
    logger.info(f"Interpolating missing candles for train data")
    ohlc_train = interpolate_missing_candles(ohlc_train)
    logger.info(f"Interpolating missing candles for test data")
    ohlc_test = interpolate_missing_candles(ohlc_test)


    # Step 4: create the target metric
    ohlc_train = create_target_metric(
        data=ohlc_train,
        ohlc_window_sec=ohlc_window_sec,
        prediction_window_sec=prediction_window_sec,
    )
    ohlc_test = create_target_metric(
        data=ohlc_test,
        ohlc_window_sec=ohlc_window_sec,
        prediction_window_sec=prediction_window_sec,
    )

    # create a histogram of the continuos feature ohlc_train['target_metric']
    plt.figure(figsize=(10, 5))
    plt.hist(ohlc_train['target_metric'], bins=100)
    plt.savefig('target_metric_histogram_train.png')
    experiment.log_figure(figure=plt.gcf(), figure_name='target_metric_histogram_train.png')
    plt.close()

    # Plot distribution of the target metric
    logger.info(f"Distribution of the target metric for train data")
    logger.debug(ohlc_train['target_metric'].value_counts())
    logger.info(f"Distribution of the target metric for test data")
    logger.debug(ohlc_test['target_metric'].value_counts())

    X_train = ohlc_train.drop(columns=['target_metric'])
    y_train = ohlc_train['target_metric']
    X_test = ohlc_test.drop(columns=['target_metric'])
    y_test = ohlc_test['target_metric']

    # Step 5: train the baseline model
    model = BaselineModel(
        n_candles_into_future=prediction_window_sec // ohlc_window_sec,
    )

    y_test_predictions = model.predict(X_test)
    baseline_test_mae = evaluate_model(
        predictions=y_test_predictions,
        actuals=y_test,
        description="Baseline model on Test data",
    )
    y_train_predictions = model.predict(X_train)
    baseline_train_mae = evaluate_model(
        predictions=y_train_predictions,
        actuals=y_train,
        description="Baseline model on Train data",
    )

    experiment.log_metric("baseline_test_mae", baseline_test_mae)
    experiment.log_metric("baseline_train_mae", baseline_train_mae)

    # add indicators
    X_train = add_features(X_train, timeperiod=14, n_candles_into_future=prediction_window_sec // ohlc_window_sec)
    X_test = add_features(X_test, timeperiod=14, n_candles_into_future=prediction_window_sec // ohlc_window_sec)

    # Select only numeric columns for model training (exclude product_id, timestamp, datetime)
    X_train = X_train.select_dtypes(include=['number'])
    X_test = X_test.select_dtypes(include=['number'])

    # Handle NaN values - drop rows with NaN and align y accordingly
    train_mask = ~X_train.isna().any(axis=1)
    test_mask = ~X_test.isna().any(axis=1)
    X_train = X_train[train_mask]
    y_train = y_train[train_mask]
    X_test = X_test[test_mask]
    y_test = y_test[test_mask]

    experiment.log_metric("x_train_shape", X_train.shape)
    experiment.log_metric("x_test_shape", X_test.shape)
    experiment.log_metric("y_train_shape", y_train.shape)
    experiment.log_metric("y_test_shape", y_test.shape)

    # Step 6: build a more complex model
    model = fit_lasso_regressor(X_train, y_train)

    test_mae = evaluate_model(
        predictions=model.predict(X_test),
        actuals=y_test,
        description="Lasso Regressor on Test data",
    )
    train_mae = evaluate_model(
        predictions=model.predict(X_train),
        actuals=y_train,
        description="Lasso Regressor on Train data",
    )
    experiment.log_metric("lasso_test_mae", test_mae)
    experiment.log_metric("lasso_train_mae", train_mae)

    # Step 7: build a more complex model
    # model = fit_xgboost_regressor(X_train, y_train)
    # test_mae = evaluate_model(
    #     predictions=model.predict(X_test),
    #     actuals=y_test,
    #     description="XGBoost Regressor on Test data",
    # )
    # train_mae = evaluate_model(
    #     predictions=model.predict(X_train),
    #     actuals=y_train,
    #     description="XGBoost Regressor on Train data",
    # )
    # experiment.log_metric("xgboost_test_mae", test_mae)
    # experiment.log_metric("xgboost_train_mae", train_mae)

    # Step 7: save the model as a pickle file
    import pickle
    with open('./lasso_model.pkl', 'wb') as f:
        pickle.dump(model, f)

    experiment.log_model(name='BTC_USD_PRICE_PREDICTOR_LASSO', file_or_folder='./lasso_model.pkl')

    # if baseline_test_mae < baseline_train_mae:
    logger.info(f"Pushing model to the registry")
    experiment.register_model(model_name='BTC_USD_PRICE_PREDICTOR_LASSO')
    experiment.end()

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
        ohlc_window_sec=config.ohlc_window_sec,
        feature_group_name=config.feature_group_name,
        feature_group_version=config.feature_group_version,
        last_n_minutes=config.last_n_minutes,
        prediction_window_sec=60 * 5,
        last_n_days_to_fetch_from_store=90,
        last_n_days_to_test_model=30,
    )