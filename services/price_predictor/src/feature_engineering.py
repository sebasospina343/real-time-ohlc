import talib
import pandas as pd

def add_features(
    data: pd.DataFrame,
    timeperiod: int,
    n_candles_into_future: int,
) -> pd.DataFrame:
    X_ = data.copy()
    X_ = add_momentum_indicator(X_, timeperiod=timeperiod)
    X_ = add_volatility_indicator(X_, timeperiod=timeperiod)
    X_ = add_temportal_features(X_)
    return X_

def add_momentum_indicator(
    data: pd.DataFrame,
    timeperiod: int,
) -> pd.DataFrame:
    X_ = data.copy()
    X_['rsi'] = talib.RSI(X_['close'], timeperiod=timeperiod)
    X_['momentum'] = talib.MOM(X_['close'], timeperiod=timeperiod)
    return X_

def add_volatility_indicator(
    data: pd.DataFrame,
    timeperiod: int,
) -> pd.DataFrame:
    X_ = data.copy()
    X_['volatility'] = talib.STDDEV(X_['close'], timeperiod=timeperiod)
    return X_

def add_temportal_features(
    data: pd.DataFrame,
) -> pd.DataFrame:
    X_ = data.copy()
    X_['day_of_week'] = X_['datetime'].dt.dayofweek
    X_['hour_of_day'] = X_['datetime'].dt.hour
    X_['minute_of_hour'] = X_['datetime'].dt.minute

    return X_