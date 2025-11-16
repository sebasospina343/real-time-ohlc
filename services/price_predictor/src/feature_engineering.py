import talib
import pandas as pd

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