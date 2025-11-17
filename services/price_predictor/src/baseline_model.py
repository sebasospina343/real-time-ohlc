import pandas as pd

class BaselineModel:
    def __init__(
        self, 
        n_candles_into_future: int,
    ):
        self.n_candles_into_future = n_candles_into_future

    def predict(self, X: pd.DataFrame) -> pd.Series:
        
        X_ = X.copy()

        X_['target_metric'] = 0

        return X_['target_metric']