import pandas as pd
from typing import List

class BaselineModel:
    def __init__(
        self, 
        n_candles_into_future: int,
        discretization_tresholds: List[float],
    ):
        self.n_candles_into_future = n_candles_into_future
        self.discretization_tresholds = discretization_tresholds

    def train(self) -> None:
        pass

    def predict(self, X: pd.DataFrame) -> pd.Series:
        X_ = X.copy()
        X_['close_pct_change'] = X_['close'].pct_change(self.n_candles_into_future)
        
        def discretize_value(x: float) -> int:
            if pd.isna(x):
                return None
            if x < self.discretization_tresholds[0]:
                return 0
            elif x < self.discretization_tresholds[1]:
                return 1
            elif x >= self.discretization_tresholds[1]:
                return 2
            else:
                return None
        
        X_['target_metric'] = X_['close_pct_change'].apply(discretize_value)

        X_['target_metric'].fillna(1, inplace=True)

        return X_['target_metric']