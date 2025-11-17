import pandas as pd

def fit_lasso_regressor(X_train: pd.DataFrame, y_train: pd.Series) -> None:
    from sklearn.linear_model import Lasso
    model = Lasso(alpha=0.1)
    model.fit(X_train, y_train)
    return model

def fit_xgboost_regressor(X_train: pd.DataFrame, y_train: pd.Series) -> None:
    from xgboost import XGBRegressor
    model = XGBRegressor()
    model.fit(X_train, y_train)
    return model