from __future__ import annotations

import os
import pandas as pd
import joblib
import numpy as np
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder
from sklearn.impute import SimpleImputer
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

# Features available from ml_features_attendance
NUM_COLS = [
    "temperature_f",
    "precipitation_mm",
    "wind_kmh",
    "home_win_pct_10",
    "away_win_pct_10",
    "month",
]

CAT_COLS = [
    "day_of_week",
    "weekend_flag",
    "home_team_id",
    "away_team_id",
    "venue_id",
]

TARGET = "attendance"


def build_pipeline():
    num_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="median")),
    ])

    cat_pipe = Pipeline(steps=[
        ("imputer", SimpleImputer(strategy="most_frequent")),
        ("ohe", OneHotEncoder(handle_unknown="ignore")),
    ])

    pre = ColumnTransformer(
        transformers=[
            ("num", num_pipe, NUM_COLS),
            ("cat", cat_pipe, CAT_COLS),
        ],
        remainder="drop",
    )

    model = RandomForestRegressor(
        n_estimators=200,
        max_depth=None,
        random_state=0,
        n_jobs=-1,
    )

    return Pipeline(steps=[
        ("pre", pre),
        ("model", model),
    ])


def metrics(y_true, y_pred):
    mae = mean_absolute_error(y_true, y_pred)
    mse = mean_squared_error(y_true, y_pred)
    rmse = float(np.sqrt(mse))
    r2 = r2_score(y_true, y_pred)
    return mae, rmse, r2


def main():
    train = pd.read_parquet("data/train.parquet")
    valid = pd.read_parquet("data/valid.parquet")

    X_train = train[NUM_COLS + CAT_COLS]
    y_train = train[TARGET].astype(float)

    X_valid = valid[NUM_COLS + CAT_COLS]
    y_valid = valid[TARGET].astype(float)

    pipe = build_pipeline()
    pipe.fit(X_train, y_train)

    pred_valid = pipe.predict(X_valid)
    mae, rmse, r2 = metrics(y_valid, pred_valid)

    print("Validation MAE:", round(mae, 2))
    print("Validation RMSE:", round(rmse, 2))
    print("Validation R2:", round(r2, 4))

    os.makedirs("models", exist_ok=True)
    joblib.dump(pipe, "models/random_forest.joblib")
    print("Saved models/random_forest.joblib")


if __name__ == "__main__":
    main()