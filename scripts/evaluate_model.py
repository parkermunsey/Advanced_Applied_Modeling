from __future__ import annotations

import os
import numpy as np
import pandas as pd
import joblib
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

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

MODELS = {
    "random_forest": "models/random_forest.joblib",
}


def compute_metrics(y_true, y_pred):
    mae = mean_absolute_error(y_true, y_pred)
    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))
    r2 = r2_score(y_true, y_pred)
    return mae, rmse, r2


def build_output_df(test_df: pd.DataFrame, pred_test, q95: float) -> pd.DataFrame:
    keep_cols = [
        "game_id",
        "game_date",
        "season",
        "attendance",
        "home_team_id",
        "away_team_id",
        "venue_id",
        "day_of_week",
        "weekend_flag",
        "month",
    ]
    present = [c for c in keep_cols if c in test_df.columns]

    out = test_df[present].copy()
    out["pred_attendance"] = pred_test
    out["lo95"] = (out["pred_attendance"] - q95).clip(lower=0)
    out["hi95"] = out["pred_attendance"] + q95
    return out


def main():
    os.makedirs("outputs", exist_ok=True)

    # -------------------------
    # Load data
    # -------------------------
    valid = pd.read_parquet("data/valid.parquet")
    test = pd.read_parquet("data/test.parquet")

    X_valid = valid[NUM_COLS + CAT_COLS]
    y_valid = valid[TARGET].astype(float)

    X_test = test[NUM_COLS + CAT_COLS]
    y_test = test[TARGET].astype(float)

    comparison_rows = []

    for model_name, model_path in MODELS.items():
        if not os.path.exists(model_path):
            print(f"Skipping {model_name}: could not find {model_path}")
            continue

        print("\n" + "=" * 50)
        print(f"MODEL: {model_name}")
        print("=" * 50)

        pipe = joblib.load(model_path)

        # -------------------------
        # 1) Validation predictions
        # -------------------------
        pred_valid = pipe.predict(X_valid)

        val_mae, val_rmse, val_r2 = compute_metrics(y_valid, pred_valid)

        # Conformal interval half-width from validation residuals
        abs_err_valid = np.abs(y_valid.values - pred_valid)
        q95 = float(np.quantile(abs_err_valid, 0.95))

        print("Validation MAE:", round(val_mae, 2))
        print("Validation RMSE:", round(val_rmse, 2))
        print("Validation R2:", round(val_r2, 4))
        print("Conformal q95 half-width:", round(q95, 2))

        valid_out = valid[["game_id", "game_date", "season", "attendance"]].copy()
        valid_out["pred_attendance"] = pred_valid
        valid_out["residual"] = y_valid.values - pred_valid
        valid_out.to_parquet(f"outputs/valid_residuals_{model_name}.parquet", index=False)
        print(f"Wrote outputs/valid_residuals_{model_name}.parquet")

        # -------------------------
        # 2) Test predictions
        # -------------------------
        pred_test = pipe.predict(X_test)

        test_mae, test_rmse, test_r2 = compute_metrics(y_test, pred_test)

        print("Test MAE:", round(test_mae, 2))
        print("Test RMSE:", round(test_rmse, 2))
        print("Test R2:", round(test_r2, 4))

        # Coverage check on test
        lo95 = np.clip(pred_test - q95, a_min=0, a_max=None)
        hi95 = pred_test + q95
        coverage = float(np.mean((y_test.values >= lo95) & (y_test.values <= hi95)))

        print("Test empirical coverage:", round(coverage, 4))

        out = build_output_df(test, pred_test, q95)

        # Backwards-compatible style output for each model
        out[["game_id", "game_date", "season", "attendance", "pred_attendance"]].to_parquet(
            f"outputs/test_predictions_{model_name}.parquet",
            index=False,
        )
        print(f"Wrote outputs/test_predictions_{model_name}.parquet")

        out.to_parquet(f"outputs/test_predictions_with_intervals_{model_name}.parquet", index=False)
        print(f"Wrote outputs/test_predictions_with_intervals_{model_name}.parquet")

        comparison_rows.append(
            {
                "model_name": model_name,
                "valid_mae": val_mae,
                "valid_rmse": val_rmse,
                "valid_r2": val_r2,
                "q95": q95,
                "test_mae": test_mae,
                "test_rmse": test_rmse,
                "test_r2": test_r2,
                "test_coverage": coverage,
            }
        )

    if not comparison_rows:
        raise FileNotFoundError("No model files were found. Train ridge and random forest first.")

    comparison = pd.DataFrame(comparison_rows).sort_values("test_rmse")
    comparison.to_csv("outputs/model_comparison.csv", index=False)

    print("\n" + "=" * 50)
    print("MODEL COMPARISON SUMMARY")
    print("=" * 50)
    print(comparison.round(4).to_string(index=False))
    print("\nWrote outputs/model_comparison.csv")


if __name__ == "__main__":
    main()