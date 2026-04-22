from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import pandas as pd
import psycopg2
from dotenv import load_dotenv


MODEL_NAME = "random_forest_v1_conformal"
INTERVAL_METHOD = "conformal_95_valid"
PRED_PATH = "outputs/test_predictions_with_intervals_random_forest.parquet"


def get_connection():
    load_dotenv()

    password = os.getenv("PGPASSWORD")
    if not password:
        print("ERROR: PGPASSWORD is missing from .env", file=sys.stderr)
        sys.exit(1)

    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        database=os.getenv("PGDATABASE", "sports"),
        user=os.getenv("PGUSER", "postgres"),
        password=password,
        port=int(os.getenv("PGPORT", "5432")),
    )


def ensure_table(cur):
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS fact_attendance_forecast (
            game_id BIGINT NOT NULL,
            model_name TEXT NOT NULL,
            run_ts_utc TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            interval_method TEXT NOT NULL,
            pred_attendance NUMERIC,
            lo95 NUMERIC,
            hi95 NUMERIC,
            PRIMARY KEY (game_id, model_name)
        );
        """
    )


def safe_float(value):
    try:
        return float(value) if value is not None else None
    except Exception:
        return None


def main():
    if not os.path.exists(PRED_PATH):
        raise FileNotFoundError(f"Could not find {PRED_PATH}. Run evaluate_model.py first.")

    df = pd.read_parquet(PRED_PATH)

    required_cols = ["game_id", "pred_attendance", "lo95", "hi95"]
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise ValueError(f"Predictions file is missing columns: {missing_cols}")

    conn = get_connection()
    conn.autocommit = False
    run_ts = datetime.now(timezone.utc)

    try:
        with conn.cursor() as cur:
            ensure_table(cur)

            rows_written = 0

            for row in df.itertuples(index=False):
                game_id = int(getattr(row, "game_id"))
                pred_attendance = round(safe_float(getattr(row, "pred_attendance")) or 0)
                lo95 = round(safe_float(getattr(row, "lo95")) or 0)
                hi95 = round(safe_float(getattr(row, "hi95")) or 0)

                cur.execute(
                    """
                    INSERT INTO fact_attendance_forecast (
                        game_id,
                        model_name,
                        run_ts_utc,
                        interval_method,
                        pred_attendance,
                        lo95,
                        hi95
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (game_id, model_name)
                    DO UPDATE SET
                        run_ts_utc = EXCLUDED.run_ts_utc,
                        interval_method = EXCLUDED.interval_method,
                        pred_attendance = EXCLUDED.pred_attendance,
                        lo95 = EXCLUDED.lo95,
                        hi95 = EXCLUDED.hi95;
                    """,
                    (
                        game_id,
                        MODEL_NAME,
                        run_ts,
                        INTERVAL_METHOD,
                        pred_attendance,
                        lo95,
                        hi95,
                    ),
                )

                rows_written += 1

        conn.commit()
        print(f"Wrote {rows_written} forecasts to fact_attendance_forecast")
        print(f"Model: {MODEL_NAME}")
        print(f"Prediction file: {PRED_PATH}")

    except Exception:
        conn.rollback()
        print("Write failed. Rolled back transaction.", file=sys.stderr)
        raise

    finally:
        conn.close()


if __name__ == "__main__":
    main()