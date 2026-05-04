from __future__ import annotations

import os
import sys
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine

load_dotenv()

def engine():
    pw = os.getenv("PGPASSWORD")
    if not pw:
        print("ERROR: PGPASSWORD missing in env", file=sys.stderr)
        sys.exit(1)

    url = (
        f"postgresql+psycopg2://{os.getenv('PGUSER','postgres')}:{pw}"
        f"@{os.getenv('PGHOST','localhost')}:{os.getenv('PGPORT','5432')}/{os.getenv('PGDATABASE','sports')}"
    )
    return create_engine(url, future=True)

def main():
    eng = engine()

    query = """
        SELECT *
        FROM ml_features_attendance
        WHERE attendance IS NOT NULL
        ORDER BY game_date, game_id;
    """

    df = pd.read_sql(query, eng)
    n = len(df)

    print("Total rows pulled:", n)
    if n < 30:
        print("ERROR: Need at least 30 rows to split.")
        sys.exit(1)

    # 70/15/15 split by time order
    train_end = int(n * 0.70)
    valid_end = int(n * 0.85)

    # Guards to guarantee non-empty splits
    if train_end < 1:
        train_end = 1
    if valid_end <= train_end:
        valid_end = train_end + 1
    if valid_end >= n:
        valid_end = n - 1

    train = df.iloc[:train_end].copy()
    valid = df.iloc[train_end:valid_end].copy()
    test = df.iloc[valid_end:].copy()

    os.makedirs("data", exist_ok=True)
    train.to_parquet("data/train.parquet", index=False)
    valid.to_parquet("data/valid.parquet", index=False)
    test.to_parquet("data/test.parquet", index=False)

    print("Train rows:", len(train))
    print("Valid rows:", len(valid))
    print("Test rows:", len(test))
    print("Export complete.")

if __name__ == "__main__":
    main()
