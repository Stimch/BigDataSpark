#!/usr/bin/env python3
"""Загрузка 10 CSV-файлов mock_data в PostgreSQL (staging.mock_data)."""

from __future__ import annotations

import glob
import os
import sys
import time

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text

DATA_DIR = os.environ.get("MOCK_DATA_DIR", "/data/source")
PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
PG_DB = os.environ.get("POSTGRES_DB", "bigdata")
PG_USER = os.environ.get("POSTGRES_USER", "lab")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "lab")


def wait_for_postgres(max_attempts: int = 60) -> None:
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASSWORD,
            )
            conn.close()
            print("PostgreSQL is ready")
            return
        except psycopg2.OperationalError as exc:
            print(f"Waiting for PostgreSQL ({attempt}/{max_attempts}): {exc}")
            time.sleep(2)
    raise RuntimeError("PostgreSQL is not available")


def collect_csv_files(directory: str) -> list[str]:
    patterns = [
        os.path.join(directory, "MOCK_DATA*.csv"),
        os.path.join(directory, "mock_data*.csv"),
    ]
    files: list[str] = []
    for pattern in patterns:
        files.extend(glob.glob(pattern))
    return sorted(set(files))


def main() -> None:
    wait_for_postgres()

    csv_files = collect_csv_files(DATA_DIR)
    if not csv_files:
        print(f"No CSV files found in {DATA_DIR}", file=sys.stderr)
        sys.exit(1)

    frames = [pd.read_csv(path) for path in csv_files]
    df = pd.concat(frames, ignore_index=True)
    print(f"Loaded {len(df)} rows from {len(csv_files)} files")

    engine = create_engine(
        f"postgresql+psycopg2://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"
    )
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE staging.mock_data"))
    df.to_sql(
        "mock_data",
        engine,
        schema="staging",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    print(f"Inserted {len(df)} rows into staging.mock_data")


if __name__ == "__main__":
    main()
