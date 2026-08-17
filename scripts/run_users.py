"""The entry-point for the users Bronze -> Silver (SCD2) pipeline.

uv run python scripts/run_users.py --source ./data/source/users --snapshot-date 2026-03-01
"""

from __future__ import annotations

import argparse
import os
import sys

from sonicwave_ingestion_pipeline.spark_session import build_spark_session
from sonicwave_ingestion_pipeline.users import run_users


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the users Bronze->Silver (SCD2) pipeline for one snapshot."
    )
    parser.add_argument("--source", required=True, help="Path to the users source landing folder.")
    parser.add_argument(
        "--snapshot-date", required=True, help="Snapshot date to process, e.g. 2026-03-01."
    )
    args = parser.parse_args()

    # needed on Windows
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = build_spark_session("run-users")
    run_users(
        spark,
        source_path=args.source,
        snapshot_date=args.snapshot_date,
        bronze_path="data/bronze/users",
        silver_path="data/silver/users",
        reject_path="data/reject/users",
    )


if __name__ == "__main__":
    main()
