"""The entry-point for the plays Bronze -> Silver pipeline.

uv run python scripts/run_plays.py --source ./data/source/plays --snapshot-date 2026-03-01
"""

from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import SparkSession

from sonicwave_ingestion_pipeline.plays import run_plays


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the plays Bronze->Silver pipeline for one snapshot."
    )
    parser.add_argument("--source", required=True, help="Path to the plays source landing folder.")
    parser.add_argument(
        "--snapshot-date", required=True, help="Snapshot date to process, e.g. 2026-03-01."
    )
    args = parser.parse_args()

    # needed on Windows
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    spark = SparkSession.builder.appName("run-plays").getOrCreate()
    run_plays(
        spark,
        source_path=args.source,
        snapshot_date=args.snapshot_date,
        bronze_path="data/bronze/plays",
        silver_path="data/silver/plays",
        reject_path="data/reject/plays",
    )


if __name__ == "__main__":
    main()
