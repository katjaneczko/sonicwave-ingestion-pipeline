from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from sonicwave_ingestion_pipeline.silver import deduplicate, split_valid_invalid, write_silver


def test_deduplicate_keeps_latest_by_order_col(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            ("1000", "2026-03-01T00:00:00", "120000"),
            ("1000", "2026-03-01T05:00:00", "200000"),  # newer created_at in result
            ("1001", "2026-03-01T01:00:00", "90000"),
        ],
        schema="play_id string, created_at string, ms_played string",
    )

    result = deduplicate(df, key_cols=["play_id"], order_col="created_at")

    assert result.count() == 2
    assert "rn" not in result.columns

    kept = {row["play_id"]: row["ms_played"] for row in result.collect()}
    assert kept == {"1000": "200000", "1001": "90000"}


def test_split_valid_invalid(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [
            ("1", "10", "120000"),  # valid
            ("2", None, "90000"),  # invalid: null user_id
            ("3", "11", "-500"),  # invalid: negative ms_played
            (
                "4",
                "12",
                None,
            ),  # is_valid itself becomes NULL here -> must land in invalid, not rejected
        ],
        schema="play_id string, user_id string, ms_played string",
    )
    is_valid = F.col("user_id").isNotNull() & (F.col("ms_played").cast("long") >= 0)
    valid, invalid = split_valid_invalid(df, is_valid)
    assert invalid.count() == 3
    assert valid.count() == 1


def test_write_silver(spark: SparkSession, tmp_path: Path) -> None:
    out_path = tmp_path / "plays"

    day1 = spark.createDataFrame(
        [("1", "2026-03-01")], schema="play_id string, snapshot_date string"
    )
    write_silver(day1, str(out_path))

    result = spark.read.parquet(str(out_path))
    assert result.first()["snapshot_date"] == "2026-03-01"


def test_write_silver_partition_overwrite_is_dynamic(spark: SparkSession, tmp_path: Path) -> None:
    out_path = tmp_path / "plays"

    df1 = spark.createDataFrame(
        [("1", "2026-03-01")], schema="play_id string, snapshot_date string"
    )
    write_silver(df1, str(out_path))

    df2 = spark.createDataFrame(
        [("2", "2026-03-02")], schema="play_id string, snapshot_date string"
    )
    write_silver(df2, str(out_path))

    result = spark.read.parquet(str(out_path))
    assert result.count() == 2
    assert {row["snapshot_date"] for row in result.collect()} == {"2026-03-01", "2026-03-02"}


def test_rerunning_same_snapshot_does_not_duplicate_rows(
    spark: SparkSession, tmp_path: Path
) -> None:
    out_path = tmp_path / "plays"

    df1 = spark.createDataFrame(
        [("1", "2026-03-01")], schema="play_id string, snapshot_date string"
    )
    write_silver(df1, str(out_path))
    # re-run for the same day
    write_silver(df1, str(out_path))

    result = spark.read.parquet(str(out_path))
    assert result.count() == 1
