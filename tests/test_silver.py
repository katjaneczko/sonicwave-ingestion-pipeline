from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.errors import AnalysisException
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

from sonicwave_ingestion_pipeline.silver import (
    cast_to_schema,
    deduplicate,
    split_valid_invalid,
    write_silver,
)


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


def test_cast_to_schema_casts_types(spark: SparkSession) -> None:
    df = spark.createDataFrame([("1", "120000")], schema="play_id string, ms_played string")
    target_schema = StructType(
        [
            StructField("play_id", StringType()),
            StructField("ms_played", LongType()),
        ]
    )

    result = cast_to_schema(df, target_schema)

    assert dict(result.dtypes) == {"play_id": "string", "ms_played": "bigint"}
    assert result.first()["ms_played"] == 120000


def test_cast_to_schema_drops_extra_columns(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("1", "120000", "2026-03-01")],
        schema="play_id string, ms_played string, snapshot_date string",
    )
    target_schema = StructType([StructField("play_id", StringType())])

    result = cast_to_schema(df, target_schema)

    assert result.columns == ["play_id"]


def test_cast_to_schema_raises_on_missing_column(spark: SparkSession) -> None:
    df = spark.createDataFrame([("1",)], schema="play_id string")
    target_schema = StructType([StructField("does_not_exist", StringType())])

    with pytest.raises(AnalysisException):
        cast_to_schema(df, target_schema).collect()


def test_cast_to_schema_returns_null_for_unparseable_value(spark: SparkSession) -> None:
    # Regression test: under ANSI mode (the default), a plain cast() on a
    # malformed numeric string raises and aborts the whole write. This must
    # not crash -- it should come back as NULL so validation can quarantine it.
    df = spark.createDataFrame([("1", "NaN")], schema="play_id string, ms_played string")
    target_schema = StructType(
        [
            StructField("play_id", StringType()),
            StructField("ms_played", LongType()),
        ]
    )

    result = cast_to_schema(df, target_schema)

    assert result.first()["ms_played"] is None
