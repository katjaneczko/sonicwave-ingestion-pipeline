from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from sonicwave_ingestion_pipeline.bronze import read_source_permissive, write_bronze


def test_read_source_permissive_reads_json(spark: SparkSession, tmp_path: Path) -> None:
    source_dir = tmp_path / "plays" / "2026-03-01"
    source_dir.mkdir(parents=True)
    (source_dir / "part-00000.json").write_text(
        json.dumps({"play_id": "1", "user_id": "1", "ms_played": "120000"}) + "\n"
    )
    schema = StructType(
        [
            StructField("play_id", StringType()),
            StructField("user_id", StringType()),
            StructField("ms_played", StringType()),
        ]
    )

    df = read_source_permissive(spark, str(source_dir), schema)

    assert df.count() == 1
    assert df.collect()[0]["play_id"] == "1"


def test_read_source_permissive_keeps_always_null_field(
    spark: SparkSession, tmp_path: Path
) -> None:
    source_dir = tmp_path / "plays" / "2026-03-01"
    source_dir.mkdir(parents=True)
    (source_dir / "part-00000.json").write_text(
        json.dumps({"play_id": "1", "updated_at": None}) + "\n"
    )
    schema = StructType(
        [
            StructField("play_id", StringType()),
            StructField("updated_at", StringType()),
        ]
    )

    df = read_source_permissive(spark, str(source_dir), schema)

    assert "updated_at" in df.columns
    assert df.first()["updated_at"] is None


def test_write_bronze_adds_provenance(spark: SparkSession, tmp_path: Path) -> None:
    df = spark.createDataFrame(
        [("1", "1", "120000")], schema="play_id string, user_id string, ms_played string"
    )
    out_path = tmp_path / "plays" / "2026-03-01"
    out_path.mkdir(parents=True)
    write_bronze(df, str(out_path), "2026-03-01")
    result = spark.read.parquet(str(out_path))
    assert {"ingested_at", "source_file", "snapshot_date"} <= set(result.columns)
    assert result.first()["snapshot_date"] == "2026-03-01"


def test_write_bronze_partition_overwrite_is_dynamic(spark: SparkSession, tmp_path: Path) -> None:
    out_path = tmp_path / "plays"

    df1 = spark.createDataFrame([("1",)], schema="play_id string")
    write_bronze(df1, str(out_path), "2026-03-01")

    df2 = spark.createDataFrame([("2",)], schema="play_id string")
    write_bronze(df2, str(out_path), "2026-03-02")

    result = spark.read.parquet(str(out_path))
    assert result.count() == 2
    assert {row["snapshot_date"] for row in result.collect()} == {"2026-03-01", "2026-03-02"}


def test_rerunning_same_snapshot_does_not_duplicate_rows(
    spark: SparkSession, tmp_path: Path
) -> None:
    out_path = tmp_path / "plays"

    df1 = spark.createDataFrame([("1",)], schema="play_id string")
    write_bronze(df1, str(out_path), "2026-03-01")
    # re-run for the same day
    write_bronze(df1, str(out_path), "2026-03-01")

    result = spark.read.parquet(str(out_path))
    assert result.count() == 1
