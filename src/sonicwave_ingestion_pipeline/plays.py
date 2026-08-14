from __future__ import annotations

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType, TimestampType

from sonicwave_ingestion_pipeline.bronze import read_source_permissive, write_bronze
from sonicwave_ingestion_pipeline.silver import (
    cast_to_schema,
    deduplicate,
    split_valid_invalid,
    write_silver,
)

PLAYS_SCHEMA = StructType(
    [
        StructField("play_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("content_id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("played_at", TimestampType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
        StructField("ms_played", LongType(), True),
    ]
)

# Same columns as PLAYS_SCHEMA, but every field is a string (permissive)
# An explicit schema so no column is dropped silently because of
# all NULL records and Bronze columns are consistent between snapshots
PLAYS_RAW_SCHEMA = StructType(
    [StructField(field.name, StringType(), True) for field in PLAYS_SCHEMA.fields]
)


def plays_is_valid() -> Column:
    return F.col("user_id").isNotNull() & (F.col("ms_played") >= 0)


def run_plays(
    spark: SparkSession,
    source_path: str,
    snapshot_date: str,
    bronze_path: str,
    silver_path: str,
    reject_path: str,
) -> None:
    raw = read_source_permissive(spark, f"{source_path}/{snapshot_date}", PLAYS_RAW_SCHEMA)
    write_bronze(raw, bronze_path, snapshot_date)

    read_bronze = spark.read.parquet(bronze_path).filter(F.col("snapshot_date") == snapshot_date)
    typed = cast_to_schema(read_bronze, PLAYS_SCHEMA)
    valid, invalid = split_valid_invalid(typed, plays_is_valid())

    clean = deduplicate(valid, key_cols=["play_id"], order_col="created_at")
    clean = clean.withColumn("event_date", F.to_date("played_at"))
    clean = clean.withColumn("snapshot_date", F.lit(snapshot_date))
    write_silver(clean, silver_path)

    invalid = invalid.withColumn("snapshot_date", F.lit(snapshot_date))
    write_silver(invalid, reject_path)
