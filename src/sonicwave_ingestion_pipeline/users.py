from __future__ import annotations

from pathlib import Path

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, StringType, StructField, StructType, TimestampType

from sonicwave_ingestion_pipeline.bronze import read_source_permissive, write_bronze
from sonicwave_ingestion_pipeline.scd2 import apply_scd2
from sonicwave_ingestion_pipeline.silver import (
    cast_to_schema,
    deduplicate,
    split_valid_invalid,
    write_dimension,
    write_silver,
)

# Source columns only
USERS_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), True),
        StructField("email", StringType(), True),
        StructField("country", StringType(), True),
        StructField("plan_tier", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True),
    ]
)

USERS_SCD2_SCHEMA = StructType(
    USERS_SCHEMA.fields
    + [
        StructField("valid_from", TimestampType(), True),
        StructField("valid_to", TimestampType(), True),
        StructField("is_current", BooleanType(), True),
    ]
)

# Same columns as USERS_SCHEMA, but every field is a string - read Bronze
# JSON with an explicit schema (see bronze.read_source_permissive)
USERS_RAW_SCHEMA = StructType(
    [StructField(field.name, StringType(), True) for field in USERS_SCHEMA.fields]
)


def users_is_valid() -> Column:
    return (
        F.col("user_id").isNotNull()
        & F.col("email").isNotNull()
        & F.col("country").isNotNull()
        & F.col("plan_tier").isNotNull()
        & F.col("created_at").isNotNull()
    )


def run_users(
    spark: SparkSession,
    source_path: str,
    snapshot_date: str,
    bronze_path: str,
    silver_path: str,
    reject_path: str,
) -> None:
    raw = read_source_permissive(spark, f"{source_path}/{snapshot_date}", USERS_RAW_SCHEMA)
    write_bronze(raw, bronze_path, snapshot_date)

    read_bronze = spark.read.parquet(bronze_path).filter(F.col("snapshot_date") == snapshot_date)
    typed = cast_to_schema(read_bronze, USERS_SCHEMA)
    typed = typed.withColumn("effective_at", F.coalesce(F.col("updated_at"), F.col("created_at")))

    valid, invalid = split_valid_invalid(typed, users_is_valid())

    deduped = deduplicate(valid, key_cols=["user_id"], order_col="effective_at")

    if Path(silver_path).exists():
        existing_silver = spark.read.parquet(silver_path)
    else:
        existing_silver = spark.createDataFrame([], USERS_SCD2_SCHEMA)

    scd2_applied = apply_scd2(
        existing_silver,
        deduped,
        key_col="user_id",
        tracked_cols=["plan_tier", "country"],
        valid_from_col="effective_at",
    )

    write_dimension(scd2_applied, silver_path)
    invalid = invalid.drop("effective_at").withColumn("snapshot_date", F.lit(snapshot_date))
    write_silver(invalid, reject_path)
