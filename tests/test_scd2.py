from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from sonicwave_ingestion_pipeline.scd2 import apply_scd2

SCHEMA = StructType(
    [
        StructField("user_id", StringType()),
        StructField("plan_tier", StringType()),
        StructField("country", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType()),
        StructField("valid_from", TimestampType()),
        StructField("valid_to", TimestampType()),
        StructField("is_current", BooleanType()),
    ]
)


def _with_effective_at(df):
    return df.withColumn("effective_at", F.coalesce("updated_at", "created_at"))


def _apply(spark: SparkSession, existing, rows):
    new_snapshot = _with_effective_at(
        spark.createDataFrame(
            rows,
            schema="user_id string, plan_tier string, country string, "
            "created_at string, updated_at string",
        )
        .withColumn("created_at", F.col("created_at").cast("timestamp"))
        .withColumn("updated_at", F.col("updated_at").cast("timestamp"))
    )
    return apply_scd2(
        existing,
        new_snapshot,
        key_col="user_id",
        tracked_cols=["plan_tier", "country"],
        valid_from_col="effective_at",
    )


def test_apply_scd2_first_run_opens_one_open_version(spark: SparkSession) -> None:
    empty_silver = spark.createDataFrame([], SCHEMA)

    result = _apply(spark, empty_silver, [("3", "free", "PL", "2026-01-01T00:00:00", None)])

    assert result.count() == 1
    row = result.first()
    assert row["is_current"] is True
    assert row["valid_to"] is None


def test_apply_scd2_closes_old_version_when_tracked_attribute_changes(
    spark: SparkSession,
) -> None:
    day1 = _apply(
        spark,
        spark.createDataFrame([], SCHEMA),
        [("3", "free", "PL", "2026-01-01T00:00:00", None)],
    )

    day2 = _apply(
        spark, day1, [("3", "premium", "PL", "2026-01-01T00:00:00", "2026-01-02T09:00:00")]
    )

    assert day2.count() == 2
    assert day2.filter("is_current").count() == 1

    old_version = day2.filter("plan_tier = 'free'").first()
    new_version = day2.filter("plan_tier = 'premium'").first()

    assert old_version["is_current"] is False
    assert new_version["is_current"] is True
    assert new_version["valid_to"] is None
    assert old_version["valid_to"] == new_version["valid_from"]


def test_apply_scd2_reapplying_same_snapshot_opens_no_new_version(
    spark: SparkSession,
) -> None:
    day1 = _apply(
        spark,
        spark.createDataFrame([], SCHEMA),
        [("3", "free", "PL", "2026-01-01T00:00:00", None)],
    )
    day2_rows = [("3", "premium", "PL", "2026-01-01T00:00:00", "2026-01-02T09:00:00")]
    day2 = _apply(spark, day1, day2_rows)

    # Re-apply the exact same snapshot again
    reapplied = _apply(spark, day2, day2_rows)

    assert reapplied.count() == day2.count() == 2
    assert reapplied.filter("is_current").count() == 1
