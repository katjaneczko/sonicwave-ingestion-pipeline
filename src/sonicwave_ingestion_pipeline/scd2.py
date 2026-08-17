from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def apply_scd2(
    existing_silver: DataFrame,
    new_snapshot: DataFrame,
    key_col: str,
    tracked_cols: list[str],
    valid_from_col: str,
) -> DataFrame:
    """Recompute a full SCD2 dimension from its current state + one day's ingestion

    existing_silver: the full Silver dimension before this run
        (empty on the very first run). Historical (closed) rows are never
        touched -> only the currently-open version per key can change
    new_snapshot: today's deduplicated, validated one-row-per-key snapshot
    tracked_cols: attributes that open a new version when they change
    valid_from_col: column on new_snapshot to use as the new version's
    valid_from when a version is opened (e.g. coalesce(updated_at, created_at))
    """
    current = existing_silver.filter(F.col("is_current"))
    historical = existing_silver.filter(~F.col("is_current"))

    joined = new_snapshot.alias("new").join(
        current.alias("cur"),
        on=F.col(f"new.{key_col}") == F.col(f"cur.{key_col}"),
        how="left",
    )

    # boolean Column, True when either:
    #   - cur.<key_col> is null (no existing current row), OR
    #   - any column in tracked_cols differs between "new" and "cur"
    changed = F.col(f"cur.{key_col}").isNull()
    for col in tracked_cols:
        changed = changed | (F.col(f"new.{col}") != F.col(f"cur.{col}"))

    new_versions = (
        joined.filter(changed)
        .select("new.*")
        .withColumn("valid_from", F.col(valid_from_col))
        .withColumn("valid_to", F.lit(None).cast("timestamp"))
        .withColumn("is_current", F.lit(True))
        .drop(valid_from_col)
    )

    closed_versions = (
        joined.filter(changed & F.col(f"cur.{key_col}").isNotNull())
        .select("cur.*")
        .withColumn("is_current", F.lit(False))
    )

    unchanged_versions = joined.filter(~changed).select("cur.*")

    result = (
        historical.unionByName(unchanged_versions)
        .unionByName(closed_versions)
        .unionByName(new_versions)
    )

    # recompute valid_from/valid_to/is_current
    w = Window.partitionBy(key_col).orderBy(F.col("valid_from").asc())
    return result.withColumn("valid_to", F.lead("valid_from").over(w)).withColumn(
        "is_current", F.col("valid_to").isNull()
    )
