from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def deduplicate(df: DataFrame, key_cols: list[str], order_col: str) -> DataFrame:
    # Keep the highest order_col per key (e.g. latest created_at) and drop the rest.
    w = Window.partitionBy(*key_cols).orderBy(F.col(order_col).desc())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")


def split_valid_invalid(df: DataFrame, is_valid: Column) -> tuple[DataFrame, DataFrame]:
    safe_valid = F.coalesce(is_valid, F.lit(False))
    valid = df.filter(safe_valid)
    invalid = df.filter(~safe_valid)
    return valid, invalid


def write_silver(df: DataFrame, out_path: str) -> None:
    df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    df.write.mode("overwrite").partitionBy("snapshot_date").parquet(out_path)
