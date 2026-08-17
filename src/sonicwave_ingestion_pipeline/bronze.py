from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def read_source_permissive(spark: SparkSession, path: str, schema: StructType) -> DataFrame:
    # An explicit schema so no column is dropped silently because of all NULL records
    # and Bronze columns are consistent between snapshots
    return spark.read.schema(schema).json(path)


def write_bronze(df: DataFrame, out_path: str, snapshot_date: str) -> None:
    bronze_df = (
        df.withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.input_file_name())
        .withColumn("snapshot_date", F.lit(snapshot_date))
    )

    # Without dynamic mode, overwrite wipes the WHOLE out_path, not just this
    # snapshot's partition (see test_write_bronze_partition_overwrite_is_dynamic)
    df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    bronze_df.write.mode("overwrite").partitionBy("snapshot_date").parquet(out_path)
