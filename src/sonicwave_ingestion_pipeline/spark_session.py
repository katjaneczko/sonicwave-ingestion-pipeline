from pyspark.sql import SparkSession


def build_spark_session(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        # Don't let Spark guess types from partition folder names (e.g. reading
        # snapshot_date=2026-03-01 back as a DateType instead of the string that
        # was written) -- Bronze must land/round-trip data exactly as written.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()
    )
