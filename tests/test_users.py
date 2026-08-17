from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession

from sonicwave_ingestion_pipeline.users import run_users


def test_run_users(spark: SparkSession, tmp_path: Path) -> None:
    source_root = tmp_path / "source" / "users"

    day1_dir = source_root / "2026-03-01"
    day1_dir.mkdir(parents=True)
    day1_rows = [
        # exact duplicate row: deduped to one row
        {
            "user_id": "1",
            "email": "a@x.com",
            "country": "PL",
            "plan_tier": "free",
            "created_at": "2026-01-01T00:00:00",
        },
        {
            "user_id": "1",
            "email": "a@x.com",
            "country": "PL",
            "plan_tier": "free",
            "created_at": "2026-01-01T00:00:00",
        },
        # invalid: null email
        {
            "user_id": "2",
            "email": None,
            "country": "US",
            "plan_tier": "premium",
            "created_at": "2026-01-01T00:00:00",
        },
    ]
    (day1_dir / "part-00000.json").write_text("\n".join(json.dumps(r) for r in day1_rows) + "\n")

    day2_dir = source_root / "2026-03-02"
    day2_dir.mkdir(parents=True)
    day2_rows = [
        # plan_tier change: close the old version and open a new one
        {
            "user_id": "1",
            "email": "a@x.com",
            "country": "PL",
            "plan_tier": "premium",
            "created_at": "2026-01-01T00:00:00",
            "updated_at": "2026-03-02T09:00:00",
        },
        # first valid appearance of user 2 (was rejected on day 1)
        {
            "user_id": "2",
            "email": "b@x.com",
            "country": "US",
            "plan_tier": "premium",
            "created_at": "2026-01-01T00:00:00",
        },
    ]
    (day2_dir / "part-00000.json").write_text("\n".join(json.dumps(r) for r in day2_rows) + "\n")

    bronze_path = tmp_path / "bronze" / "users"
    silver_path = tmp_path / "silver" / "users"
    reject_path = tmp_path / "reject" / "users"

    run_users(
        spark, str(source_root), "2026-03-01", str(bronze_path), str(silver_path), str(reject_path)
    )
    run_users(
        spark, str(source_root), "2026-03-02", str(bronze_path), str(silver_path), str(reject_path)
    )

    silver = spark.read.parquet(str(silver_path))
    reject = spark.read.parquet(str(reject_path))

    # user 1: two SCD2 versions (free -> premium), exactly one current
    assert silver.filter("user_id = '1'").count() == 2
    assert silver.filter("user_id = '1' AND is_current").count() == 1

    # user 2: rejected on day 1 (null email), first lands in Silver on day 2
    assert silver.filter("user_id = '2'").count() == 1

    assert silver.count() == 3
    assert reject.count() == 1
    assert reject.first()["user_id"] == "2"
