from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession

from sonicwave_ingestion_pipeline.plays import run_plays


def test_run_plays(spark: SparkSession, tmp_path: Path) -> None:
    source_root = tmp_path / "source" / "plays"
    source_dir = source_root / "2026-03-01"
    source_dir.mkdir(parents=True)

    rows = [
        {
            "play_id": "1",
            "user_id": "1",
            "ms_played": "120000",
            "created_at": "2026-03-01T00:00:00",
        },
        # duplicate play_id, newer created_at -> this one should win dedup
        {
            "play_id": "1",
            "user_id": "1",
            "ms_played": "90000",
            "created_at": "2026-03-01T05:00:00",
        },
        # invalid: negative ms_played
        {
            "play_id": "2",
            "user_id": "1",
            "ms_played": "-500",
            "created_at": "2026-03-01T01:00:00",
        },
        # invalid: non-numeric ms_played
        {
            "play_id": "3",
            "user_id": "2",
            "ms_played": "NaN",
            "created_at": "2026-03-01T02:00:00",
        },
    ]
    content = "\n".join(json.dumps(row) for row in rows) + "\n"
    (source_dir / "part-00000.json").write_text(content)

    bronze_path = tmp_path / "bronze" / "plays"
    silver_path = tmp_path / "silver" / "plays"
    reject_path = tmp_path / "reject" / "plays"

    run_plays(
        spark,
        str(source_root),
        "2026-03-01",
        str(bronze_path),
        str(silver_path),
        str(reject_path),
    )

    silver = spark.read.parquet(str(silver_path))
    reject = spark.read.parquet(str(reject_path))

    assert silver.count() == 1
    assert silver.first()["ms_played"] == 90000  # deduped: newer created_at won

    assert reject.count() == 2
