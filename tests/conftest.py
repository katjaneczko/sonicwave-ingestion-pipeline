"""Shared pytest fixtures. `spark` lives here so every test can use it without
importing it explicitly (pytest auto-discovers fixtures from conftest.py).
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    # On Windows, a bare "python" on PATH can resolve to the Microsoft Store's
    # App Execution Alias stub instead of this venv's interpreter so declared expicilty
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    session = (
        SparkSession.builder.master("local[1]")
        .appName("sonicwave-ingestion-pipeline-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        # Spark guesses types from partition folder names (e.g. reading snapshot_date=2026-03-01
        # back as a DateType instead of the string)
        # Bronze must land data exactly as it was written, untyped.
        .config("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
