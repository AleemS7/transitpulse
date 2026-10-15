from __future__ import annotations

import glob
import os
import sys
from collections.abc import Iterator
from datetime import UTC, datetime

import pytest
from pyspark.sql import SparkSession


def _ensure_java() -> None:
    # PySpark 4 needs Java 17+. If JAVA_HOME is unset, use a Microsoft or Adoptium JDK 17 install on Windows.
    if os.environ.get("JAVA_HOME"):
        return
    for pattern in (r"C:\Program Files\Microsoft\jdk-17*", r"C:\Program Files\Eclipse Adoptium\jdk-17*"):
        found = sorted(glob.glob(pattern))
        if found:
            os.environ["JAVA_HOME"] = found[-1]
            return


@pytest.fixture(scope="session")
def spark() -> Iterator[SparkSession]:
    _ensure_java()
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    session = (
        SparkSession.builder.master("local[1]")
        .appName("transitpulse-tests")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()


def utc(value: datetime) -> datetime:
    """PySpark hands TIMESTAMP values to Python in the machine's local zone; convert back to naive UTC."""
    return value.astimezone(UTC).replace(tzinfo=None)
