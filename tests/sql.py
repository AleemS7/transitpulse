"""Run the pipeline's SQL files on local Spark: table references become temp-view names."""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

PIPELINES = Path(__file__).parents[1] / "src" / "transitpulse" / "pipelines"


def run(spark: SparkSession, file: str) -> DataFrame:
    text = (PIPELINES / file).read_text()
    text = re.sub(r"\$\{tp_catalog\}\.\$\{tp_\w+\}\.", "", text)
    _, query = text.split("\nAS\n", 1)
    return spark.sql(query)
