"""Run the pipeline's SQL files on local Spark: table references become temp-view names."""

from __future__ import annotations

import re
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

PIPELINES = Path(__file__).parents[1] / "src" / "transitpulse" / "pipelines"

_COLUMN = re.compile(r"^ {4}(\w+) (INT|BIGINT|STRING|DATE|DOUBLE|BOOLEAN|TIMESTAMP)\b", re.MULTILINE)


def declared(header: str) -> list[tuple[str, str]]:
    """Column names and types a gold file declares in its CREATE statement (empty if it declares none)."""
    return [(name, kind.lower()) for name, kind in _COLUMN.findall(header)]


def run(spark: SparkSession, file: str) -> DataFrame:
    text = (PIPELINES / file).read_text()
    text = re.sub(r"\$\{tp_catalog\}\.\$\{tp_\w+\}\.", "", text)
    header, query = text.split("\nAS\n", 1)
    df = spark.sql(query)
    # The declared schema is the gold contract; the query must produce exactly it.
    if columns := declared(header):
        produced = [(f.name, f.dataType.simpleString()) for f in df.schema.fields]
        assert produced == columns, f"{file}: query returns {produced}, declares {columns}"
    return df
