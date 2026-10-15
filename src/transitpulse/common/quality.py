"""Data-quality rules shared by expectations and quarantine tables.

A rule set maps a rule name to a SQL condition that must be true for a good row. The same dict feeds the
pipeline's expectations (so pass rates show up in the event log) and the quarantine split below.
"""

from __future__ import annotations

from collections.abc import Mapping

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

Rules = Mapping[str, str]

FAILED_RULES = "_failed_rules"


def failed_rules(rules: Rules) -> Column:
    """Array of the names of rules a row breaks; a NULL condition counts as broken."""
    checks = [F.when(~F.coalesce(F.expr(cond), F.lit(False)), F.lit(name)) for name, cond in rules.items()]
    return F.filter(F.array(*checks), lambda name: name.isNotNull())


def with_failed_rules(df: DataFrame, rules: Rules) -> DataFrame:
    return df.withColumn(FAILED_RULES, failed_rules(rules))


def passed(df: DataFrame) -> DataFrame:
    return df.where(F.size(F.col(FAILED_RULES)) == 0).drop(FAILED_RULES)


def quarantined(df: DataFrame) -> DataFrame:
    return df.where(F.size(F.col(FAILED_RULES)) > 0).withColumn("_quarantined_at", F.current_timestamp())
