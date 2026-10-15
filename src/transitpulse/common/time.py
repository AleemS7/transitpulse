"""Time handling. Timestamps are stored in UTC; Atlanta time appears only as hour_local and service_date.

MARTA's rail feed sends Atlanta wall-clock text. Parsing it together with the zone name gives the correct
instant regardless of the Spark session time zone, and handles daylight saving changes.
"""

from __future__ import annotations

from pyspark.sql import Column
from pyspark.sql import functions as F

ATLANTA_TZ = "America/New_York"

# A service day runs from 03:00 to 03:00 local, so late-night trips count toward the day they started.
SERVICE_DAY_START_HOURS = 3


def local_text_to_utc(text: Column, pattern: str) -> Column:
    """Atlanta wall-clock text -> UTC TIMESTAMP, or NULL if the text doesn't match the pattern."""
    return F.try_to_timestamp(F.concat(text, F.lit(f" {ATLANTA_TZ}")), F.lit(f"{pattern} VV"))


def local_text_to_local(text: Column, pattern: str) -> Column:
    """Atlanta wall-clock text -> TIMESTAMP_NTZ holding the local wall-clock value, or NULL."""
    return F.try_to_timestamp(text, F.lit(pattern)).cast("timestamp_ntz")


def local_to_utc(local: Column) -> Column:
    """TIMESTAMP_NTZ holding Atlanta wall-clock time -> UTC TIMESTAMP."""
    return local_text_to_utc(F.date_format(local, "yyyy-MM-dd HH:mm:ss"), "yyyy-MM-dd HH:mm:ss")


def service_date(local: Column) -> Column:
    return F.to_date(local - F.expr(f"INTERVAL {SERVICE_DAY_START_HOURS} HOURS"))
