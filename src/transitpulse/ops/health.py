"""Health checks run after each hourly refresh. Plain Python: inputs are already-queried rows, so every
rule is unit-tested without Spark.

The checks separate where a problem sits:
- collector_running: is the Azure collector writing heartbeats at all? (any hour)
- feed_available: is MARTA answering for this feed? (service hours)
- feed_current: is MARTA's snapshot itself fresh, or is it serving old data? (service hours)
- dq_pass_rate: did a quality rule reject more than 5% of rows in the last day?
- bus_arrivals_flowing: are observed bus arrivals still arriving in silver? (daytime)
A failed pipeline refresh never reaches these checks; the job fails on that task and emails instead.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

ATLANTA = ZoneInfo("America/New_York")

COLLECTOR_SILENCE = timedelta(minutes=15)
FEED_SILENCE = timedelta(minutes=15)
SNAPSHOT_AGE = timedelta(minutes=15)
ARRIVAL_SILENCE = timedelta(minutes=90)
MIN_PASS_RATE = 0.95
MIN_DQ_RECORDS = 100

# Local hours when MARTA runs enough service that a silent feed means a problem.
SERVICE_HOURS = range(5, 24)
DAYTIME_HOURS = range(6, 22)


@dataclass(frozen=True)
class FeedFreshness:
    feed: str
    last_attempt_at: datetime | None
    last_collected_at: datetime | None
    last_source_ts: datetime | None


@dataclass(frozen=True)
class RulePassRate:
    dataset: str
    rule: str
    passed: int
    failed: int


@dataclass(frozen=True)
class Check:
    name: str
    subject: str
    ok: bool
    observed: str
    expected: str


def _minutes(delta: timedelta) -> str:
    return f"{delta.total_seconds() / 60:.0f} min"


def _age(now: datetime, then: datetime | None) -> timedelta | None:
    return None if then is None else now - then


def collector_checks(feeds: list[FeedFreshness], now: datetime) -> list[Check]:
    in_service = now.astimezone(ATLANTA).hour in SERVICE_HOURS
    attempts = [f.last_attempt_at for f in feeds if f.last_attempt_at is not None]
    silence = _age(now, max(attempts)) if attempts else None
    checks = [
        Check(
            "collector_running",
            "collector",
            silence is not None and silence <= COLLECTOR_SILENCE,
            "no heartbeat" if silence is None else f"last heartbeat {_minutes(silence)} ago",
            f"<= {_minutes(COLLECTOR_SILENCE)}",
        )
    ]
    if not in_service:
        return checks
    for f in feeds:
        quiet = _age(now, f.last_collected_at)
        checks.append(
            Check(
                "feed_available",
                f.feed,
                quiet is not None and quiet <= FEED_SILENCE,
                "never collected" if quiet is None else f"last success {_minutes(quiet)} ago",
                f"<= {_minutes(FEED_SILENCE)}",
            )
        )
        if f.last_collected_at is not None and f.last_source_ts is not None:
            snapshot_age = f.last_collected_at - f.last_source_ts
            checks.append(
                Check(
                    "feed_current",
                    f.feed,
                    snapshot_age <= SNAPSHOT_AGE,
                    f"snapshot {_minutes(snapshot_age)} old when fetched",
                    f"<= {_minutes(SNAPSHOT_AGE)}",
                )
            )
    return checks


def dq_checks(rates: Iterable[RulePassRate]) -> list[Check]:
    checks = []
    for r in rates:
        total = r.passed + r.failed
        if total < MIN_DQ_RECORDS:
            continue
        rate = r.passed / total
        checks.append(
            Check(
                "dq_pass_rate",
                f"{r.dataset}.{r.rule}",
                rate >= MIN_PASS_RATE,
                f"{rate:.1%} of {total:,} rows passed",
                f">= {MIN_PASS_RATE:.0%}",
            )
        )
    return checks


def arrival_checks(latest_arrival: datetime | None, now: datetime) -> list[Check]:
    if now.astimezone(ATLANTA).hour not in DAYTIME_HOURS:
        return []
    silence = _age(now, latest_arrival)
    return [
        Check(
            "bus_arrivals_flowing",
            "silver.bus_stop_arrivals",
            silence is not None and silence <= ARRIVAL_SILENCE,
            "no arrivals" if silence is None else f"latest arrival {_minutes(silence)} ago",
            f"<= {_minutes(ARRIVAL_SILENCE)}",
        )
    ]


def failures(checks: Iterable[Check]) -> list[Check]:
    return [c for c in checks if not c.ok]
