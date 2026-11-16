import json
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from tests import sql
from tests.conftest import utc
from transitpulse.common.quality import FAILED_RULES, with_failed_rules
from transitpulse.ops import health, heartbeats

HEARTBEAT = {
    "run_id": "88bbcfbe79fc",
    "kind": "realtime",
    "collector_version": "0.1.0",
    "started_at": "2026-11-16T00:14:00.005475+00:00",
    "finished_at": "2026-11-16T00:14:02.056584+00:00",
    "duration_ms": 2051,
    "replayed": 0,
    "deadletter_pending": 0,
    "feeds": [
        {"feed": "bus_vehicle_positions", "status": "ok", "http_status": 200, "fetch_attempts": 1,
         "upload_attempts": 1, "bytes_raw": 12998, "bytes_stored": 4853,
         "feed_ts": "2026-11-16T00:13:49+00:00", "entity_count": 161, "unparsed": False,
         "path": "/x.pb.gz", "last_modified": None, "error": None},
        {"feed": "rail_arrivals", "status": "failed", "http_status": 500, "fetch_attempts": 3,
         "upload_attempts": 0, "bytes_raw": None, "bytes_stored": None, "feed_ts": None, "entity_count": None,
         "unparsed": False, "path": None, "last_modified": None, "error": "HTTP 500"},
    ],
}  # fmt: skip


def test_heartbeat_becomes_one_row_per_feed(spark: SparkSession) -> None:
    raw = spark.read.schema(heartbeats.RAW_SCHEMA).json(
        spark.sparkContext.parallelize([json.dumps(HEARTBEAT)])
    )
    meta = "struct('landing/_heartbeat/x.json' AS file_path)"
    bronze = heartbeats.bronze(
        raw.withColumn("_rescued_data", F.lit(None)).withColumn("_metadata", F.expr(meta))
    )
    rows = {
        r.feed: r for r in with_failed_rules(heartbeats.collector_runs(bronze), heartbeats.RULES).collect()
    }
    vp, rail = rows["bus_vehicle_positions"], rows["rail_arrivals"]
    assert utc(vp.finished_at) == datetime(2026, 11, 16, 0, 14, 2, 56584)
    assert utc(vp.feed_ts) == datetime(2026, 11, 16, 0, 13, 49) and vp.entity_count == 161
    assert rail.status == "failed" and rail.http_status == 500 and rail.error == "HTTP 500"
    assert vp[FAILED_RULES] == [] and rail[FAILED_RULES] == []


def test_unknown_status_and_unparsed_times_are_flagged(spark: SparkSession) -> None:
    df = spark.createDataFrame(
        [("r1", "bus", None, datetime(2026, 1, 1), "weird")],
        "run_id string, feed string, started_at timestamp, finished_at timestamp, status string",
    )
    assert with_failed_rules(df, heartbeats.RULES).first()[FAILED_RULES] == ["times_parsed", "known_status"]  # type: ignore[index]


# 2026-11-16 17:00 UTC is 12:00 in Atlanta (service hours); 08:00 UTC is 03:00 (overnight).
NOON = datetime(2026, 11, 16, 17, 0, tzinfo=UTC)
NIGHT = datetime(2026, 11, 16, 8, 0, tzinfo=UTC)


def feed(
    name: str, now: datetime, attempt: int, collected: int | None, source: int | None
) -> health.FeedFreshness:
    def ago(minutes: int | None) -> datetime | None:
        return None if minutes is None else now - timedelta(minutes=minutes)

    return health.FeedFreshness(name, ago(attempt), ago(collected), ago(source))


def by_key(checks: list[health.Check]) -> dict[tuple[str, str], health.Check]:
    return {(c.name, c.subject): c for c in checks}


def test_healthy_feeds_pass_every_check() -> None:
    checks = health.collector_checks([feed("bus", NOON, 1, 1, 2), feed("rail", NOON, 1, 1, 1)], NOON)
    assert len(checks) == 5 and health.failures(checks) == []


def test_marta_down_is_told_apart_from_the_collector_being_down() -> None:
    marta_down = by_key(health.collector_checks([feed("rail", NOON, 1, 40, 41)], NOON))
    assert marta_down[("collector_running", "collector")].ok
    assert not marta_down[("feed_available", "rail")].ok
    assert marta_down[("feed_available", "rail")].observed == "last success 40 min ago"

    collector_down = by_key(health.collector_checks([feed("rail", NOON, 30, 30, 31)], NOON))
    assert not collector_down[("collector_running", "collector")].ok


def test_stale_snapshot_from_marta_fails_feed_current() -> None:
    checks = by_key(health.collector_checks([feed("bus", NOON, 1, 1, 25)], NOON))
    assert checks[("feed_available", "bus")].ok
    assert not checks[("feed_current", "bus")].ok
    assert checks[("feed_current", "bus")].observed == "snapshot 24 min old when fetched"


def test_overnight_only_the_collector_itself_is_checked() -> None:
    checks = health.collector_checks([feed("rail", NIGHT, 1, 240, 300)], NIGHT)
    assert [(c.name, c.ok) for c in checks] == [("collector_running", True)]


def test_dq_fails_below_95_percent_and_ignores_tiny_samples() -> None:
    checks = health.dq_checks(
        [
            health.RulePassRate("silver.rail", "valid_line", 990, 10),
            health.RulePassRate("silver.bus", "near_schedule", 900, 100),
            health.RulePassRate("silver.bus", "has_stop", 5, 5),
        ]
    )
    assert [(c.subject, c.ok) for c in checks] == [
        ("silver.rail.valid_line", True),
        ("silver.bus.near_schedule", False),
    ]
    assert checks[1].observed == "90.0% of 1,000 rows passed"


@pytest.mark.parametrize(
    ("latest_minutes_ago", "now", "expected"),
    [(30, NOON, [True]), (120, NOON, [False]), (None, NOON, [False]), (600, NIGHT, [])],
)
def test_bus_arrivals_must_keep_flowing_in_the_daytime(
    latest_minutes_ago: int | None, now: datetime, expected: list[bool]
) -> None:
    latest = None if latest_minutes_ago is None else now - timedelta(minutes=latest_minutes_ago)
    assert [c.ok for c in health.arrival_checks(latest, now)] == expected


def test_feed_freshness_splits_attempts_successes_and_source_time(spark: SparkSession) -> None:
    t = datetime(2026, 11, 16, 12, 0)
    rows: list[tuple[Any, ...]] = [
        ("realtime", "rail", "ok", t, t - timedelta(minutes=1)),
        ("realtime", "rail", "failed", t + timedelta(minutes=2), None),
        ("realtime", "rail", "unchanged", t - timedelta(days=2), t - timedelta(days=2)),
        ("static", "gtfs_static", "ok", t + timedelta(minutes=5), None),
    ]
    spark.createDataFrame(
        rows, "kind string, feed string, status string, finished_at timestamp, feed_ts timestamp"
    ).createOrReplaceTempView("collector_runs")
    out = sql.run(spark, "feed_freshness.sql").collect()
    assert [r.feed for r in out] == ["rail"]
    rail = out[0]
    assert rail.last_attempt_at == t + timedelta(minutes=2) and rail.last_collected_at == t
    assert rail.last_source_ts == t - timedelta(minutes=1)
    assert (rail.runs_last_day, rail.failures_last_day) == (2, 1)


def test_dq_metrics_reads_expectations_from_the_event_log(spark: SparkSession) -> None:
    details = json.dumps(
        {
            "flow_progress": {
                "data_quality": {
                    "expectations": [
                        {
                            "name": "valid_line",
                            "dataset": "rail_checked",
                            "passed_records": 98,
                            "failed_records": 2,
                        }
                    ]
                }
            }
        }
    )
    spark.createDataFrame(
        [
            (datetime(2026, 11, 16), ("u1",), "flow_progress", details),
            (datetime(2026, 11, 16), ("u1",), "x", "{}"),
        ],
        "timestamp timestamp, origin struct<update_id: string>, event_type string, details string",
    ).createOrReplaceTempView("pipeline_events")
    out = sql.run(spark, "dq_metrics.sql").collect()
    assert [(r.update_id, r.dataset, r.rule_name, r.passed_records, r.failed_records) for r in out] == [
        ("u1", "rail_checked", "valid_line", 98, 2)
    ]
