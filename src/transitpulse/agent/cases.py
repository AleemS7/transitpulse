"""Labeled triage cases: what the tools would return in a known failure, and the right cause.

"replayed" cases copy tool output from failures this project actually had; "chaos" cases are the failures
the chaos tests inject (stop the collector, corrupt a file, rename a field, revoke a key). Tests run the rules
on them, and the evaluation job runs the rules and the agent side by side.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from transitpulse.agent.incident import CauseCategory
from transitpulse.agent.tools import Row
from transitpulse.agent.trigger import FailedCheck, Trigger


@dataclass(frozen=True)
class CaseData:
    tables: dict[str, list[Row]] = field(default_factory=dict)

    def call(self, name: str, hours: int | None) -> list[Row]:
        return list(self.tables.get(name, []))


@dataclass(frozen=True)
class Case:
    name: str
    origin: str
    expected: CauseCategory
    trigger: Trigger
    data: CaseData


def _check(name: str, subject: str, observed: str, expected: str = "<= 15 min") -> FailedCheck:
    return FailedCheck(name, subject, observed, expected)


FEEDS_OK = [
    {
        "feed": f,
        "minutes_since_attempt": 1,
        "minutes_since_success": 1,
        "snapshot_age_minutes": 0,
        "runs_last_day": 720,
        "failures_last_day": 0,
    }
    for f in ("bus_vehicle_positions", "bus_trip_updates", "rail_arrivals")
]
RUNS_OK = [
    {
        "feed": f,
        "status": "ok",
        "http_status": 200,
        "runs": 30,
        "last_finished_at": "2026-11-16 14:02:01",
        "latest_error": None,
    }
    for f in ("bus_vehicle_positions", "bus_trip_updates", "rail_arrivals")
]
PIPELINE_OK = [
    {
        "update_id": "cb9949e8",
        "started_at": "2026-11-16 14:15:40",
        "ended_at": "2026-11-16 14:19:02",
        "final_state": "COMPLETED",
        "failed_flow": None,
        "error_code": None,
        "error_message": None,
    }
]
SERVICE_OK = [
    {
        "hour_start_utc": "2026-11-16 13:00:00",
        "arrivals": 6240,
        "on_time_pct": 61.5,
        "avg_delay_seconds": 290.0,
        "arrivals_week_before": 6105,
    }
]


def _data(**tables: list[Row]) -> CaseData:
    base: dict[str, list[Row]] = {
        "get_feed_freshness": FEEDS_OK,
        "get_collector_runs": RUNS_OK,
        "get_pipeline_updates": PIPELINE_OK,
        "get_bus_service": SERVICE_OK,
    }
    return CaseData(base | tables)


def _runs(feed: str, status: str, http: int | None, runs: int, error: str | None) -> list[Row]:
    others = [r for r in RUNS_OK if r["feed"] != feed]
    row = {
        "feed": feed,
        "status": status,
        "http_status": http,
        "runs": runs,
        "last_finished_at": "2026-11-16 14:02:01",
        "latest_error": error,
    }
    return [*others, row]


CASES: list[Case] = [
    Case(
        "collector_stopped",
        "chaos",
        "collector_failure",
        Trigger("run-1", (_check("collector_running", "collector", "last heartbeat 47 min ago"),)),
        _data(
            get_feed_freshness=[
                {**r, "minutes_since_attempt": 47, "minutes_since_success": 47} for r in FEEDS_OK
            ],
            get_collector_runs=[],
        ),
    ),
    Case(
        "marta_rail_http_500",
        "replayed",
        "source_outage",
        Trigger("run-2", (_check("feed_available", "rail_arrivals", "last success 22 min ago"),)),
        _data(get_collector_runs=_runs("rail_arrivals", "failed", 500, 11, "HTTP 500 Internal Server Error")),
    ),
    Case(
        "rail_key_rejected",
        "chaos",
        "collector_failure",
        Trigger("run-3", (_check("feed_available", "rail_arrivals", "last success 31 min ago"),)),
        _data(get_collector_runs=_runs("rail_arrivals", "failed", 401, 15, "HTTP 401 Unauthorized")),
    ),
    Case(
        "uploads_dead_lettered",
        "chaos",
        "collector_failure",
        Trigger("run-4", (_check("feed_available", "bus_trip_updates", "last success 26 min ago"),)),
        _data(
            get_collector_runs=_runs("bus_trip_updates", "deadlettered", 200, 13, "upload failed: HTTP 403")
        ),
    ),
    Case(
        "rail_connect_timeouts",
        "replayed",
        "source_outage",
        Trigger("run-5", (_check("feed_available", "rail_arrivals", "last success 40 min ago"),)),
        _data(
            get_collector_runs=_runs("rail_arrivals", "failed", None, 20, "ConnectTimeout after 3 attempts")
        ),
    ),
    Case(
        "stale_bus_snapshot",
        "chaos",
        "source_outage",
        Trigger(
            "run-6",
            (_check("feed_current", "bus_vehicle_positions", "snapshot 38 min old when fetched"),),
        ),
        _data(
            get_feed_freshness=[
                {**r, "snapshot_age_minutes": 38} if r["feed"] == "bus_vehicle_positions" else r
                for r in FEEDS_OK
            ]
        ),
    ),
    Case(
        "pipeline_udf_memory_limit",
        "replayed",
        "pipeline_failure",
        Trigger("run-7", (), upstream_failed=True),
        _data(
            get_pipeline_updates=[
                {
                    "update_id": "9a0c3ea8",
                    "started_at": "2026-11-15 23:48:31",
                    "ended_at": "2026-11-15 23:53:35",
                    "final_state": "FAILED",
                    "failed_flow": "workspace.dev_bronze.bus_trip_updates",
                    "error_code": "UDF_PYSPARK_USER_CODE_ERROR.MEMORY_LIMIT_SERVERLESS",
                    "error_message": "[STREAM_FAILED] Query terminated with exception",
                },
            ]
        ),
    ),
    Case(
        "pipeline_missing_module",
        "replayed",
        "pipeline_failure",
        Trigger("run-8", (), upstream_failed=True),
        _data(
            get_pipeline_updates=[
                {
                    "update_id": "7ff2a059",
                    "started_at": "2026-11-15 23:44:55",
                    "ended_at": "2026-11-15 23:47:15",
                    "final_state": "FAILED",
                    "failed_flow": "workspace.dev_bronze.bus_vehicle_positions",
                    "error_code": "PYTHON_EXCEPTION",
                    "error_message": "ModuleNotFoundError: No module named 'google.transit'",
                },
            ]
        ),
    ),
    Case(
        "bus_file_corrupted",
        "chaos",
        "schema_drift",
        Trigger(
            "run-9",
            (
                _check(
                    "dq_pass_rate",
                    "bus_vehicle_positions_checked.decoded",
                    "61.0% of 9,800 rows passed",
                    ">= 95%",
                ),
            ),
        ),
        _data(
            get_dq_failures=[
                {
                    "dataset": "bus_vehicle_positions_checked",
                    "rule_name": "decoded",
                    "passed_records": 5978,
                    "failed_records": 3822,
                    "pass_pct": 61.0,
                },
            ]
        ),
    ),
    Case(
        "rail_field_renamed",
        "chaos",
        "schema_drift",
        Trigger(
            "run-10",
            (
                _check(
                    "dq_pass_rate",
                    "rail_arrivals_checked.event_time_parsed",
                    "0.0% of 4,120 rows passed",
                    ">= 95%",
                ),
            ),
        ),
        _data(
            get_dq_failures=[
                {
                    "dataset": "rail_arrivals_checked",
                    "rule_name": "event_time_parsed",
                    "passed_records": 0,
                    "failed_records": 4120,
                    "pass_pct": 0.0,
                },
            ]
        ),
    ),
    Case(
        "far_from_schedule",
        "chaos",
        "data_quality",
        Trigger(
            "run-11",
            (
                _check(
                    "dq_pass_rate",
                    "bus_stop_arrivals_checked.near_schedule",
                    "93.1% of 152,000 rows passed",
                    ">= 95%",
                ),
            ),
        ),
        _data(
            get_dq_failures=[
                {
                    "dataset": "bus_stop_arrivals_checked",
                    "rule_name": "near_schedule",
                    "passed_records": 141512,
                    "failed_records": 10488,
                    "pass_pct": 93.1,
                },
            ]
        ),
    ),
    Case(
        "buses_off_the_road",
        "chaos",
        "real_service_disruption",
        Trigger(
            "run-12",
            (
                _check(
                    "bus_arrivals_flowing",
                    "silver.bus_stop_arrivals",
                    "latest arrival 104 min ago",
                    "<= 90 min",
                ),
            ),
        ),
        _data(
            get_bus_service=[
                {
                    "hour_start_utc": "2026-11-16 12:00:00",
                    "arrivals": 0,
                    "on_time_pct": None,
                    "avg_delay_seconds": None,
                    "arrivals_week_before": 6105,
                },
                {
                    "hour_start_utc": "2026-11-16 13:00:00",
                    "arrivals": 0,
                    "on_time_pct": None,
                    "avg_delay_seconds": None,
                    "arrivals_week_before": 6240,
                },
            ]
        ),
    ),
    Case(
        "trip_updates_stop_decoding",
        "chaos",
        "schema_drift",
        # The arrivals check fires first; the quality rule hasn't crossed its threshold over a whole day yet.
        Trigger(
            "run-13",
            (
                _check(
                    "bus_arrivals_flowing",
                    "silver.bus_stop_arrivals",
                    "latest arrival 95 min ago",
                    "<= 90 min",
                ),
            ),
        ),
        _data(
            get_dq_failures=[
                {
                    "dataset": "bus_trips_checked",
                    "rule_name": "decoded",
                    "passed_records": 41000,
                    "failed_records": 1380,
                    "pass_pct": 96.7,
                },
            ],
            get_bus_service=[
                {
                    "hour_start_utc": "2026-11-16 13:00:00",
                    "arrivals": 0,
                    "on_time_pct": None,
                    "avg_delay_seconds": None,
                    "arrivals_week_before": 6240,
                },
            ],
        ),
    ),
    Case(
        "marta_bus_feed_down",
        "chaos",
        "source_outage",
        Trigger(
            "run-14",
            (
                _check("feed_available", "bus_trip_updates", "last success 33 min ago"),
                _check(
                    "bus_arrivals_flowing",
                    "silver.bus_stop_arrivals",
                    "latest arrival 97 min ago",
                    "<= 90 min",
                ),
            ),
        ),
        _data(
            get_collector_runs=_runs("bus_trip_updates", "failed", 503, 16, "HTTP 503 Service Unavailable")
        ),
    ),
]
