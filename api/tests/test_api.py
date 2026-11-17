import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from transitpulse_api.fake import FakeRepository
from transitpulse_api.genie import Answer, FakeAsker
from transitpulse_api.main import create_app
from transitpulse_api.repository import SqlRepository
from transitpulse_api.settings import Settings

DAY = date(2026, 11, 13)


@pytest.fixture
def repo() -> FakeRepository:
    return FakeRepository(
        routes_rows=[
            {
                "route_id": "29178",
                "route_short_name": "4",
                "route_long_name": "Moreland",
                "transit_mode": "bus",
            },
            {
                "route_id": "RED",
                "route_short_name": "RED",
                "route_long_name": "Red Line",
                "transit_mode": "rail",
            },
        ],
        stops={"130182"},
        otp_rows=[
            {"route_id": "29178", "service_date": DAY, "hour_local": h, "on_time_pct": 50.0}
            for h in range(6, 9)
        ],
        stop_rows=[{"stop_id": "130182", "service_date": DAY, "avg_delay_seconds": 120}],
        rail_rows=[
            {"line": "RED", "service_date": DAY, "station": "FIVE POINTS"},
            {"line": "RED", "service_date": DAY - timedelta(days=1), "station": "FIVE POINTS"},
        ],
        ghost_rows=[
            {"service_date": DAY, "trip_id": "t2", "route_id": "29178", "reason": "no_bus_seen"},
            {"service_date": DAY, "trip_id": "t3", "route_id": "29187", "reason": "cancelled"},
        ],
        kpi_rows=[{"service_date": DAY, "bus_on_time_pct": 60.3}],
    )


@pytest.fixture
def client(repo: FakeRepository) -> TestClient:
    return TestClient(create_app(repo))


def test_health_reports_an_unreachable_warehouse(repo: FakeRepository, client: TestClient) -> None:
    assert client.get("/health").json()["data"] == {"status": "ok"}
    repo.healthy = False
    resp = client.get("/health")
    assert resp.status_code == 503 and resp.json()["error"]["code"] == "warehouse_unreachable"


def test_list_endpoints_page_with_next_offset(client: TestClient) -> None:
    first = client.get("/routes", params={"limit": 1}).json()
    assert [r["route_id"] for r in first["data"]] == ["29178"]
    assert first["meta"] | {"generated_at": None} == {
        "count": 1, "source": "workspace.gold.dim_route", "generated_at": None,
        "limit": 1, "offset": 0, "next_offset": 1,
    }  # fmt: skip
    last = client.get("/routes", params={"limit": 1, "offset": 1}).json()
    assert last["meta"]["next_offset"] == 1 + 1
    assert client.get("/routes", params={"limit": 5}).json()["meta"]["next_offset"] is None


def test_route_delays_by_date_range(client: TestClient) -> None:
    body = client.get("/routes/29178/delays", params={"start": "2026-11-13", "end": "2026-11-13"}).json()
    assert [r["hour_local"] for r in body["data"]] == [6, 7, 8]
    assert body["meta"]["source"] == "workspace.gold.otp_route_hour"


@pytest.mark.parametrize(
    ("path", "params", "status", "code"),
    [
        ("/routes/nope/delays", {}, 404, "route_not_found"),
        ("/stops/nope/delays", {}, 404, "stop_not_found"),
        ("/rail/lines/PURPLE/health", {}, 404, "line_not_found"),
        ("/routes/29178/delays", {"start": "2026-11-14", "end": "2026-11-13"}, 400, "invalid_range"),
        ("/routes/29178/delays", {"start": "2026-01-01", "end": "2026-11-13"}, 400, "invalid_range"),
        ("/routes", {"limit": 0}, 422, "invalid_parameter"),
        ("/ghost-trips", {}, 422, "invalid_parameter"),
        ("/ghost-trips", {"service_date": "13/11/2026"}, 422, "invalid_parameter"),
        ("/no/such/path", {}, 404, "not_found"),
    ],
)
def test_errors_share_one_shape(
    client: TestClient, path: str, params: dict[str, Any], status: int, code: str
) -> None:
    resp = client.get(path, params=params)
    assert resp.status_code == status
    assert resp.json()["error"]["code"] == code and resp.json()["error"]["message"]


def test_rail_health_defaults_to_the_latest_day_and_ignores_case(client: TestClient) -> None:
    rows = client.get("/rail/lines/red/health").json()["data"]
    assert [r["service_date"] for r in rows] == ["2026-11-13"]


def test_ghost_trips_filter_by_route(client: TestClient) -> None:
    rows = client.get("/ghost-trips", params={"service_date": "2026-11-13", "route_id": "29187"}).json()[
        "data"
    ]
    assert [(r["trip_id"], r["reason"]) for r in rows] == [("t3", "cancelled")]


def test_default_range_is_the_last_week(repo: FakeRepository, client: TestClient) -> None:
    today = datetime.now(UTC).date()
    repo.kpi_rows = [{"service_date": today - timedelta(days=d)} for d in range(10)]
    assert client.get("/kpis/daily").json()["meta"]["count"] == 7


def test_request_id_is_echoed(client: TestClient) -> None:
    assert client.get("/health", headers={"x-request-id": "abc"}).headers["x-request-id"] == "abc"


def test_warehouse_errors_become_503_without_leaking_details() -> None:
    def broken(_sql: str, _params: dict[str, Any]) -> list[dict[str, Any]]:
        raise RuntimeError("token=secret connection reset")

    client = TestClient(create_app(SqlRepository(Settings(), broken)), raise_server_exceptions=False)
    resp = client.get("/routes")
    assert resp.status_code == 503 and "secret" not in resp.text


def test_sql_binds_every_value_and_takes_table_names_from_settings() -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def record(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        calls.append((sql, params))
        return []

    repo = SqlRepository(Settings(gold_schema="dev_gold"), record)
    repo.ghost_trips(DAY, "x' OR 1=1 --", 10, 0)
    sql, params = calls[0]
    assert "`workspace`.`dev_gold`.`ghost_trips`" in sql and "OR 1=1" not in sql
    assert params == {"service_date": DAY, "route_id": "x' OR 1=1 --", "limit": 10, "offset": 0}


@pytest.mark.parametrize(
    "env",
    [
        {"TP_BACKEND": "databricks"},
        {"TP_BACKEND": "postgres"},
        {"TP_GOLD_SCHEMA": "gold; DROP TABLE x"},
    ],
)
def test_bad_settings_fail_at_startup(env: dict[str, str]) -> None:
    with pytest.raises(ValueError):
        Settings.from_env(env)


def test_published_openapi_matches_the_code() -> None:
    from transitpulse_api.main import app

    published = json.loads((Path(__file__).parents[1] / "samples" / "openapi.json").read_text())
    assert published == json.loads(json.dumps(app.openapi())), "regenerate samples/openapi.json"


def test_ask_returns_the_answer_with_the_sql_and_rows_behind_it(repo: FakeRepository) -> None:
    answer = Answer("COMPLETED", "Route 4 was least on time (36.2%).", "SELECT ...", ["route", "pct"])
    asker = FakeAsker(Answer(**{**answer.__dict__, "rows": [["4", "36.2"]], "conversation_id": "c1"}))
    body = (
        TestClient(create_app(repo, asker=asker)).post("/ask", json={"question": "  worst route?  "}).json()
    )
    assert asker.questions == ["worst route?"]
    assert body["data"]["rows"] == [["4", "36.2"]] and body["data"]["sql"] == "SELECT ..."
    assert body["meta"]["grounded"] is True and body["meta"]["conversation_id"] == "c1"


def test_ask_without_a_query_is_flagged_ungrounded(repo: FakeRepository) -> None:
    asker = FakeAsker(Answer("COMPLETED", "I can only answer questions about MARTA reliability.", None))
    body = TestClient(create_app(repo, asker=asker)).post("/ask", json={"question": "hello there"}).json()
    assert body["meta"]["grounded"] is False and body["data"]["rows"] == []


@pytest.mark.parametrize(
    ("asker", "payload", "status", "code"),
    [
        (None, {"question": "worst route?"}, 501, "ask_not_configured"),
        (FakeAsker(fail=True), {"question": "worst route?"}, 503, "genie_unavailable"),
        (FakeAsker(Answer("FAILED", None, None)), {"question": "worst route?"}, 502, "genie_no_answer"),
        (FakeAsker(), {"question": "hi"}, 422, "invalid_parameter"),
        (FakeAsker(), {"question": "x" * 501}, 422, "invalid_parameter"),
    ],
)
def test_ask_failures(
    repo: FakeRepository, asker: FakeAsker | None, payload: dict[str, Any], status: int, code: str
) -> None:
    resp = TestClient(create_app(repo, asker=asker)).post("/ask", json=payload)
    assert resp.status_code == status and resp.json()["error"]["code"] == code


def _incident(incident_id: str, status: str, opened: datetime) -> dict[str, Any]:
    return {
        "incident_id": incident_id,
        "status": status,
        "severity": "medium",
        "symptom": "MARTA's rail feed returns errors.",
        "cause_category": "source_outage",
        "opened_at": opened,
        "acknowledged_by": None,
        "acknowledged_at": None,
        "evidence": json.dumps(
            [{"source": "T1", "statement": "rail_arrivals: 11 runs failed with HTTP 500"}]
        ),
        "tool_results": json.dumps({"T1": {"tool": "get_collector_runs", "result": [{"runs": 11}]}}),
    }


@pytest.fixture
def incidents(repo: FakeRepository) -> TestClient:
    now = datetime(2026, 11, 16, 14, tzinfo=UTC)
    repo.incident_rows = [
        _incident("run1-aaaa", "acknowledged", now - timedelta(hours=5)),
        _incident("run2-bbbb", "proposed", now),
    ]
    return TestClient(create_app(repo))


def test_incidents_list_newest_first_and_filter_by_status(incidents: TestClient) -> None:
    assert [r["incident_id"] for r in incidents.get("/incidents").json()["data"]] == [
        "run2-bbbb",
        "run1-aaaa",
    ]
    proposed = incidents.get("/incidents", params={"status": "proposed"}).json()
    assert [r["incident_id"] for r in proposed["data"]] == ["run2-bbbb"]
    assert proposed["meta"]["source"] == "workspace.ops.incidents"
    assert incidents.get("/incidents", params={"status": "closed"}).status_code == 422


def test_incident_detail_returns_evidence_and_the_tool_results_behind_it(incidents: TestClient) -> None:
    data = incidents.get("/incidents/run2-bbbb").json()["data"]
    assert data["evidence"][0]["source"] == "T1"
    assert data["tool_results"]["T1"]["result"] == [{"runs": 11}]
    assert incidents.get("/incidents/nope").json()["error"]["code"] == "incident_not_found"


def test_acknowledge_records_who_and_happens_once(incidents: TestClient) -> None:
    who = {"x-forwarded-email": "oncall@example.com"}
    resp = incidents.post("/incidents/run2-bbbb/ack", headers=who)
    assert resp.status_code == 200
    assert resp.json()["data"]["status"] == "acknowledged"
    assert resp.json()["data"]["acknowledged_by"] == "oncall@example.com"
    again = incidents.post("/incidents/run2-bbbb/ack", headers=who)
    assert again.status_code == 409 and "oncall@example.com" in again.json()["error"]["message"]


@pytest.mark.parametrize(
    ("incident_id", "headers", "status", "code"),
    [
        ("run2-bbbb", {}, 401, "user_unknown"),
        ("nope", {"x-forwarded-email": "a@example.com"}, 404, "incident_not_found"),
        ("run1-aaaa", {"x-forwarded-email": "a@example.com"}, 409, "already_acknowledged"),
    ],
)
def test_acknowledge_failures(
    incidents: TestClient, incident_id: str, headers: dict[str, str], status: int, code: str
) -> None:
    resp = incidents.post(f"/incidents/{incident_id}/ack", headers=headers)
    assert resp.status_code == status and resp.json()["error"]["code"] == code


def test_acknowledge_sql_only_moves_proposed_incidents() -> None:
    calls: list[tuple[str, dict[str, Any]]] = []

    def record(sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        calls.append((sql, params))
        return [{"num_affected_rows": 0}]

    repo = SqlRepository(Settings(ops_schema="dev_ops"), record)
    assert repo.acknowledge("x' --", "a@example.com") is False
    sql, params = calls[0]
    assert "`workspace`.`dev_ops`.`incidents`" in sql and "status = 'proposed'" in sql
    assert params == {"id": "x' --", "who": "a@example.com"}


def test_home_opens_the_incidents_page(client: TestClient) -> None:
    resp = client.get("/", follow_redirects=False)
    assert resp.status_code == 307 and resp.headers["location"] == "ui/incidents.html"


@pytest.mark.parametrize(("path", "kind"), [("incidents.html", "text/html"), ("incidents.js", "javascript")])
def test_incidents_page_is_served_with_a_strict_policy(client: TestClient, path: str, kind: str) -> None:
    resp = client.get(f"/ui/{path}")
    assert resp.status_code == 200 and kind in resp.headers["content-type"]
    assert resp.headers["content-security-policy"].startswith("default-src 'self'")
    assert resp.headers["x-content-type-options"] == "nosniff"


def test_incidents_page_never_renders_data_as_html() -> None:
    static = Path(__file__).parents[1] / "transitpulse_api" / "static"
    # Incident text comes from an LLM and from pipeline errors, so it must only ever be set as text.
    script = (static / "incidents.js").read_text(encoding="utf-8")
    assert "innerHTML" not in script and "insertAdjacentHTML" not in script
    assert "<script>" not in (static / "incidents.html").read_text(encoding="utf-8")


def test_unknown_page_file_is_a_json_404(client: TestClient) -> None:
    resp = client.get("/ui/nope.html")
    assert resp.status_code == 404 and resp.json()["error"]["code"] == "not_found"
