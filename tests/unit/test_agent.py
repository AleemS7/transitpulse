import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from transitpulse.agent import functions, tools
from transitpulse.agent.cases import CASES, CaseData
from transitpulse.agent.evaluate import run_cases, summary
from transitpulse.agent.incident import Incident
from transitpulse.agent.loop import SUBMIT, Limits, Reply, run_agent
from transitpulse.agent.rules import classify
from transitpulse.agent.tools import Runbooks, ToolError, Transcript
from transitpulse.agent.tracing import numbers, untraced
from transitpulse.agent.triage import triage
from transitpulse.agent.trigger import FailedCheck, Trigger
from transitpulse.common.config import PipelineConfig

RUNBOOKS = Runbooks.load(Path(__file__).parents[2] / "design" / "runbooks")
RUNBOOK_NAMES = {e.runbook for e in RUNBOOKS.entries}
RAIL_500 = next(c for c in CASES if c.name == "marta_rail_http_500")


class ScriptedChat:
    """Plays back replies. Each reply is a list of (tool, args) calls, or a plain-text answer."""

    endpoint = "scripted"

    def __init__(self, *replies: list[tuple[str, dict[str, Any]]] | str | Exception) -> None:
        self.replies = list(replies)
        self.seen: list[list[dict[str, Any]]] = []

    def chat(self, messages: list[dict[str, Any]], tool_specs: list[dict[str, Any]]) -> Reply:
        self.seen.append(list(messages))
        reply = self.replies.pop(0) if self.replies else "I think it's fine."
        if isinstance(reply, Exception):
            raise reply
        if isinstance(reply, str):
            return Reply({"role": "assistant", "content": reply}, "scripted-1", 100, 20)
        calls = [
            {"id": f"call{i}", "type": "function", "function": {"name": n, "arguments": json.dumps(a)}}
            for i, (n, a) in enumerate(reply)
        ]
        return Reply({"role": "assistant", "content": "", "tool_calls": calls}, "scripted-1", 100, 20)


def submit(
    cause: str = "source_outage",
    statement: str = "rail_arrivals: 11 runs failed with HTTP 500",
    source: str = "T1",
    runbook: str = "collector-failure.md",
) -> tuple[str, dict[str, Any]]:
    return (
        SUBMIT,
        {
            "severity": "medium",
            "symptom": "MARTA's rail feed returns errors.",
            "cause_category": cause,
            "evidence": [{"source": source, "statement": statement}],
            "runbook": runbook,
            "runbook_step": "HTTP 5xx on every feed: MARTA outage, nothing to fix.",
            "confidence": 0.8,
        },
    )


# --- number tracing


def test_numbers_skip_dates_times_and_identifiers() -> None:
    assert numbers("2026-11-15 at 04:18 on dev_bronze, route 4: 1,077 trips, 38.9% late") == [4, 1077, 38.9]


def test_untraced_accepts_rounded_numbers_and_rejects_invented_ones() -> None:
    results = {"T1": json.dumps([{"minutes": 44.6, "runs": 11}])}
    ok = Incident.model_validate(
        {
            "severity": "low",
            "symptom": "x" * 10,
            "cause_category": None,
            "recommended_action": None,
            "confidence": None,
            "evidence": [{"source": "T1", "statement": "silent 45 min over 11 runs"}],
        }
    )
    assert untraced(ok, results) == []
    bad = ok.model_copy(
        update={"evidence": [ok.evidence[0].model_copy(update={"statement": "silent 90 min"})]}
    )
    assert untraced(bad, results) == ["90 in 'silent 90 min' does not appear in T1"]
    missing = ok.model_copy(update={"evidence": [ok.evidence[0].model_copy(update={"source": "T7"})]})
    assert untraced(missing, results) == ["T7 is not a tool result in this conversation"]


@pytest.mark.parametrize(
    "change",
    [
        {"cause_category": "aliens"},
        {"confidence": 1.5},
        {"evidence": []},
        {"severity": "critical"},
        {"extra_field": True},
        {"evidence": [{"source": "result 1", "statement": "something happened"}]},
    ],
)
def test_incident_schema_rejects_bad_records(change: dict[str, Any]) -> None:
    good = {
        "severity": "low",
        "symptom": "feed is late",
        "cause_category": None,
        "recommended_action": None,
        "confidence": 0.2,
        "evidence": [{"source": "T0", "statement": "check failed"}],
    }
    Incident.model_validate(good)
    with pytest.raises(ValidationError):
        Incident.model_validate(good | change)


# --- triggers and tool functions


def test_fingerprint_ignores_order_and_separates_upstream_failures() -> None:
    a = FailedCheck("feed_available", "rail_arrivals", "x", "y")
    b = FailedCheck("feed_current", "bus_trip_updates", "x", "y")
    assert Trigger("1", (a, b)).fingerprint() == Trigger("2", (b, a)).fingerprint()
    assert Trigger("1", (a,)).fingerprint() != Trigger("1", (a,), upstream_failed=True).fingerprint()
    assert not Trigger("1", ()).needs_triage


def test_tool_functions_render_for_an_environment() -> None:
    statements = functions.statements(PipelineConfig(ops="dev_ops", gold="dev_gold"))
    assert len(statements) == len(tools.DATA_TOOLS)
    assert all(
        "${" not in s and s.startswith("CREATE OR REPLACE FUNCTION workspace.dev_ops.") for s in statements
    )
    assert {s.split(".")[2].split("(")[0].strip() for s in statements} == set(tools.DATA_TOOLS)
    with pytest.raises(ValueError, match="identifier"):
        functions.statements(PipelineConfig(ops="ops; DROP TABLE x"))


def test_tool_calls_are_validated_and_clipped() -> None:
    data = CaseData({"get_collector_runs": [{"latest_error": "e" * 1000}] * 100})
    rows = tools.run(data, RUNBOOKS, "get_collector_runs", {"hours": "6"})
    assert len(rows) == tools.MAX_ROWS and len(rows[0]["latest_error"]) == tools.MAX_TEXT
    for name, args in [
        ("drop_table", {}),
        ("get_collector_runs", {"hours": 0}),
        ("get_collector_runs", {"hours": True}),
        ("search_runbooks", {"query": " "}),
    ]:
        with pytest.raises(ToolError):
            tools.run(data, RUNBOOKS, name, args)


def test_runbook_search_finds_the_matching_fix() -> None:
    hits = RUNBOOKS.search("rail 401 key rejected")
    assert hits[0]["runbook"] == "collector-failure.md" and "401" in hits[0]["text"]
    assert RUNBOOKS.search("zzz qqq") == []


# --- rules


def test_rules_classify_every_case_except_the_known_miss() -> None:
    wrong = {
        c.name
        for c in CASES
        if classify(c.trigger, Transcript(c.trigger.rows(), c.data, RUNBOOKS)).cause_category != c.expected
    }
    # Decode errors below the 5% threshold aren't a failed check, so the rules see only "no arrivals".
    assert wrong == {"trip_updates_stop_decoding"}


def test_rules_incidents_always_trace() -> None:
    for c in CASES:
        t = Transcript(c.trigger.rows(), c.data, RUNBOOKS)
        incident = classify(c.trigger, t)
        assert untraced(incident, t.results) == [], c.name
        assert incident.recommended_action is None or incident.recommended_action.runbook in RUNBOOK_NAMES


# --- agent loop


def test_agent_looks_then_submits_a_traced_incident() -> None:
    model = ScriptedChat([("get_collector_runs", {"hours": 1})], [submit()])
    t = Transcript(RAIL_500.trigger.rows(), RAIL_500.data, RUNBOOKS)
    run = run_agent(RAIL_500.trigger, t, model, RUNBOOK_NAMES)
    assert run.incident is not None and run.incident.cause_category == "source_outage"
    assert (run.steps, run.prompt_tokens, run.completion_tokens, run.model) == (2, 200, 40, "scripted-1")
    assert set(t.results) == {"T0", "T1"}


def test_agent_invented_number_is_rejected_then_corrected() -> None:
    model = ScriptedChat(
        [("get_collector_runs", {"hours": 1})],
        [submit(statement="rail failed 250 times")],
        [submit()],
    )
    t = Transcript(RAIL_500.trigger.rows(), RAIL_500.data, RUNBOOKS)
    run = run_agent(RAIL_500.trigger, t, model, RUNBOOK_NAMES)
    assert run.incident is not None and len(run.rejections) == 1
    assert "250" in run.rejections[0]
    feedback = model.seen[2][-1]
    assert feedback["role"] == "tool" and "rejected" in feedback["content"]


@pytest.mark.parametrize(
    ("reply", "problem"),
    [
        (submit(runbook="made-up.md"), "made-up.md is not a runbook"),
        (submit(source="T9"), "T9 is not a tool result"),
        ((SUBMIT, {"severity": "medium"}), "symptom"),
    ],
)
def test_agent_submissions_that_fail_review(reply: tuple[str, dict[str, Any]], problem: str) -> None:
    t = Transcript(RAIL_500.trigger.rows(), RAIL_500.data, RUNBOOKS)
    run = run_agent(
        RAIL_500.trigger,
        t,
        ScriptedChat([("get_collector_runs", {"hours": 1})], [reply]),
        RUNBOOK_NAMES,
        Limits(max_steps=2),
    )
    assert run.incident is None and problem in run.rejections[0]


def test_unknown_cause_becomes_null() -> None:
    t = Transcript(RAIL_500.trigger.rows(), RAIL_500.data, RUNBOOKS)
    run = run_agent(
        RAIL_500.trigger,
        t,
        ScriptedChat([("get_collector_runs", {"hours": 1})], [submit(cause="unknown")]),
        RUNBOOK_NAMES,
    )
    assert run.incident is not None and run.incident.cause_category is None


def test_bad_tool_call_is_reported_back_to_the_model() -> None:
    model = ScriptedChat(
        [("run_sql", {"sql": "DROP TABLE x"})], [("get_collector_runs", {"hours": 1})], [submit()]
    )
    t = Transcript(RAIL_500.trigger.rows(), RAIL_500.data, RUNBOOKS)
    run = run_agent(RAIL_500.trigger, t, model, RUNBOOK_NAMES)
    assert run.incident is not None
    assert "unknown tool" in model.seen[1][-1]["content"]


@pytest.mark.parametrize(
    ("model", "limits", "error"),
    [
        (ScriptedChat(), Limits(max_steps=3), "no accepted incident within 3 steps"),
        (ScriptedChat(TimeoutError("endpoint timed out")), None, "model call failed: TimeoutError"),
        (ScriptedChat([("get_feed_freshness", {})] * 5), Limits(max_tokens=200), "token budget"),
    ],
)
def test_agent_failures_fall_back_to_rules(model: ScriptedChat, limits: Limits | None, error: str) -> None:
    result = triage(RAIL_500.trigger, RAIL_500.data, RUNBOOKS, model, limits)
    assert result.classifier == "rules" and result.incident == result.rules_incident
    assert result.agent is not None and result.agent.error is not None and error in result.agent.error


def test_triage_without_a_model_uses_rules() -> None:
    result = triage(RAIL_500.trigger, RAIL_500.data, RUNBOOKS, None)
    assert result.classifier == "rules" and result.agent is None
    assert result.incident.cause_category == "source_outage"


def test_evaluation_scores_rules_against_labels() -> None:
    results = run_cases(CASES, RUNBOOKS, None)
    s = summary(results)
    assert s["cases"] == len(CASES) == 14
    assert s["rules_accuracy"] == pytest.approx(13 / 14)
    assert s["agent_accuracy"] == 0 and s["agent_accepted_rate"] == 0
    assert {r.origin for r in results} == {"chaos", "replayed"}


def test_incident_row_matches_the_table_and_starts_proposed() -> None:
    from datetime import UTC, datetime

    from transitpulse.agent import record

    model = ScriptedChat([("get_collector_runs", {"hours": 1})], [submit()])
    result = triage(RAIL_500.trigger, RAIL_500.data, RUNBOOKS, model)
    now = datetime(2026, 11, 16, 14, tzinfo=UTC)
    row = record.row(RAIL_500.trigger, result, "databricks-meta-llama-3-3-70b-instruct", now)
    assert tuple(row) == record.NAMES
    assert (
        row["status"] == "proposed" and row["classifier"] == "agent" and row["model_version"] == "scripted-1"
    )
    assert row["incident_id"] == record.row(RAIL_500.trigger, result, None, now)["incident_id"]
    assert json.loads(row["tool_results"])["T1"]["result"][-1]["http_status"] == 500
    assert row["rules_cause_category"] == "source_outage"
    message = record.alert(row, new=True)
    assert message.startswith(f"Proposed incident {row['incident_id']} (medium, source_outage, by agent)")
