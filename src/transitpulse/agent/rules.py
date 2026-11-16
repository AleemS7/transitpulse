"""Deterministic triage: the fallback when the model is unavailable or its answer is rejected, and the
baseline the agent is evaluated against. Each branch follows the runbook table in health-checks.md.
"""

from __future__ import annotations

from transitpulse.agent.incident import Action, CauseCategory, Evidence, Incident, Severity
from transitpulse.agent.tools import Row, Transcript
from transitpulse.agent.trigger import Trigger

PIPELINE_STEP = Action(
    runbook="health-checks.md",
    step="If refresh_pipeline failed: open the failed update; the first ERROR event names the flow.",
)
COLLECTOR_STEP = Action(
    runbook="collector-failure.md",
    step="Check that the Function is running, then read the latest heartbeat's status and error per feed.",
)
CREDENTIAL_STEP = Action(
    runbook="collector-failure.md",
    step="HTTP 401/403 or dead-lettered uploads: renew the rejected key or token in Key Vault, restart.",
)
OUTAGE_STEP = Action(
    runbook="collector-failure.md",
    step="HTTP 5xx or timeouts on MARTA's side: nothing to fix; record it in the incident log.",
)
STALE_STEP = Action(
    runbook="health-checks.md",
    step="feed_current: compare feed_ts with finished_at in ops.collector_runs; usually MARTA-side, wait.",
)
DRIFT_STEP = Action(
    runbook="health-checks.md",
    step="dq_pass_rate: group the quarantine table by failed rule; a new MARTA format needs a parser fix.",
)
ARRIVALS_STEP = Action(
    runbook="health-checks.md",
    step="bus_arrivals_flowing: if feeds are available, check the bus_trip_updates flow and quarantine.",
)

_DRIFT_RULES = ("decoded", "_parsed")


def _incident(
    severity: Severity,
    cause: CauseCategory | None,
    symptom: str,
    evidence: list[Evidence],
    action: Action | None,
    confidence: float,
) -> Incident:
    return Incident(
        severity=severity,
        symptom=symptom,
        cause_category=cause,
        evidence=evidence,
        recommended_action=action,
        confidence=confidence,
    )


def _check_evidence(trigger: Trigger, names: set[str]) -> list[Evidence]:
    return [
        Evidence(source="T0", statement=f"{c.name} failed for {c.subject}: {c.observed}")
        for c in trigger.failed
        if c.name in names
    ][:4]


def _credential_problem(rows: list[Row]) -> bool:
    return any(r.get("http_status") in (401, 403) or r.get("status") == "deadlettered" for r in rows)


def classify(trigger: Trigger, t: Transcript) -> Incident:
    names = {c.name for c in trigger.failed}

    if trigger.upstream_failed:
        tid, rows = t.call("get_pipeline_updates", {"hours": 3})
        failed = [r for r in rows if r.get("final_state") == "FAILED"]
        if failed:
            f = failed[0]
            evidence = [
                Evidence(
                    source=tid,
                    statement=f"Pipeline update ended FAILED on {f.get('failed_flow')}"
                    f" with {f.get('error_code')}",
                )
            ]
            return _incident(
                "high",
                "pipeline_failure",
                "The hourly pipeline refresh failed.",
                evidence,
                PIPELINE_STEP,
                0.9,
            )
        evidence = [
            Evidence(source="T0", statement="Health checks did not run because an earlier task failed")
        ]
        return _incident(
            "high",
            "pipeline_failure",
            "An earlier job task failed before the checks ran.",
            evidence,
            PIPELINE_STEP,
            0.6,
        )

    if "collector_running" in names:
        return _incident(
            "high",
            "collector_failure",
            "The collector stopped writing heartbeats.",
            _check_evidence(trigger, {"collector_running"}),
            COLLECTOR_STEP,
            0.9,
        )

    if "feed_available" in names:
        feeds = {c.subject for c in trigger.failed if c.name == "feed_available"}
        tid, rows = t.call("get_collector_runs", {"hours": 1})
        bad = [r for r in rows if r.get("feed") in feeds and r.get("status") in ("failed", "deadlettered")]
        evidence = _check_evidence(trigger, {"feed_available"})
        evidence += [
            Evidence(
                source=tid,
                statement=f"{r.get('feed')}: {r.get('runs')} runs {r.get('status')}"
                f" (HTTP {r.get('http_status') or 'none'})",
            )
            for r in bad[:3]
        ]
        if _credential_problem(bad):
            return _incident(
                "high",
                "collector_failure",
                "A feed's requests or uploads are being rejected.",
                evidence,
                CREDENTIAL_STEP,
                0.8,
            )
        return _incident(
            "medium",
            "source_outage",
            "MARTA isn't answering for one or more feeds.",
            evidence,
            OUTAGE_STEP,
            0.7 if bad else 0.5,
        )

    if "feed_current" in names:
        return _incident(
            "low",
            "source_outage",
            "MARTA is serving stale snapshots.",
            _check_evidence(trigger, {"feed_current"}),
            STALE_STEP,
            0.7,
        )

    if "dq_pass_rate" in names:
        rules = {c.subject.rsplit(".", 1)[-1] for c in trigger.failed if c.name == "dq_pass_rate"}
        drift = any(r == "decoded" or r.endswith("_parsed") for r in rules)
        return _incident(
            "medium",
            "schema_drift" if drift else "data_quality",
            "Rows that don't decode are being quarantined." if drift else "A quality rule rejects many rows.",
            _check_evidence(trigger, {"dq_pass_rate"}),
            DRIFT_STEP,
            0.7 if drift else 0.6,
        )

    if "bus_arrivals_flowing" in names:
        return _incident(
            "medium",
            "real_service_disruption",
            "No observed bus arrivals recently while the feeds are up.",
            _check_evidence(trigger, {"bus_arrivals_flowing"}),
            ARRIVALS_STEP,
            0.4,
        )

    return _incident(
        "low",
        None,
        "A check failed that no rule covers.",
        _check_evidence(trigger, names)
        or [Evidence(source="T0", statement="No failed check matched a known rule")],
        None,
        0.0,
    )
