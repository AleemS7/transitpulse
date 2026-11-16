"""The ops.incidents row for one triage. Kept apart from the job so it's testable without Spark."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from transitpulse.agent.triage import Triage
from transitpulse.agent.trigger import Trigger

COLUMNS = (
    "incident_id STRING NOT NULL, fingerprint STRING, status STRING, opened_at TIMESTAMP,"
    " last_seen_at TIMESTAMP, occurrences INT, run_id STRING, last_run_id STRING, severity STRING,"
    " symptom STRING, cause_category STRING, confidence DOUBLE, evidence STRING, recommended_runbook STRING,"
    " recommended_step STRING, tool_results STRING, classifier STRING, model_endpoint STRING,"
    " model_version STRING, rules_cause_category STRING, agent_error STRING, agent_steps INT,"
    " prompt_tokens INT, completion_tokens INT, acknowledged_by STRING, acknowledged_at TIMESTAMP,"
    " cleared_at TIMESTAMP, _updated_at TIMESTAMP"
)
NAMES = tuple(c.strip().split(" ")[0] for c in COLUMNS.split(","))


def incident_id(trigger: Trigger) -> str:
    """Stable per job run and problem, so re-running the same job run doesn't add a second incident."""
    return f"{trigger.run_id}-{trigger.fingerprint()[:8]}"


def row(trigger: Trigger, result: Triage, model_endpoint: str | None, now: datetime) -> dict[str, Any]:
    i = result.incident
    run = result.agent
    t = result.transcript
    tool_results = {c["id"]: {**c, "result": json.loads(t.results[c["id"]])} for c in t.calls}
    values: dict[str, Any] = {
        "incident_id": incident_id(trigger),
        "fingerprint": trigger.fingerprint(),
        # Only a person moves an incident past proposed (POST /incidents/{id}/ack).
        "status": "proposed",
        "opened_at": now,
        "last_seen_at": now,
        "occurrences": 1,
        "run_id": trigger.run_id,
        "last_run_id": trigger.run_id,
        "severity": i.severity,
        "symptom": i.symptom,
        "cause_category": i.cause_category,
        "confidence": i.confidence,
        "evidence": json.dumps([e.model_dump() for e in i.evidence]),
        "recommended_runbook": i.recommended_action.runbook if i.recommended_action else None,
        "recommended_step": i.recommended_action.step if i.recommended_action else None,
        "tool_results": json.dumps(tool_results, default=str),
        "classifier": result.classifier,
        "model_endpoint": model_endpoint,
        "model_version": run.model if run else None,
        "rules_cause_category": result.rules_incident.cause_category,
        "agent_error": run.error if run else None,
        "agent_steps": run.steps if run else None,
        "prompt_tokens": run.prompt_tokens if run else None,
        "completion_tokens": run.completion_tokens if run else None,
        "acknowledged_by": None,
        "acknowledged_at": None,
        "cleared_at": None,
        "_updated_at": now,
    }
    return values


def alert(incident_row: dict[str, Any], new: bool) -> str:
    """The message the failing triage task leaves, which the job's failure email carries."""
    prefix = "Proposed incident" if new else "Still open: incident"
    cause = incident_row["cause_category"] or "cause unknown"
    step = incident_row["recommended_step"] or "no runbook step"
    return (
        f"{prefix} {incident_row['incident_id']} ({incident_row['severity']}, {cause}, by"
        f" {incident_row['classifier']}): {incident_row['symptom']} Next: {step}"
        f" [{incident_row['recommended_runbook'] or '-'}]"
    )
