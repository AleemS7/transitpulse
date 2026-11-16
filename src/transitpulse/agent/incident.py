"""The incident record the triage step proposes. Whoever writes it (the agent or the rules), it must pass
this schema, and every number in its evidence must come from a tool result it cites (see tracing.py).
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

CauseCategory = Literal[
    "source_outage",
    "collector_failure",
    "pipeline_failure",
    "schema_drift",
    "data_quality",
    "real_service_disruption",
]
CAUSES: tuple[str, ...] = CauseCategory.__args__  # type: ignore[attr-defined]

Severity = Literal["low", "medium", "high"]


class Evidence(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source: str = Field(pattern=r"^T\d+$", description="ID of the tool result this statement comes from")
    statement: str = Field(min_length=5, max_length=400)


class Action(BaseModel):
    model_config = ConfigDict(extra="forbid")

    runbook: str = Field(pattern=r"^[a-z0-9-]+\.md$")
    step: str = Field(min_length=5, max_length=400)


class Incident(BaseModel):
    model_config = ConfigDict(extra="forbid")

    severity: Severity
    symptom: str = Field(min_length=5, max_length=300)
    # None means "can't tell from the evidence"; a guess is worse than no answer.
    cause_category: CauseCategory | None
    evidence: list[Evidence] = Field(min_length=1, max_length=8)
    recommended_action: Action | None
    confidence: float | None = Field(ge=0, le=1)
