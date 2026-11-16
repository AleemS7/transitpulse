"""Runs labeled cases through the rules and (optionally) the agent and scores root-cause accuracy."""

from __future__ import annotations

import time
from collections.abc import Iterable
from dataclasses import dataclass

from transitpulse.agent.cases import Case
from transitpulse.agent.loop import ChatModel, Limits
from transitpulse.agent.tools import Runbooks
from transitpulse.agent.triage import triage


@dataclass(frozen=True)
class CaseResult:
    case: str
    origin: str
    expected: str
    rules_cause: str | None
    agent_cause: str | None
    agent_accepted: bool
    agent_error: str | None
    rejections: int
    steps: int
    tokens: int
    seconds: float
    model: str | None


def run_cases(
    cases: Iterable[Case], runbooks: Runbooks, model: ChatModel | None, limits: Limits | None = None
) -> list[CaseResult]:
    results = []
    for case in cases:
        start = time.monotonic()
        t = triage(case.trigger, case.data, runbooks, model, limits)
        run = t.agent
        results.append(
            CaseResult(
                case=case.name,
                origin=case.origin,
                expected=case.expected,
                rules_cause=t.rules_incident.cause_category,
                agent_cause=run.incident.cause_category if run and run.incident else None,
                agent_accepted=bool(run and run.incident),
                agent_error=run.error if run else None,
                rejections=len(run.rejections) if run else 0,
                steps=run.steps if run else 0,
                tokens=(run.prompt_tokens + run.completion_tokens) if run else 0,
                seconds=round(time.monotonic() - start, 2),
                model=run.model if run else None,
            )
        )
    return results


def summary(results: list[CaseResult]) -> dict[str, float]:
    n = len(results) or 1
    return {
        "cases": len(results),
        "rules_accuracy": sum(r.rules_cause == r.expected for r in results) / n,
        "agent_accuracy": sum(r.agent_cause == r.expected for r in results) / n,
        "agent_accepted_rate": sum(r.agent_accepted for r in results) / n,
        "agent_avg_tokens": sum(r.tokens for r in results) / n,
        "agent_avg_seconds": sum(r.seconds for r in results) / n,
    }
