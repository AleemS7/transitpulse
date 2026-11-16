"""One triage: the rules always run (fallback and baseline); the agent runs when a model is configured, and
its incident is used only if it was accepted.
"""

from __future__ import annotations

from dataclasses import dataclass

from transitpulse.agent import rules
from transitpulse.agent.incident import Incident
from transitpulse.agent.loop import AgentRun, ChatModel, Limits, run_agent
from transitpulse.agent.tools import DataTools, Runbooks, Transcript
from transitpulse.agent.trigger import Trigger


@dataclass(frozen=True)
class Triage:
    incident: Incident
    classifier: str
    transcript: Transcript
    rules_incident: Incident
    agent: AgentRun | None


def triage(
    trigger: Trigger,
    data: DataTools,
    runbooks: Runbooks,
    model: ChatModel | None,
    limits: Limits | None = None,
) -> Triage:
    rules_t = Transcript(trigger.rows(), data, runbooks)
    baseline = rules.classify(trigger, rules_t)
    if model is None:
        return Triage(baseline, "rules", rules_t, baseline, None)
    agent_t = Transcript(trigger.rows(), data, runbooks)
    names = {e.runbook for e in runbooks.entries}
    run = run_agent(trigger, agent_t, model, names, limits)
    if run.incident is None:
        return Triage(baseline, "rules", rules_t, baseline, run)
    return Triage(run.incident, "agent", agent_t, baseline, run)
