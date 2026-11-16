"""The agent: a plain tool-calling loop over a Databricks Foundation Model endpoint.

The model may only call the read-only tools and, to finish, submit_incident. Its incident is accepted only if
it passes the schema and every number in it traces to a cited tool result; otherwise it's told why and may
try again within the step limit. Running out of steps or tokens means no incident, and the rules decide.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol

from pydantic import ValidationError

from transitpulse.agent import tools
from transitpulse.agent.incident import CAUSES, Incident
from transitpulse.agent.tools import ToolError, Transcript
from transitpulse.agent.tracing import untraced
from transitpulse.agent.trigger import Trigger

Message = dict[str, Any]


@dataclass(frozen=True)
class Reply:
    message: Message
    model: str
    prompt_tokens: int
    completion_tokens: int


class ChatModel(Protocol):
    endpoint: str

    def chat(self, messages: list[Message], tool_specs: list[Message]) -> Reply: ...


class ServingChat:
    """Chat completions on a Databricks serving endpoint, as the job's own identity."""

    def __init__(self, endpoint: str, max_tokens: int = 1000) -> None:
        from databricks.sdk import WorkspaceClient

        self.endpoint = endpoint
        self._client = WorkspaceClient()
        self._max_tokens = max_tokens

    def chat(self, messages: list[Message], tool_specs: list[Message]) -> Reply:
        body = {
            "messages": messages,
            "tools": tool_specs,
            "max_tokens": self._max_tokens,
            "temperature": 0,
        }
        r: Any = self._client.api_client.do(
            "POST", f"/serving-endpoints/{self.endpoint}/invocations", body=body
        )
        usage = r.get("usage") or {}
        return Reply(
            message=r["choices"][0]["message"],
            model=r.get("model", self.endpoint),
            prompt_tokens=int(usage.get("prompt_tokens", 0)),
            completion_tokens=int(usage.get("completion_tokens", 0)),
        )


SUBMIT = "submit_incident"
SUBMIT_SPEC: Message = {
    "type": "function",
    "function": {
        "name": SUBMIT,
        "description": "Submit the incident. Call exactly once, after looking at the evidence.",
        "parameters": {
            "type": "object",
            "properties": {
                "severity": {"type": "string", "enum": ["low", "medium", "high"]},
                "symptom": {"type": "string", "description": "One sentence: what is wrong, as observed."},
                "cause_category": {"type": "string", "enum": [*CAUSES, "unknown"]},
                "evidence": {
                    "type": "array",
                    "description": "1 to 8 facts, each from one tool result.",
                    "items": {
                        "type": "object",
                        "properties": {
                            "source": {"type": "string", "description": "Result ID, e.g. T2"},
                            "statement": {"type": "string"},
                        },
                        "required": ["source", "statement"],
                    },
                },
                "runbook": {"type": "string", "description": "Runbook file from search_runbooks"},
                "runbook_step": {
                    "type": "string",
                    "description": "What to do, taken from the matching runbook text",
                },
                "confidence": {"type": "number", "description": "0 to 1"},
            },
            "required": ["severity", "symptom", "cause_category", "evidence", "confidence"],
        },
    },
}

SYSTEM = """You triage failures in TransitPulse. An Azure Function (the collector) polls MARTA's live bus
and rail feeds every 2 minutes and lands the files in Databricks; a Lakeflow pipeline refreshes the tables
hourly, then health checks run. T0 lists what failed in this run.

Decide the most likely cause:
- source_outage: MARTA's feed is down or stale (HTTP 5xx, timeouts, old snapshots). Nothing to fix here.
- collector_failure: the collector stopped, or its requests or uploads are rejected (401/403, dead-lettered).
- pipeline_failure: the pipeline update or an earlier job task failed.
- schema_drift: the feed's format changed, so rows fail to decode or parse.
- data_quality: rows parse but break a quality rule for another reason.
- real_service_disruption: data flows normally, but buses or trains really stopped or ran very late.

Rules:
1. Check before concluding: call the tools that confirm or rule out a cause. Results are numbered T1, T2...
2. Each evidence statement cites one result ID and uses only numbers that appear in that result.
3. If the evidence doesn't settle it, use cause_category "unknown" with low confidence. Never guess.
4. Pick the runbook and step from search_runbooks results: the step says what to do (from the result text),
   not the section title.
5. Finish by calling submit_incident once. You have {max_steps} steps."""


@dataclass(frozen=True)
class Limits:
    max_steps: int = 8
    max_tokens: int = 40_000


@dataclass
class AgentRun:
    incident: Incident | None = None
    error: str | None = None
    steps: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    model: str | None = None
    rejections: list[str] = field(default_factory=list)


def to_incident(args: Message) -> Incident:
    """Maps the flat submit_incident arguments onto the schema; "unknown" becomes null."""
    cause = args.get("cause_category")
    action = None
    if args.get("runbook") and args.get("runbook_step"):
        action = {"runbook": args["runbook"], "step": args["runbook_step"]}
    return Incident.model_validate(
        {
            "severity": args.get("severity"),
            "symptom": args.get("symptom"),
            "cause_category": None if cause == "unknown" else cause,
            "evidence": args.get("evidence"),
            "recommended_action": action,
            "confidence": args.get("confidence"),
        }
    )


def _review(args: Message, t: Transcript, runbooks: set[str]) -> tuple[Incident | None, str]:
    try:
        incident = to_incident(args)
    except ValidationError as exc:
        return None, "; ".join(f"{'.'.join(map(str, e['loc']))}: {e['msg']}" for e in exc.errors()[:5])
    problems = untraced(incident, t.results)
    if incident.recommended_action and incident.recommended_action.runbook not in runbooks:
        problems.append(f"{incident.recommended_action.runbook} is not a runbook")
    return (None, "; ".join(problems)) if problems else (incident, "")


def run_agent(
    trigger: Trigger, t: Transcript, model: ChatModel, runbooks: set[str], limits: Limits | None = None
) -> AgentRun:
    limits = limits or Limits()
    run = AgentRun()
    specs = [*tools.specs(), SUBMIT_SPEC]
    messages: list[Message] = [
        {"role": "system", "content": SYSTEM.replace("{max_steps}", str(limits.max_steps))},
        {"role": "user", "content": f"T0 (job run {trigger.run_id}): {t.results['T0']}"},
    ]
    while run.steps < limits.max_steps:
        run.steps += 1
        try:
            reply = model.chat(messages, specs)
        except Exception as exc:  # any model-side failure hands over to the rules
            run.error = f"model call failed: {type(exc).__name__}: {str(exc)[:200]}"
            return run
        run.model = reply.model
        run.prompt_tokens += reply.prompt_tokens
        run.completion_tokens += reply.completion_tokens
        calls = reply.message.get("tool_calls") or []
        turn: Message = {"role": "assistant", "content": reply.message.get("content") or ""}
        if calls:
            turn["tool_calls"] = calls
        messages.append(turn)
        if not calls:
            messages.append({"role": "user", "content": f"Use the tools, then call {SUBMIT}."})
        for call in calls:
            name = call.get("function", {}).get("name", "")
            try:
                args = json.loads(call.get("function", {}).get("arguments") or "{}")
            except json.JSONDecodeError:
                args = None
            if not isinstance(args, dict):
                content = json.dumps({"error": "arguments must be a JSON object"})
            elif name == SUBMIT:
                incident, problem = _review(args, t, runbooks)
                if incident is not None:
                    run.incident = incident
                    return run
                run.rejections.append(problem)
                content = json.dumps({"rejected": problem, "hint": "fix these and submit again"})
            else:
                try:
                    tid, rows = t.call(name, args)
                    content = json.dumps({"id": tid, "rows": rows}, default=str)
                except ToolError as exc:
                    content = json.dumps({"error": str(exc)})
            messages.append({"role": "tool", "tool_call_id": call.get("id", ""), "content": content})
        if run.prompt_tokens + run.completion_tokens > limits.max_tokens:
            run.error = f"token budget of {limits.max_tokens} used up"
            return run
    run.error = f"no accepted incident within {limits.max_steps} steps"
    return run
