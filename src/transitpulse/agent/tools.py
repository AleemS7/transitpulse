"""The only things the triage step can look at. Each data tool is a read-only Unity Catalog SQL function
(tools.sql) that takes at most an hour window, so neither the model nor the rules can run their own SQL.
Runbook lookup is plain keyword search over the runbooks in design/runbooks.
"""

from __future__ import annotations

import json
import re
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

Row = dict[str, Any]

MAX_HOURS = 168
MAX_ROWS = 40
MAX_TEXT = 300

_HOURS = {
    "type": "object",
    "properties": {
        "hours": {"type": "integer", "description": "How many hours back to look (1 to 168)."},
    },
    "required": ["hours"],
}
_NONE: dict[str, Any] = {"type": "object", "properties": {}}

DATA_TOOLS: dict[str, tuple[str, dict[str, Any]]] = {
    "get_check_history": (
        "Health checks that failed in the window: how many hourly runs failed, first and last failure, and"
        " the latest observed value. Shows whether a problem is new or ongoing.",
        _HOURS,
    ),
    "get_feed_freshness": (
        "Per realtime feed: minutes since the collector last tried, minutes since MARTA last answered, how"
        " old MARTA's snapshot was when fetched, and runs and failures in the last day.",
        _NONE,
    ),
    "get_collector_runs": (
        "Collector outcomes per feed, status and HTTP status in the window, with the latest error text."
        " Use it to tell a MARTA outage (5xx, timeouts) from a collector problem (401/403, dead-lettered).",
        _HOURS,
    ),
    "get_pipeline_updates": (
        "Lakeflow pipeline updates in the window: final state, and for failed ones the flow and error code.",
        _HOURS,
    ),
    "get_dq_failures": (
        "Data-quality rules that rejected rows in the window, with passed and failed counts. A failing"
        " 'decoded' or '*_parsed' rule means the source format changed.",
        _HOURS,
    ),
    "get_bus_service": (
        "Observed bus arrivals per hour in the window with on-time % and average delay, next to the same"
        " hour one week earlier. Shows whether buses really stopped or ran late.",
        _HOURS,
    ),
}

SEARCH_TOOL = "search_runbooks"
SEARCH_SPEC: dict[str, Any] = {
    "type": "object",
    "properties": {"query": {"type": "string", "description": "A few words describing the problem."}},
    "required": ["query"],
}


def specs() -> list[dict[str, Any]]:
    """Tool definitions in the chat-completions format."""
    tools = [
        {"type": "function", "function": {"name": name, "description": desc, "parameters": params}}
        for name, (desc, params) in DATA_TOOLS.items()
    ]
    tools.append(
        {
            "type": "function",
            "function": {
                "name": SEARCH_TOOL,
                "description": "Find the runbook steps that match a problem. Returns runbook file and step.",
                "parameters": SEARCH_SPEC,
            },
        }
    )
    return tools


class ToolError(ValueError):
    """A bad tool call (unknown tool, bad argument). The model is told and can try again."""


class DataTools(Protocol):
    def call(self, name: str, hours: int | None) -> list[Row]: ...


Query = Callable[[str, dict[str, Any]], list[Row]]


class SqlTools:
    """Runs the Unity Catalog tool functions. Function names come from config, never from the model."""

    def __init__(self, query: Query, schema: str) -> None:
        self._query = query
        self._schema = schema

    def call(self, name: str, hours: int | None) -> list[Row]:
        if hours is None:
            return self._query(f"SELECT * FROM {self._schema}.{name}()", {})
        return self._query(f"SELECT * FROM {self._schema}.{name}(:hours)", {"hours": hours})


@dataclass(frozen=True)
class RunbookEntry:
    runbook: str
    section: str
    text: str


_WORD = re.compile(r"[a-z0-9_]{3,}")
_STOP = {"the", "and", "for", "with", "from", "not", "but", "are", "was", "has", "this", "that", "all"}


def _terms(text: str) -> set[str]:
    return set(_WORD.findall(text.lower())) - _STOP


class Runbooks:
    """Each heading's text, and each row of a fix table, is one searchable entry."""

    def __init__(self, entries: list[RunbookEntry]) -> None:
        self.entries = entries

    @classmethod
    def parse(cls, name: str, markdown: str) -> list[RunbookEntry]:
        entries: list[RunbookEntry] = []
        section, prose = "", list[str]()

        def flush() -> None:
            text = " ".join(line.strip() for line in prose if line.strip())
            if text:
                entries.append(RunbookEntry(name, section, text))
            prose.clear()

        for line in markdown.splitlines():
            if line.startswith("#"):
                flush()
                section = line.lstrip("#").strip()
            elif line.startswith("|") and not set(line) <= set("|-: "):
                cells = [c.strip() for c in line.strip("|").split("|")]
                if cells and cells[0].lower() not in {"check", "what you see", "date"}:
                    entries.append(RunbookEntry(name, section, " | ".join(cells)))
            else:
                prose.append(line)
        flush()
        return entries

    @classmethod
    def load(cls, folder: Path) -> Runbooks:
        entries: list[RunbookEntry] = []
        for path in sorted(folder.glob("*.md")):
            entries += cls.parse(path.name, path.read_text(encoding="utf-8"))
        return cls(entries)

    def search(self, query: str, limit: int = 3) -> list[Row]:
        wanted = _terms(query)
        scored = [(len(wanted & _terms(f"{e.section} {e.text}")), i, e) for i, e in enumerate(self.entries)]
        best = sorted((s for s in scored if s[0] > 0), key=lambda s: (-s[0], s[1]))[:limit]
        return [{"runbook": e.runbook, "section": e.section, "text": e.text[:600]} for _, _, e in best]


def _clip(row: Row) -> Row:
    return {k: v[:MAX_TEXT] if isinstance(v, str) else v for k, v in row.items()}


def run(data: DataTools, runbooks: Runbooks, name: str, args: dict[str, Any]) -> list[Row]:
    """Validates one tool call and runs it. Results are capped so one call can't flood the context."""
    if name == SEARCH_TOOL:
        query = args.get("query")
        if not isinstance(query, str) or not query.strip():
            raise ToolError("search_runbooks needs a non-empty 'query'")
        return runbooks.search(query[:200])
    if name not in DATA_TOOLS:
        raise ToolError(f"unknown tool {name!r}")
    hours = None
    if DATA_TOOLS[name][1] is _HOURS:
        raw = args.get("hours")
        if isinstance(raw, str) and raw.strip().isdigit():
            raw = int(raw)
        if isinstance(raw, bool) or not isinstance(raw, int | float) or not 1 <= raw <= MAX_HOURS:
            raise ToolError(f"{name} needs 'hours' between 1 and {MAX_HOURS}")
        hours = int(raw)
    return [_clip(r) for r in data.call(name, hours)[:MAX_ROWS]]


def dumps(rows: list[Row]) -> str:
    return json.dumps(rows, default=str, sort_keys=True)


class Transcript:
    """Numbers every result (T0 is the trigger, then T1, T2, ...) so evidence can cite where it came from."""

    def __init__(self, trigger: list[Row], data: DataTools, runbooks: Runbooks) -> None:
        self._data, self._runbooks = data, runbooks
        self.results: dict[str, str] = {"T0": dumps(trigger)}
        self.calls: list[Row] = [{"id": "T0", "tool": "trigger", "args": {}}]

    def call(self, name: str, args: dict[str, Any]) -> tuple[str, list[Row]]:
        rows = run(self._data, self._runbooks, name, args)
        tid = f"T{len(self.results)}"
        self.results[tid] = dumps(rows)
        self.calls.append({"id": tid, "tool": name, "args": args})
        return tid, rows
