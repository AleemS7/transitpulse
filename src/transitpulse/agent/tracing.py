"""Every number in an incident's evidence must appear in the tool result that statement cites.

A model that writes "the collector has been silent for 45 minutes" has to have seen 45 in a tool result.
Rounding is allowed (a result of 44.6 supports "45"), inventing is not. Dates, times and identifiers such as
"dev_bronze" or "2026-11-15" aren't treated as quantities.
"""

from __future__ import annotations

import re
from collections.abc import Mapping

from transitpulse.agent.incident import Incident

# A standalone quantity: not part of a word, date, time or path, e.g. 45, 1,077 or 38.9.
_NUMBER = re.compile(r"(?<![\w.:/-])\d+(?:,\d{3})*(?:\.\d+)?(?![\w/-]|[.:]\d)")


def numbers(text: str) -> list[float]:
    return [float(m.replace(",", "")) for m in _NUMBER.findall(text)]


def _supported(value: float, available: list[float]) -> bool:
    return any(value in (a, round(a), round(a, 1), round(a, 2)) for a in available)


def untraced(incident: Incident, results: Mapping[str, str]) -> list[str]:
    """Problems that make the incident untrustworthy; empty when every number traces to its source."""
    problems = []
    for item in incident.evidence:
        source = results.get(item.source)
        if source is None:
            problems.append(f"{item.source} is not a tool result in this conversation")
            continue
        available = numbers(source)
        for value in numbers(item.statement):
            if not _supported(value, available):
                problems.append(f"{value:g} in {item.statement!r} does not appear in {item.source}")
    return problems
