"""Landing retention: which day folders in the landing volume can be deleted. Plain Python, unit-tested.

Bronze is the permanent copy, so a landed file may go once it is old and its contents are safely in bronze.
A day folder is deleted whole, and only when every file in it passes. Anything unclear keeps the folder.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Set
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Literal

# Landing feed folder -> the bronze table that loads it. gtfs_static is not here on purpose: it holds one zip
# per schedule version, and the hourly job re-reads them.
FEEDS: dict[str, str] = {
    "bus_trip_updates": "bus_trip_updates",
    "bus_vehicle_positions": "bus_vehicle_positions",
    "rail_arrivals": "rail_arrivals",
    "_heartbeat": "collector_heartbeats",
}
KEEP_DAYS = 30

_DAY = re.compile(r"^dt=(\d{4}-\d{2}-\d{2})/?$")


def folder_day(name: str) -> date | None:
    """`dt=2026-11-15` -> 2026-11-15; anything else is not a day folder."""
    m = _DAY.match(name)
    if not m:
        return None
    try:
        return date.fromisoformat(m.group(1))
    except ValueError:
        return None


def file_key(path: str, feed: str) -> str | None:
    """The part of a path that names the file inside the landing volume, e.g.
    `rail_arrivals/dt=2026-11-15/024804Z_6bcfac4cca4dc9b0.json.gz`.

    Bronze stores `/Volumes/...` paths and Auto Loader's state stores cloud storage paths, so both are
    compared on this key.
    """
    marker = f"/{feed}/dt="
    at = path.find(marker)
    return None if at < 0 else path[at + 1 :]


@dataclass(frozen=True)
class DayFolder:
    feed: str
    day: date
    files: frozenset[str]
    size_bytes: int


@dataclass(frozen=True)
class Loaded:
    """What one bronze table holds, as file keys."""

    with_rows: Set[str]
    # Every file Auto Loader has picked up, including empty snapshots that produce no bronze rows.
    seen: Set[str]
    failed: Set[str]
    newest_day: date | None


Action = Literal["delete", "keep"]


@dataclass(frozen=True)
class Decision:
    feed: str
    day: date
    files: int
    size_bytes: int
    not_loaded: int
    failed: int
    action: Action
    reason: str


def decide(folder: DayFolder, loaded: Loaded, today: date, keep_days: int = KEEP_DAYS) -> Decision:
    def result(action: Action, reason: str, not_loaded: int = 0, failed: int = 0) -> Decision:
        return Decision(
            folder.feed, folder.day, len(folder.files), folder.size_bytes, not_loaded, failed, action, reason
        )

    # Strictly older, so even the folder's last file is at least keep_days old.
    if folder.day >= today - timedelta(days=keep_days):
        return result("keep", f"newer than {keep_days} days")
    # A file Auto Loader saw but that made no rows (an empty overnight snapshot) counts as loaded only once
    # the stream has loaded a later day, so a file still waiting in a batch is never mistaken for done.
    moved_on = loaded.newest_day is not None and loaded.newest_day > folder.day
    done = set(loaded.with_rows) | (set(loaded.seen) if moved_on else set())
    not_loaded = len(folder.files - done)
    if not_loaded:
        return result("keep", f"{not_loaded} file(s) not in bronze yet", not_loaded=not_loaded)
    # Bronze keeps only an error for these, so the file is the only copy a decoder fix could reload.
    failed = len(folder.files & set(loaded.failed))
    if failed:
        return result("keep", f"{failed} file(s) failed to decode; kept for a reload", failed=failed)
    return result("delete", "every file is in bronze")


def to_delete(decisions: Iterable[Decision], max_days: int) -> list[Decision]:
    """Deletable folders from the oldest `max_days` days, so a backlog is worked off over several runs."""
    deletable = sorted((d for d in decisions if d.action == "delete"), key=lambda d: (d.day, d.feed))
    days = sorted({d.day for d in deletable})[:max_days]
    return [d for d in deletable if d.day in days]
