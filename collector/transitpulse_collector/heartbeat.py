from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Literal

FeedStatus = Literal["ok", "unchanged", "failed", "deadlettered"]


@dataclass
class FeedResult:
    feed: str
    status: FeedStatus
    http_status: int | None = None
    fetch_attempts: int = 0
    upload_attempts: int = 0
    bytes_raw: int | None = None
    bytes_stored: int | None = None
    feed_ts: datetime | None = None
    entity_count: int | None = None
    unparsed: bool = False
    path: str | None = None
    last_modified: str | None = None
    error: str | None = None


@dataclass
class Heartbeat:
    run_id: str
    kind: Literal["realtime", "static"]
    collector_version: str
    started_at: datetime
    finished_at: datetime | None = None
    duration_ms: int | None = None
    replayed: int = 0
    deadletter_pending: int | None = None
    feeds: list[FeedResult] = field(default_factory=list)

    def to_json(self) -> bytes:
        return json.dumps(asdict(self), default=_default, separators=(",", ":")).encode()


def _default(value: Any) -> str:
    if isinstance(value, datetime):
        return value.isoformat()
    raise TypeError(f"not serializable: {type(value).__name__}")
