from __future__ import annotations

import gzip
import hashlib
from datetime import datetime


def sha16(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()[:16]


def gzip_bytes(raw: bytes) -> bytes:
    # mtime=0 keeps the output identical for identical input.
    return gzip.compress(raw, compresslevel=6, mtime=0)


def realtime_path(root: str, feed: str, ts: datetime, digest: str, ext: str) -> str:
    return f"{root}/{feed}/dt={ts:%Y-%m-%d}/{ts:%H%M%S}Z_{digest}.{ext}.gz"


def heartbeat_path(root: str, ts: datetime, run_id: str) -> str:
    return f"{root}/_heartbeat/dt={ts:%Y-%m-%d}/{ts:%H%M%S}Z_{run_id}.json"


def static_path(root: str, digest: str) -> str:
    # Named by content only, so an unchanged schedule maps to a file that already exists.
    return f"{root}/gtfs_static/{digest}.zip"
