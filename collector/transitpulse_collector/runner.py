from __future__ import annotations

import logging
import time
import uuid
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Literal

from transitpulse_collector import __version__
from transitpulse_collector.deadletter import DeadLetter
from transitpulse_collector.feeds import STATIC_GTFS, Feed, inspect, realtime_feeds
from transitpulse_collector.heartbeat import FeedResult, FeedStatus, Heartbeat
from transitpulse_collector.landing import gzip_bytes, heartbeat_path, realtime_path, sha16, static_path
from transitpulse_collector.retry import (
    PermanentError,
    RetriesExhausted,
    RetryableError,
    RetryPolicy,
    with_retry,
)
from transitpulse_collector.settings import Settings
from transitpulse_collector.sinks import Sink
from transitpulse_collector.transport import Response, Transport, raise_for_status

log = logging.getLogger("transitpulse_collector")

STATIC_TIMEOUT_S = 90.0
STATIC_BUDGET_S = 240.0


def _utcnow() -> datetime:
    return datetime.now(UTC)


@dataclass
class Deps:
    settings: Settings
    transport: Transport
    sink: Sink
    deadletter: DeadLetter
    clock: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep
    now: Callable[[], datetime] = field(default=_utcnow)


class Collector:
    def __init__(self, deps: Deps) -> None:
        self._d = deps
        self._policy = RetryPolicy(max_attempts=deps.settings.max_attempts)

    def run_realtime(self) -> Heartbeat:
        hb, deadline = self._start("realtime", self._d.settings.run_budget_s)
        replay_deadline = self._d.clock() + self._d.settings.run_budget_s / 3
        hb.replayed = self._replay(replay_deadline)
        feeds = realtime_feeds(self._d.settings.rail_api_key)
        with ThreadPoolExecutor(max_workers=len(feeds)) as pool:
            hb.feeds = list(pool.map(lambda f: self._collect_realtime(f, deadline), feeds))
        return self._finish(hb, deadline)

    def run_static(self) -> Heartbeat:
        hb, deadline = self._start("static", STATIC_BUDGET_S)
        hb.feeds = [self._collect_static(STATIC_GTFS, deadline)]
        return self._finish(hb, deadline)

    def _start(self, kind: Literal["realtime", "static"], budget_s: float) -> tuple[Heartbeat, float]:
        hb = Heartbeat(
            run_id=uuid.uuid4().hex[:12],
            kind=kind,
            collector_version=__version__,
            started_at=self._d.now(),
        )
        self._t0 = self._d.clock()
        return hb, self._t0 + budget_s

    def _finish(self, hb: Heartbeat, deadline: float) -> Heartbeat:
        hb.deadletter_pending = self._pending_count()
        hb.finished_at = self._d.now()
        hb.duration_ms = int((self._d.clock() - self._t0) * 1000)
        path = heartbeat_path(self._d.settings.landing_root, hb.started_at, hb.run_id)
        status, _, error = self._store(path, hb.to_json(), deadline)
        for f in hb.feeds:
            log.info(
                "run=%s feed=%s status=%s http=%s bytes=%s error=%s",
                hb.run_id,
                f.feed,
                f.status,
                f.http_status,
                f.bytes_raw,
                f.error,
            )
        log.info(
            "run=%s heartbeat=%s replayed=%s pending=%s",
            hb.run_id,
            status,
            hb.replayed,
            hb.deadletter_pending,
        )
        if error:
            log.error("run=%s heartbeat not landed: %s", hb.run_id, error)
        return hb

    def _collect_realtime(self, feed: Feed, deadline: float) -> FeedResult:
        result = FeedResult(feed=feed.name, status="failed")
        fetched_at = self._d.now()
        try:
            resp, result.fetch_attempts = self._fetch(feed, deadline, self._d.settings.fetch_timeout_s)
        except (RetriesExhausted, PermanentError) as exc:
            return self._fetch_failed(result, exc)
        result.http_status = resp.status
        info = inspect(resp.body, feed.format)
        result.feed_ts, result.entity_count = info.feed_ts, info.entity_count
        result.unparsed = not info.parsed
        stored = gzip_bytes(resp.body)
        result.bytes_raw, result.bytes_stored = len(resp.body), len(stored)
        result.path = realtime_path(
            self._d.settings.landing_root,
            feed.name,
            info.feed_ts or fetched_at,
            sha16(resp.body),
            feed.format.value,
        )
        result.status, result.upload_attempts, result.error = self._store(result.path, stored, deadline)
        return result

    def _collect_static(self, feed: Feed, deadline: float) -> FeedResult:
        result = FeedResult(feed=feed.name, status="failed")
        try:
            resp, result.fetch_attempts = self._fetch(feed, deadline, STATIC_TIMEOUT_S)
        except (RetriesExhausted, PermanentError) as exc:
            return self._fetch_failed(result, exc)
        result.http_status = resp.status
        result.last_modified = resp.headers.get("Last-Modified")
        result.bytes_raw = result.bytes_stored = len(resp.body)
        result.path = static_path(self._d.settings.landing_root, sha16(resp.body))
        result.status, result.upload_attempts, result.error = self._store(result.path, resp.body, deadline)
        return result

    def _fetch(self, feed: Feed, deadline: float, timeout_s: float) -> tuple[Response, int]:
        headers = {"User-Agent": self._d.settings.user_agent}

        def op() -> Response:
            resp = self._d.transport.request(
                "GET", feed.url, headers=headers, timeout=timeout_s, max_bytes=feed.max_bytes
            )
            raise_for_status(resp, "fetch")
            if not resp.body:
                raise PermanentError("fetch: empty payload", resp.status)
            return resp

        return with_retry(
            op, policy=self._policy, deadline=deadline, clock=self._d.clock, sleep=self._d.sleep
        )

    def _fetch_failed(self, result: FeedResult, exc: RetriesExhausted | PermanentError) -> FeedResult:
        result.http_status = exc.http_status
        result.fetch_attempts = exc.attempts if isinstance(exc, RetriesExhausted) else 1
        result.error = self._redact(str(exc))
        return result

    def _store(self, path: str, data: bytes, deadline: float) -> tuple[FeedStatus, int, str | None]:
        """Upload with retries; anything that still fails goes to the dead-letter store."""

        def op() -> str:
            return self._d.sink.put(path, data)

        try:
            outcome, attempts = with_retry(
                op, policy=self._policy, deadline=deadline, clock=self._d.clock, sleep=self._d.sleep
            )
            return ("ok" if outcome == "created" else "unchanged"), attempts, None
        except (RetriesExhausted, PermanentError) as exc:
            error = self._redact(str(exc))
            attempts = exc.attempts if isinstance(exc, RetriesExhausted) else 1
        try:
            self._d.deadletter.save(path, data, error)
            return "deadlettered", attempts, error
        except Exception as exc:
            log.exception("dead-letter save failed for %s", path)
            return "failed", attempts, self._redact(f"{error}; dead-letter failed: {exc}")

    def _replay(self, deadline: float) -> int:
        try:
            items = self._d.deadletter.pending(self._d.settings.replay_limit)
        except Exception:
            log.exception("could not list dead-letter items")
            return 0
        replayed = 0
        for item in items:
            if self._d.clock() >= deadline:
                break
            try:
                self._d.sink.put(item.target_path, item.data)
            except (RetryableError, PermanentError) as exc:
                log.warning("replay stopped at %s: %s", item.target_path, self._redact(str(exc)))
                break
            self._d.deadletter.remove(item)
            replayed += 1
        return replayed

    def _pending_count(self) -> int | None:
        try:
            return self._d.deadletter.count()
        except Exception:
            log.exception("could not count dead-letter items")
            return None

    def _redact(self, text: str) -> str:
        for secret in self._d.settings.secrets():
            text = text.replace(secret, "***")
        return text[:300]
