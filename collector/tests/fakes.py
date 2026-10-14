from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime

from transitpulse_collector.deadletter import DeadLetterItem
from transitpulse_collector.retry import PermanentError
from transitpulse_collector.sinks import PutResult
from transitpulse_collector.transport import Response

Scripted = Response | Exception


@dataclass
class Call:
    method: str
    url: str
    headers: Mapping[str, str]


class FakeTransport:
    """Answers by URL substring; each route plays its script in order and repeats the last step."""

    def __init__(self, routes: dict[str, list[Scripted]]) -> None:
        self.routes = routes
        self.calls: list[Call] = []

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout: float,
        body: bytes | None = None,
        max_bytes: int | None = None,
    ) -> Response:
        self.calls.append(Call(method, url, headers))
        for fragment, script in self.routes.items():
            if fragment in url:
                step = script.pop(0) if len(script) > 1 else script[0]
                if isinstance(step, Exception):
                    raise step
                if max_bytes is not None and len(step.body) > max_bytes:
                    raise PermanentError(f"response over {max_bytes} bytes", step.status)
                return step
        raise AssertionError(f"unexpected url {url}")

    def calls_to(self, fragment: str) -> list[Call]:
        return [c for c in self.calls if fragment in c.url]


class FakeSink:
    def __init__(self) -> None:
        self.files: dict[str, bytes] = {}
        self.failure: Exception | None = None

    def put(self, path: str, data: bytes) -> PutResult:
        if self.failure is not None:
            raise self.failure
        if path in self.files:
            return "exists"
        self.files[path] = data
        return "created"


@dataclass
class FakeDeadLetter:
    items: list[DeadLetterItem] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    def save(self, target_path: str, data: bytes, error: str) -> None:
        self.items.append(DeadLetterItem(f"k{len(self.items)}", target_path, data))
        self.errors.append(error)

    def pending(self, limit: int) -> list[DeadLetterItem]:
        return list(self.items[:limit])

    def remove(self, item: DeadLetterItem) -> None:
        self.items.remove(item)

    def count(self) -> int:
        return len(self.items)


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.t += seconds


FIXED_NOW = datetime(2026, 10, 14, 20, 30, 0, tzinfo=UTC)
