import pytest

from tests.fakes import FakeClock, FakeTransport
from transitpulse_collector.retry import (
    PermanentError,
    RetriesExhausted,
    RetryableError,
    RetryPolicy,
    with_retry,
)
from transitpulse_collector.settings import Settings, SettingsError
from transitpulse_collector.sinks import DatabricksFilesSink, LocalSink
from transitpulse_collector.transport import Response

POLICY = RetryPolicy(max_attempts=3, base_delay_s=1.0, jitter_s=0.0)


def flaky(failures: int) -> tuple[list[int], object]:
    calls: list[int] = []

    def op() -> str:
        calls.append(1)
        if len(calls) <= failures:
            raise RetryableError("HTTP 503", 503)
        return "done"

    return calls, op


def test_retries_with_exponential_backoff_then_succeeds() -> None:
    clock = FakeClock()
    calls, op = flaky(2)
    result, attempts = with_retry(op, policy=POLICY, deadline=100, clock=clock, sleep=clock.sleep)
    assert (result, attempts, len(calls)) == ("done", 3, 3)
    assert clock.sleeps == [1.0, 2.0]


def test_gives_up_after_max_attempts() -> None:
    clock = FakeClock()
    _, op = flaky(5)
    with pytest.raises(RetriesExhausted) as info:
        with_retry(op, policy=POLICY, deadline=100, clock=clock, sleep=clock.sleep)
    assert info.value.attempts == 3
    assert info.value.http_status == 503


def test_stops_retrying_when_the_deadline_would_pass() -> None:
    clock = FakeClock()
    _, op = flaky(5)
    with pytest.raises(RetriesExhausted) as info:
        with_retry(op, policy=POLICY, deadline=0.5, clock=clock, sleep=clock.sleep)
    assert info.value.attempts == 1
    assert clock.sleeps == []


def test_permanent_errors_are_not_retried() -> None:
    clock = FakeClock()
    calls: list[int] = []

    def op() -> str:
        calls.append(1)
        raise PermanentError("HTTP 403", 403)

    with pytest.raises(PermanentError):
        with_retry(op, policy=POLICY, deadline=100, clock=clock, sleep=clock.sleep)
    assert len(calls) == 1


def test_databricks_sink_puts_without_overwrite_and_treats_409_as_exists() -> None:
    transport = FakeTransport({"/fs/files": [Response(204, b""), Response(409, b"")]})
    sink = DatabricksFilesSink("https://example.cloud.databricks.com/", "tok", transport)
    path = "/Volumes/workspace/bronze/landing/rail_arrivals/dt=2026-10-14/a.json.gz"
    assert sink.put(path, b"x") == "created"
    assert sink.put(path, b"x") == "exists"
    call = transport.calls[0]
    assert call.method == "PUT"
    assert call.url == (
        "https://example.cloud.databricks.com/api/2.0/fs/files"
        "/Volumes/workspace/bronze/landing/rail_arrivals/dt%3D2026-10-14/a.json.gz?overwrite=false"
    )
    assert call.headers["Authorization"] == "Bearer tok"


@pytest.mark.parametrize(("status", "error"), [(503, RetryableError), (401, PermanentError)])
def test_databricks_sink_classifies_failures(status: int, error: type[Exception]) -> None:
    sink = DatabricksFilesSink("https://h", "tok", FakeTransport({"/fs/files": [Response(status, b"")]}))
    with pytest.raises(error):
        sink.put("/Volumes/x", b"x")


def test_local_sink_never_overwrites(tmp_path) -> None:
    sink = LocalSink(tmp_path)
    assert sink.put("/Volumes/a/b.gz", b"one") == "created"
    assert sink.put("/Volumes/a/b.gz", b"two") == "exists"
    assert (tmp_path / "Volumes/a/b.gz").read_bytes() == b"one"


def test_settings_require_rail_key() -> None:
    with pytest.raises(SettingsError):
        Settings.from_env({})
    s = Settings.from_env({"TP_RAIL_API_KEY": "k", "TP_DATABRICKS_HOST": "https://h/"})
    assert s.databricks_host == "https://h"
    assert s.user_agent.startswith("transitpulse-collector/")
