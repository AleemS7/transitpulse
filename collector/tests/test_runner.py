import gzip
import json

import pytest

from tests.fakes import FIXED_NOW, FakeClock, FakeDeadLetter, FakeSink, FakeTransport
from transitpulse_collector.retry import PermanentError, RetryableError
from transitpulse_collector.runner import Collector, Deps
from transitpulse_collector.settings import Settings
from transitpulse_collector.transport import Response

RAIL_KEY = "secret-rail-key"
ROOT = "/Volumes/workspace/bronze/landing"


def make(routes: dict, sink: FakeSink | None = None, deadletter: FakeDeadLetter | None = None):
    clock = FakeClock()
    transport = FakeTransport(routes)
    sink = sink or FakeSink()
    deadletter = deadletter or FakeDeadLetter()
    deps = Deps(
        settings=Settings(rail_api_key=RAIL_KEY, landing_root=ROOT),
        transport=transport,
        sink=sink,
        deadletter=deadletter,
        clock=clock,
        sleep=clock.sleep,
        now=lambda: FIXED_NOW,
    )
    return Collector(deps), transport, sink, deadletter


def healthy(vehicle_pb: bytes, trip_updates_pb: bytes, rail_json: bytes) -> dict:
    return {
        "vehiclepositions.pb": [Response(200, vehicle_pb)],
        "tripupdates.pb": [Response(200, trip_updates_pb)],
        "traindata": [Response(200, rail_json)],
    }


def by_feed(hb):
    return {f.feed: f for f in hb.feeds}


def test_healthy_run_lands_three_files_and_a_heartbeat(vehicle_pb, trip_updates_pb, rail_json) -> None:
    collector, transport, sink, _ = make(healthy(vehicle_pb, trip_updates_pb, rail_json))
    hb = collector.run_realtime()

    feeds = by_feed(hb)
    assert {f.status for f in feeds.values()} == {"ok"}
    assert feeds["bus_vehicle_positions"].entity_count == 10
    assert feeds["rail_arrivals"].entity_count == 10
    assert feeds["bus_trip_updates"].path.startswith(f"{ROOT}/bus_trip_updates/dt=")
    assert feeds["rail_arrivals"].path.startswith(f"{ROOT}/rail_arrivals/dt=")

    heartbeats = [p for p in sink.files if "/_heartbeat/" in p]
    assert len(sink.files) == 4 and len(heartbeats) == 1
    record = json.loads(sink.files[heartbeats[0]])
    assert record["run_id"] == hb.run_id and len(record["feeds"]) == 3

    landed = sink.files[feeds["bus_vehicle_positions"].path]
    assert gzip.decompress(landed) == vehicle_pb
    assert all(c.headers["User-Agent"].startswith("transitpulse-collector/") for c in transport.calls)


def test_one_feed_failing_does_not_stop_the_others(vehicle_pb, trip_updates_pb) -> None:
    timeout = RetryableError(f"ReadTimeout: https://host/traindata?apiKey={RAIL_KEY}")
    collector, _, sink, _ = make(
        {
            "vehiclepositions.pb": [Response(200, vehicle_pb)],
            "tripupdates.pb": [Response(200, trip_updates_pb)],
            "traindata": [timeout],
        }
    )
    hb = collector.run_realtime()

    feeds = by_feed(hb)
    assert feeds["bus_vehicle_positions"].status == "ok"
    assert feeds["bus_trip_updates"].status == "ok"
    assert feeds["rail_arrivals"].status == "failed"
    assert feeds["rail_arrivals"].fetch_attempts == 3
    assert RAIL_KEY not in feeds["rail_arrivals"].error
    assert all(RAIL_KEY.encode() not in data for data in sink.files.values())


def test_transient_errors_are_retried(vehicle_pb, trip_updates_pb, rail_json) -> None:
    routes = healthy(vehicle_pb, trip_updates_pb, rail_json)
    routes["vehiclepositions.pb"] = [Response(503, b""), Response(503, b""), Response(200, vehicle_pb)]
    collector, _, _, _ = make(routes)
    feed = by_feed(collector.run_realtime())["bus_vehicle_positions"]
    assert (feed.status, feed.fetch_attempts) == ("ok", 3)


def test_client_errors_are_not_retried(vehicle_pb, trip_updates_pb, rail_json) -> None:
    routes = healthy(vehicle_pb, trip_updates_pb, rail_json)
    routes["traindata"] = [Response(403, b"forbidden")]
    collector, transport, _, _ = make(routes)
    feed = by_feed(collector.run_realtime())["rail_arrivals"]
    assert (feed.status, feed.http_status, feed.fetch_attempts) == ("failed", 403, 1)
    assert len(transport.calls_to("traindata")) == 1


def test_same_snapshot_twice_is_unchanged(vehicle_pb, trip_updates_pb, rail_json) -> None:
    collector, _, sink, _ = make(healthy(vehicle_pb, trip_updates_pb, rail_json))
    collector.run_realtime()
    second = by_feed(collector.run_realtime())
    assert {f.status for f in second.values()} == {"unchanged"}
    assert len([p for p in sink.files if "/bus_trip_updates/" in p]) == 1
    assert len([p for p in sink.files if "/rail_arrivals/" in p]) == 1


def test_unparseable_payload_is_still_landed_and_flagged(trip_updates_pb, rail_json) -> None:
    routes = {
        "vehiclepositions.pb": [Response(200, b"<html>maintenance</html>")],
        "tripupdates.pb": [Response(200, trip_updates_pb)],
        "traindata": [Response(200, rail_json)],
    }
    collector, _, sink, _ = make(routes)
    feed = by_feed(collector.run_realtime())["bus_vehicle_positions"]
    assert feed.status == "ok" and feed.unparsed
    assert "/dt=2026-10-14/203000Z_" in feed.path
    assert feed.path in sink.files


def test_oversized_and_empty_payloads_fail(trip_updates_pb, rail_json) -> None:
    routes = {
        "vehiclepositions.pb": [Response(200, b"x" * (5 * 1024 * 1024 + 1))],
        "tripupdates.pb": [Response(200, trip_updates_pb)],
        "traindata": [Response(200, b"")],
    }
    collector, _, _, _ = make(routes)
    feeds = by_feed(collector.run_realtime())
    assert feeds["bus_vehicle_positions"].status == "failed"
    assert feeds["rail_arrivals"].status == "failed"
    assert "empty" in feeds["rail_arrivals"].error


def test_databricks_down_dead_letters_everything_then_replays(vehicle_pb, trip_updates_pb, rail_json) -> None:
    sink, deadletter = FakeSink(), FakeDeadLetter()
    sink.failure = PermanentError("upload: HTTP 401", 401)
    collector, _, _, _ = make(healthy(vehicle_pb, trip_updates_pb, rail_json), sink, deadletter)

    hb = collector.run_realtime()
    assert {f.status for f in hb.feeds} == {"deadlettered"}
    assert len(deadletter.items) == 4
    assert any("/_heartbeat/" in i.target_path for i in deadletter.items)
    expected_paths = {i.target_path for i in deadletter.items}

    sink.failure = None
    hb2 = collector.run_realtime()
    assert hb2.replayed == 4
    assert expected_paths <= set(sink.files)
    assert deadletter.items == []
    assert hb2.deadletter_pending == 0


def test_replay_stops_at_first_failure() -> None:
    sink, deadletter = FakeSink(), FakeDeadLetter()
    deadletter.save("/Volumes/a", b"1", "e")
    deadletter.save("/Volumes/b", b"2", "e")
    sink.failure = RetryableError("upload: HTTP 503", 503)
    collector, _, _, _ = make({"": [RetryableError("down")]}, sink, deadletter)
    assert collector._replay(deadline=100) == 0
    assert len(deadletter.items) == 2


def test_static_schedule_lands_once_and_records_last_modified() -> None:
    zip_bytes = b"PK\x03\x04 pretend zip"
    response = Response(200, zip_bytes, {"Last-Modified": "Tue, 18 Aug 2026 18:28:00 GMT"})
    collector, _, sink, _ = make({"google_transit.zip": [response]})

    first = collector.run_static().feeds[0]
    second = collector.run_static().feeds[0]
    assert first.status == "ok" and second.status == "unchanged"
    assert first.last_modified == "Tue, 18 Aug 2026 18:28:00 GMT"
    assert first.path.startswith(f"{ROOT}/gtfs_static/") and first.path.endswith(".zip")
    assert sink.files[first.path] == zip_bytes


@pytest.mark.parametrize("status", [200, 404])
def test_heartbeat_is_written_even_when_every_feed_fails(status: int) -> None:
    collector, _, sink, _ = make({"": [Response(status, b"")]})
    hb = collector.run_realtime()
    assert all(f.status == "failed" for f in hb.feeds)
    assert any("/_heartbeat/" in p for p in sink.files)
