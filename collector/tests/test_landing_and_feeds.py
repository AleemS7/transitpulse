import gzip
from datetime import UTC, datetime

from transitpulse_collector.feeds import Format, inspect
from transitpulse_collector.landing import gzip_bytes, heartbeat_path, realtime_path, sha16, static_path

ROOT = "/Volumes/workspace/bronze/landing"


def test_gzip_is_deterministic_and_reversible() -> None:
    raw = b"same payload" * 100
    assert gzip_bytes(raw) == gzip_bytes(raw)
    assert gzip.decompress(gzip_bytes(raw)) == raw


def test_sha16_is_16_hex_chars() -> None:
    digest = sha16(b"abc")
    assert len(digest) == 16
    int(digest, 16)


def test_realtime_path_uses_utc_date_and_time() -> None:
    ts = datetime(2026, 10, 14, 23, 59, 58, tzinfo=UTC)
    assert (
        realtime_path(ROOT, "bus_trip_updates", ts, "0123456789abcdef", "pb")
        == f"{ROOT}/bus_trip_updates/dt=2026-10-14/235958Z_0123456789abcdef.pb.gz"
    )


def test_heartbeat_and_static_paths() -> None:
    ts = datetime(2026, 10, 14, 1, 2, 3, tzinfo=UTC)
    assert heartbeat_path(ROOT, ts, "run1") == f"{ROOT}/_heartbeat/dt=2026-10-14/010203Z_run1.json"
    assert static_path(ROOT, "0123456789abcdef") == f"{ROOT}/gtfs_static/0123456789abcdef.zip"


def test_inspect_gtfs_rt_reads_header_timestamp_and_entities(vehicle_pb: bytes) -> None:
    info = inspect(vehicle_pb, Format.GTFS_RT)
    assert info.parsed
    assert info.entity_count == 10
    assert info.feed_ts is not None and info.feed_ts.tzinfo is UTC


def test_inspect_gtfs_rt_garbage_is_unparsed_not_an_error() -> None:
    info = inspect(b"<html>Service Unavailable</html>", Format.GTFS_RT)
    assert not info.parsed
    assert info.feed_ts is None


def test_inspect_rail_counts_records(rail_json: bytes) -> None:
    info = inspect(rail_json, Format.JSON)
    assert info.parsed and info.entity_count == 10
    assert info.feed_ts is not None and info.feed_ts.tzinfo is UTC


def test_rail_timestamp_is_newest_event_time_converted_from_atlanta_time() -> None:
    payload = (
        b'[{"EVENT_TIME": "10/14/2026 4:35:48 PM"}, {"EVENT_TIME": "10/14/2026 4:36:02 PM"},'
        b' {"EVENT_TIME": "not a time"}, {"STATION": "no time"}]'
    )
    info = inspect(payload, Format.JSON)
    assert info.feed_ts == datetime(2026, 10, 14, 20, 36, 2, tzinfo=UTC)
    assert info.entity_count == 4


def test_rail_timestamp_after_local_midnight_lands_on_the_next_utc_day() -> None:
    info = inspect(b'[{"EVENT_TIME": "1/15/2026 11:30:00 PM"}]', Format.JSON)
    assert info.feed_ts == datetime(2026, 1, 16, 4, 30, 0, tzinfo=UTC)


def test_inspect_rail_rejects_non_array() -> None:
    assert not inspect(b'{"error": "bad key"}', Format.JSON).parsed
    assert not inspect(b"\xff\xfe not json", Format.JSON).parsed
