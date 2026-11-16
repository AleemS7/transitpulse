from datetime import date

import pytest

from transitpulse.ops.retention import DayFolder, Loaded, decide, file_key, folder_day, to_delete

TODAY = date(2026, 11, 16)
OLD = date(2026, 10, 15)
FEED = "bus_trip_updates"


def folder(day: date = OLD, names: tuple[str, ...] = ("a.pb.gz", "b.pb.gz"), feed: str = FEED) -> DayFolder:
    return DayFolder(feed, day, frozenset(f"{feed}/dt={day}/{n}" for n in names), 1000)


def loaded(
    rows: set[str] | None = None,
    seen: set[str] | None = None,
    failed: set[str] | None = None,
    newest: date | None = TODAY,
) -> Loaded:
    return Loaded(rows or set(), seen or set(), failed or set(), newest)


def key(name: str, day: date = OLD) -> str:
    return f"{FEED}/dt={day}/{name}"


def test_folder_day_parses_only_day_folders() -> None:
    assert folder_day("dt=2026-11-15") == date(2026, 11, 15)
    assert folder_day("dt=2026-11-15/") == date(2026, 11, 15)
    assert folder_day("dt=2026-13-01") is None
    assert folder_day("f4a36309f42858d3.zip") is None
    assert folder_day("_checkpoint") is None


@pytest.mark.parametrize(
    "path",
    [
        "/Volumes/workspace/bronze/landing/bus_trip_updates/dt=2026-10-15/a.pb.gz",
        "dbfs:/Volumes/workspace/bronze/landing/bus_trip_updates/dt=2026-10-15/a.pb.gz",
        "dbstorage-prod-x/uc/1/2/__unitystorage/catalogs/c/volumes/v/bus_trip_updates/dt=2026-10-15/a.pb.gz",
    ],
)
def test_bronze_and_auto_loader_paths_map_to_the_same_key(path: str) -> None:
    assert file_key(path, FEED) == "bus_trip_updates/dt=2026-10-15/a.pb.gz"


def test_file_key_ignores_other_feeds() -> None:
    assert file_key("/Volumes/w/b/landing/rail_arrivals/dt=2026-10-15/a.json.gz", FEED) is None


def test_a_recent_day_is_kept_even_when_loaded() -> None:
    f = folder(day=date(2026, 10, 20))
    d = decide(f, loaded(rows=set(f.files)), TODAY)
    assert (d.action, d.reason) == ("keep", "newer than 30 days")


def test_the_boundary_day_is_kept_and_the_day_before_goes() -> None:
    boundary = folder(day=date(2026, 10, 17))
    older = folder(day=date(2026, 10, 16))
    assert decide(boundary, loaded(rows=set(boundary.files)), TODAY).action == "keep"
    assert decide(older, loaded(rows=set(older.files)), TODAY).action == "delete"


def test_an_old_day_fully_in_bronze_is_deleted() -> None:
    f = folder()
    d = decide(f, loaded(rows=set(f.files)), TODAY)
    assert (d.action, d.files, d.not_loaded) == ("delete", 2, 0)


def test_a_missing_file_keeps_the_whole_day() -> None:
    d = decide(folder(), loaded(rows={key("a.pb.gz")}), TODAY)
    assert (d.action, d.not_loaded) == ("keep", 1)
    assert d.reason == "1 file(s) not in bronze yet"


def test_an_empty_snapshot_counts_once_the_stream_has_moved_past_the_day() -> None:
    # An empty overnight snapshot makes no bronze rows; Auto Loader's state still lists it.
    state = loaded(rows={key("a.pb.gz")}, seen={key("b.pb.gz")}, newest=date(2026, 10, 16))
    assert decide(folder(), state, TODAY).action == "delete"


def test_a_file_only_seen_does_not_count_while_the_stream_is_still_on_that_day() -> None:
    d = decide(folder(), loaded(rows={key("a.pb.gz")}, seen={key("b.pb.gz")}, newest=OLD), TODAY)
    assert (d.action, d.not_loaded) == ("keep", 1)


def test_nothing_counts_as_seen_when_bronze_is_empty() -> None:
    f = folder()
    d = decide(f, loaded(seen=set(f.files), newest=None), TODAY)
    assert (d.action, d.not_loaded) == ("keep", 2)


def test_a_file_that_failed_to_decode_keeps_the_day_for_a_reload() -> None:
    f = folder()
    d = decide(f, loaded(rows=set(f.files), failed={key("b.pb.gz")}), TODAY)
    assert (d.action, d.failed) == ("keep", 1)
    assert "failed to decode" in d.reason


def test_an_empty_day_folder_is_deleted() -> None:
    d = decide(folder(names=()), loaded(), TODAY)
    assert (d.action, d.files) == ("delete", 0)


def test_to_delete_takes_the_oldest_days_first_across_feeds() -> None:
    days = [date(2026, 10, 14), date(2026, 10, 15), date(2026, 10, 16)]
    decisions = [
        decide(f, loaded(rows=set(f.files)), TODAY)
        for day in days
        for f in (folder(day=day), folder(day=day, feed="rail_arrivals"))
    ]
    decisions.append(decide(folder(day=date(2026, 10, 13)), loaded(), TODAY))
    chosen = to_delete(decisions, max_days=2)
    assert [(d.day, d.feed) for d in chosen] == [
        (date(2026, 10, 14), FEED),
        (date(2026, 10, 14), "rail_arrivals"),
        (date(2026, 10, 15), FEED),
        (date(2026, 10, 15), "rail_arrivals"),
    ]
