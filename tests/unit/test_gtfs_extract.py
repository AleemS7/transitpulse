from pathlib import Path
from zipfile import ZipFile

import pytest

from transitpulse.gtfs.extract import InvalidFeedError, extract_new_feeds

MINIMAL = {
    "routes.txt": "route_id,route_short_name\n1,One\n",
    "trips.txt": "route_id,service_id,trip_id\n1,wk,t1\n",
    "stops.txt": "stop_id,stop_name\ns1,Five Points\n",
    "stop_times.txt": "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
    "t1,25:10:00,25:10:00,s1,1\n",
    "calendar.txt": "service_id,monday,start_date,end_date\nwk,1,20260822,20261211\n",
    "shapes.txt": "shape_id\nx\n",
}


def make_zip(folder: Path, name: str, files: dict[str, str]) -> Path:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / name
    with ZipFile(path, "w") as z:
        for member, text in files.items():
            z.writestr(member, text)
    return path


def test_each_table_lands_in_its_own_versioned_folder(tmp_path: Path) -> None:
    make_zip(tmp_path / "zips", "abc123.zip", MINIMAL)
    out = tmp_path / "out"

    extracted = extract_new_feeds(tmp_path / "zips", out)

    assert {e.table for e in extracted} == {"routes", "trips", "stops", "stop_times", "calendar"}
    assert (out / "stop_times/feed_version=abc123/stop_times.txt").read_text() == MINIMAL["stop_times.txt"]
    assert not (out / "shapes").exists()
    assert not list(out.rglob("*.partial"))


def test_running_again_extracts_nothing(tmp_path: Path) -> None:
    make_zip(tmp_path / "zips", "abc123.zip", MINIMAL)
    extract_new_feeds(tmp_path / "zips", tmp_path / "out")
    assert extract_new_feeds(tmp_path / "zips", tmp_path / "out") == []


def test_a_new_version_gets_its_own_folders(tmp_path: Path) -> None:
    make_zip(tmp_path / "zips", "aaa.zip", MINIMAL)
    extract_new_feeds(tmp_path / "zips", tmp_path / "out")
    make_zip(tmp_path / "zips", "bbb.zip", MINIMAL)

    extracted = extract_new_feeds(tmp_path / "zips", tmp_path / "out")

    assert {e.feed_version for e in extracted} == {"bbb"}
    assert (tmp_path / "out/routes/feed_version=aaa/routes.txt").exists()


def test_a_feed_missing_a_required_table_fails_loudly(tmp_path: Path) -> None:
    broken = {k: v for k, v in MINIMAL.items() if k != "stop_times.txt"}
    make_zip(tmp_path / "zips", "bad.zip", broken)
    with pytest.raises(InvalidFeedError, match="stop_times"):
        extract_new_feeds(tmp_path / "zips", tmp_path / "out")
