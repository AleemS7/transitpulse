"""Unpack new GTFS schedule zips into one folder per table, so Auto Loader can read them as CSV.

Each zip in the landing zone is named by its content hash, which becomes the feed_version:
    <out>/<table>/feed_version=<hash>/<table>.txt
Files already extracted are skipped, so running this every hour is cheap and safe.
"""

from __future__ import annotations

import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from zipfile import ZipFile

# The tables this project reads. shapes.txt (map geometry) and agency.txt aren't needed.
TABLES = ("routes", "trips", "stops", "stop_times", "calendar", "calendar_dates")
REQUIRED = ("routes", "trips", "stops", "stop_times")


class InvalidFeedError(Exception):
    pass


@dataclass(frozen=True)
class Extracted:
    feed_version: str
    table: str
    path: Path


def extract_new_feeds(zip_dir: Path, out_dir: Path) -> list[Extracted]:
    extracted: list[Extracted] = []
    for zip_path in sorted(zip_dir.glob("*.zip")):
        version = zip_path.stem
        with ZipFile(zip_path) as feed:
            names = set(feed.namelist())
            missing = [t for t in REQUIRED if f"{t}.txt" not in names]
            if missing or not {"calendar.txt", "calendar_dates.txt"} & names:
                raise InvalidFeedError(f"{zip_path.name} is missing {missing or ['calendar']}")
            for table in TABLES:
                member = f"{table}.txt"
                target = out_dir / table / f"feed_version={version}" / member
                if member not in names or target.exists():
                    continue
                target.parent.mkdir(parents=True, exist_ok=True)
                # Write under a temporary name first so a reader never sees a half-written file.
                partial = target.with_name(f"{member}.partial")
                with feed.open(member) as src, partial.open("wb") as dst:
                    shutil.copyfileobj(src, dst)
                os.replace(partial, target)
                extracted.append(Extracted(version, table, target))
    return extracted
