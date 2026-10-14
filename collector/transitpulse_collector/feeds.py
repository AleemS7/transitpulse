from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any
from zoneinfo import ZoneInfo

from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

MB = 1024 * 1024
ATLANTA = ZoneInfo("America/New_York")
RAIL_TIME_FORMAT = "%m/%d/%Y %I:%M:%S %p"


class Format(StrEnum):
    GTFS_RT = "pb"
    JSON = "json"
    ZIP = "zip"


@dataclass(frozen=True)
class Feed:
    name: str
    url: str
    format: Format
    max_bytes: int


@dataclass(frozen=True)
class Inspection:
    feed_ts: datetime | None
    entity_count: int | None
    parsed: bool


GTFS_RT_BASE = "https://gtfs-rt.itsmarta.com/TMGTFSRealTimeWebService"
RAIL_URL = (
    "https://developerservices.itsmarta.com:18096/itsmarta/railrealtimearrivals/developerservices/traindata"
)

STATIC_GTFS = Feed(
    "gtfs_static", "https://itsmarta.com/google_transit_feed/google_transit.zip", Format.ZIP, 100 * MB
)


def realtime_feeds(rail_api_key: str) -> list[Feed]:
    return [
        Feed("bus_vehicle_positions", f"{GTFS_RT_BASE}/vehicle/vehiclepositions.pb", Format.GTFS_RT, 5 * MB),
        Feed("bus_trip_updates", f"{GTFS_RT_BASE}/tripupdate/tripupdates.pb", Format.GTFS_RT, 20 * MB),
        Feed("rail_arrivals", f"{RAIL_URL}?apiKey={rail_api_key}", Format.JSON, 5 * MB),
    ]


def inspect(payload: bytes, fmt: Format) -> Inspection:
    """Read just enough of a payload to name the file and fill the heartbeat. Never raises."""
    if fmt is Format.GTFS_RT:
        msg = gtfs_realtime_pb2.FeedMessage()
        try:
            msg.ParseFromString(payload)
        except DecodeError:
            return Inspection(None, None, parsed=False)
        ts = msg.header.timestamp
        feed_ts = datetime.fromtimestamp(ts, tz=UTC) if ts else None
        return Inspection(feed_ts, len(msg.entity), parsed=True)
    if fmt is Format.JSON:
        try:
            data = json.loads(payload)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return Inspection(None, None, parsed=False)
        if not isinstance(data, list):
            return Inspection(None, None, parsed=False)
        return Inspection(_latest_event_time(data), len(data), parsed=True)
    return Inspection(None, None, parsed=True)


def _latest_event_time(rows: list[Any]) -> datetime | None:
    """Newest EVENT_TIME in a rail payload, in UTC. MARTA sends it as Atlanta local time text."""
    latest: datetime | None = None
    for row in rows:
        raw = row.get("EVENT_TIME") if isinstance(row, dict) else None
        if not isinstance(raw, str):
            continue
        try:
            ts = datetime.strptime(raw, RAIL_TIME_FORMAT).replace(tzinfo=ATLANTA)
        except ValueError:
            continue
        if latest is None or ts > latest:
            latest = ts
    return latest.astimezone(UTC) if latest else None
