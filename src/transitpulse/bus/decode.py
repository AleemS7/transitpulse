"""GTFS-realtime protobuf files -> plain Python rows. No Spark here, so the decoding is unit-tested directly.

The collector lands each snapshot gzipped. One call decodes one file; a file that can't be read comes back
with an error message and no rows, so the pipeline can quarantine it instead of failing the run.

The GTFS-realtime schema comes in as bytes (gtfs-realtime.desc, the compiled .proto) rather than from the
gtfs-realtime-bindings package, so Spark's Python workers need nothing beyond protobuf itself.
"""

from __future__ import annotations

import gzip
import zlib
from pathlib import Path
from typing import Any

from google.protobuf import descriptor_pb2, descriptor_pool, message_factory
from google.protobuf.message import DecodeError

Row = dict[str, Any]

GZIP_MAGIC = b"\x1f\x8b"

SCHEMA_FILE = Path(__file__).with_name("gtfs-realtime.desc")


def load_schema() -> bytes:
    return SCHEMA_FILE.read_bytes()


def _feed(content: bytes, schema: bytes) -> Any:
    if content[:2] == GZIP_MAGIC:
        content = gzip.decompress(content)
    # A private pool per call: building it is cheap next to a 200 KB snapshot, and nothing is cached
    # in module state that would have to travel to the workers.
    pool = descriptor_pool.DescriptorPool()
    pool.Add(descriptor_pb2.FileDescriptorProto.FromString(schema))
    feed_type = message_factory.GetMessageClass(pool.FindMessageTypeByName("transit_realtime.FeedMessage"))
    msg = feed_type()
    msg.ParseFromString(content)
    return msg


def _opt(msg: Any, field: str) -> Any:
    """The field's value if it was sent, else None (proto2 would otherwise hand back 0 or '')."""
    return getattr(msg, field) if msg.HasField(field) else None


def _enum(msg: Any, field: str) -> str:
    """Name of an enum field's value, e.g. 'CANCELED'. Unset fields give the schema's default."""
    values = msg.DESCRIPTOR.fields_by_name[field].enum_type.values_by_number
    number = getattr(msg, field)
    return str(values[number].name) if number in values else str(number)


def _opt_enum(msg: Any, field: str) -> str | None:
    return _enum(msg, field) if msg.HasField(field) else None


def _trip(trip: Any) -> Row:
    return {
        "trip_id": _opt(trip, "trip_id"),
        "start_date": _opt(trip, "start_date"),
        "start_time": _opt(trip, "start_time"),
        "route_id": _opt(trip, "route_id"),
        "direction_id": _opt(trip, "direction_id"),
        "trip_status": _enum(trip, "schedule_relationship"),
    }


def _vehicle_position(entity: Any) -> Row:
    vp = entity.vehicle
    pos = vp.position
    return {
        "entity_id": entity.id,
        **_trip(vp.trip),
        "vehicle_id": _opt(vp.vehicle, "id"),
        "vehicle_label": _opt(vp.vehicle, "label"),
        "latitude": _opt(pos, "latitude"),
        "longitude": _opt(pos, "longitude"),
        "bearing": _opt(pos, "bearing"),
        "speed": _opt(pos, "speed"),
        "position_epoch": _opt(vp, "timestamp"),
        "current_status": _opt_enum(vp, "current_status"),
        "stop_id": _opt(vp, "stop_id"),
        "occupancy_status": _opt_enum(vp, "occupancy_status"),
    }


def _event(update: Any, field: str) -> tuple[int | None, int | None]:
    if not update.HasField(field):
        return None, None
    event = getattr(update, field)
    return _opt(event, "time"), _opt(event, "scheduled_time")


def _stop_time_update(update: Any) -> Row:
    arrival, scheduled_arrival = _event(update, "arrival")
    departure, _ = _event(update, "departure")
    return {
        "stop_sequence": _opt(update, "stop_sequence"),
        "stop_id": _opt(update, "stop_id"),
        "stop_status": _enum(update, "schedule_relationship"),
        "arrival_epoch": arrival,
        "scheduled_arrival_epoch": scheduled_arrival,
        "departure_epoch": departure,
    }


def _trip_update(entity: Any) -> Row:
    tu = entity.trip_update
    return {
        "entity_id": entity.id,
        **_trip(tu.trip),
        "vehicle_id": _opt(tu.vehicle, "id") if tu.HasField("vehicle") else None,
        "update_epoch": _opt(tu, "timestamp"),
        "stop_time_updates": [_stop_time_update(s) for s in tu.stop_time_update],
    }


def _decode(content: bytes | None, schema: bytes, kind: str) -> Row:
    if not content:
        return {"feed_epoch": None, "decode_error": "empty file", "entities": []}
    try:
        msg = _feed(content, schema)
    except (OSError, EOFError, zlib.error, DecodeError) as err:
        return {"feed_epoch": None, "decode_error": f"{type(err).__name__}: {err}"[:500], "entities": []}
    if kind == "vehicle":
        entities = [_vehicle_position(e) for e in msg.entity if e.HasField("vehicle")]
    else:
        entities = [_trip_update(e) for e in msg.entity if e.HasField("trip_update")]
    return {"feed_epoch": _opt(msg.header, "timestamp"), "decode_error": None, "entities": entities}


def vehicle_positions(content: bytes | None, schema: bytes) -> Row:
    """One snapshot of the vehicle positions feed: header time, error (if any) and one row per vehicle."""
    return _decode(content, schema, "vehicle")


def trip_updates(content: bytes | None, schema: bytes) -> Row:
    """One snapshot of the trip updates feed: one row per trip, each holding its list of stop updates."""
    return _decode(content, schema, "trip")
