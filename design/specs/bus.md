# Bus realtime: bronze and silver (slice S6)

## Outcome
Bus positions, trip statuses and observed stop arrivals in silver. These are the inputs for bus on-time performance, stop delays, bunching and ghost trips in gold (S7).

## Users and systems
- **Inputs:** `landing/bus_vehicle_positions/dt=*/*.pb.gz` and `landing/bus_trip_updates/dt=*/*.pb.gz`, each one gzipped GTFS-realtime snapshot (collector spec R-003).
- **Outputs:**
  - `bronze.bus_vehicle_positions`
  - `bronze.bus_trip_updates`
  - `silver.bus_vehicle_positions`
  - `silver.bus_trips`
  - `silver.bus_stop_arrivals`
  - one `_quarantine` table for each silver table
- **Runs in:** the `transitpulse` Lakeflow pipeline, triggered hourly by the `transitpulse-refresh` job.

## Requirements

### R-001: Decoding never fails the pipeline (BUS-001, BUS-002)
WHEN a landed file is read THE SYSTEM SHALL decode it in plain Python: gunzip if needed, then parse the protobuf.
WHEN a file is empty, isn't gzip, or isn't valid protobuf THE SYSTEM SHALL emit one bronze row with only `_decode_error` set. That row is then quarantined in silver.

### R-002: Bronze keeps what MARTA sent (BUS-001, BUS-002)
THE SYSTEM SHALL load each file once (Auto Loader `binaryFile` with a checkpoint).
- Vehicle positions: one bronze row per vehicle per snapshot.
- Trip updates: one bronze row per trip per snapshot, with the stop updates kept as a list.
- Every bronze row also carries `feed_epoch` (the snapshot header time), `_source_file`, `_file_modified_at` and `_ingested_at`.

Unset protobuf fields are NULL, not 0 or an empty string.

### R-003: Silver vehicle positions (BUS-003)
THE SYSTEM SHALL keep one row per `vehicle_id × position_ts`, through Auto CDC.
- Times are UTC. `hour_local` is the Atlanta hour.
- `service_date` is the trip's GTFS `start_date`.

Rules:
- `decoded`
- `has_vehicle`
- `has_trip`
- `inside_atlanta`: lat 33.0–34.5, lon −85.5 to −83.5
- `not_in_future`: position no more than 60 s after the snapshot

### R-004: Silver trips (BUS-004)
THE SYSTEM SHALL keep one row per `trip_id × service_date` with MARTA's latest trip status (`SCHEDULED`, `CANCELED`, …), the vehicle and the latest `feed_ts`.

Rules:
- `decoded`
- `has_trip`
- `has_feed_time`

### R-005: Observed arrivals (BUS-004, ADR-0005)
WHEN a stop update has status `SCHEDULED`, its trip isn't cancelled or deleted, and its arrival time is at or before the snapshot timestamp, THE SYSTEM SHALL treat it as a report of an arrival.

`silver.bus_stop_arrivals` keeps the latest report per `trip_id × service_date × stop_sequence` (Auto CDC sequenced by `reported_at`). Columns:
- `arrival_ts`
- `scheduled_arrival_ts`
- `delay_seconds`: arrival − scheduled, negative = early
- `hour_local`
- `route_id`, `stop_id`, `vehicle_id`
- audit columns

Rules:
- `has_keys`
- `has_stop`
- `near_schedule`: |delay| ≤ 3 h

### R-006: Backfill is bounded where it needs to be
WHERE the bundle variable `realtime_start` is set, THE SYSTEM SHALL load only files modified after it when a bronze table first starts. dev sets it to keep test runs inside the daily compute quota; prod loads everything collected.

## Constraints and assumptions
- **Python UDF:** gzip and protobuf have no Spark built-ins, so decoding is a Python UDF, called once per file rather than once per row. The decoder is pickled by value because serverless workers don't have the repo on their import path.
- **No pipeline dependency:** the GTFS-realtime schema ships as `gtfs-realtime.desc`, the compiled `.proto`. The driver reads it and passes it into the UDF, so workers need only `protobuf`, which the runtime already includes. Installing `gtfs-realtime-bindings` through the pipeline environment didn't reliably reach the UDF workers. A unit test checks the file against the official bindings.
- **Memory:** a decoded trip-updates snapshot is about 10 MB of Python objects, and serverless caps UDF memory at 1 GB. Trip updates are therefore read at most 25 files per micro-batch.
- **Non-standard direction IDs ✱:** MARTA sends `direction_id` values such as 5 and 11, outside GTFS's 0/1. They're kept as sent; direction comes from the static schedule in gold.
