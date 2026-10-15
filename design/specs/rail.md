# Rail arrivals: bronze to gold (slice S3)

## Outcome
Rail reliability per line, direction, station and hour, built from the rail arrivals the collector lands every 2 minutes. This is the first full path from landing files to a gold table, and later slices copy its pattern.

## Users and systems
- **Inputs:** `landing/rail_arrivals/dt=*/*.json.gz` (collector spec R-003).
- **Outputs:** `bronze.rail_arrivals`, `silver.rail_arrivals`, `silver.rail_arrivals_quarantine`, `gold.rail_line_health`.
- **Runs in:** the `transitpulse` Lakeflow pipeline, triggered hourly by the `transitpulse-refresh` job (paused in dev).

## Requirements

### R-001: Bronze keeps what MARTA sent (RAIL-001)
WHEN a new rail file lands THE SYSTEM SHALL load each record once, with every field as text, plus `_rescued_data` (unexpected fields), `_source_file`, `_file_modified_at` and `_ingested_at`. Files already loaded are never read again (Auto Loader checkpoint).

### R-002: Silver is typed, cleaned and in UTC (RAIL-002)
THE SYSTEM SHALL produce one row per distinct published prediction, with these columns:
- `record_id`: SHA-256 of all source fields
- `train_id`: NULL for timetable rows
- `line`, `direction`: upper-cased
- `station`, `destination`
- `is_realtime`
- `event_ts`: `EVENT_TIME` parsed as Atlanta time, converted to UTC
- `next_arrival_ts`: `NEXT_ARR` given the event's date, plus one day if it would otherwise fall more than 12 hours before the event; UTC
- `waiting_seconds`
- `delay_seconds`: from `T<n>S`; meaning per ADR-0002
- `latitude`, `longitude`
- `hour_local`, `service_date`: a service day runs 03:00–03:00 Atlanta time
- audit columns

### R-003: Re-runs never duplicate (RAIL-002)
WHEN the same record arrives again (re-landed file, replay, reprocessing) THE SYSTEM SHALL keep one row per `record_id` (Auto CDC, SCD Type 1).

### R-004: Bad rows are quarantined, not dropped (RAIL-002, PLAT-007)
WHEN a row breaks a rule THE SYSTEM SHALL write it to `silver.rail_arrivals_quarantine` with `_failed_rules` and `_quarantined_at`, and keep it out of `silver.rail_arrivals`.

Rules:
- `valid_line`: RED, GOLD, BLUE or GREEN
- `valid_direction`: N, S, E or W
- `has_station`
- `event_time_parsed`
- `next_arrival_parsed`
- `waiting_not_negative`
- `realtime_has_train`

The same rules are pipeline expectations (warn), so pass rates appear in the event log.

### R-005: Gold rail health (RAIL-003)
THE SYSTEM SHALL maintain `gold.rail_line_health`, one row per `service_date × line × direction × station × hour_local`, with:
- `predictions`
- `realtime_predictions`
- `trains_seen`
- `avg_delay_seconds`
- `p90_delay_seconds`
- `longest_wait_seconds`

## Constraints and assumptions
- **Unparseable input:** Spark 4 runs in ANSI mode, so unparseable text must use `try_` functions (it becomes NULL, then gets quarantined) instead of failing the pipeline.
- **Empty overnight files:** files from roughly 02:00–04:30 Atlanta time hold an empty list because no trains run, so they add no rows.
- **Repeats are rare:** MARTA re-stamps `EVENT_TIME` on every refresh, so exact repeats rarely happen. R-003 protects re-runs.
- **Stored types:** tables store UTC `TIMESTAMP`s only. Local time is exposed as `hour_local` and `service_date`.

## Verification
- **V-001:** unit tests in `tests/unit/test_rail.py` and `test_common.py` cover:
  - daylight saving time
  - arrivals after midnight
  - service-day cutoff
  - timetable rows
  - negative delay
  - fingerprints
  - quarantine reasons
- **V-002:** dev run 2026-10-15:
  - 248 files → 80,073 bronze rows → 80,073 silver rows, 0 quarantined → 1,071 gold rows
  - no arrival earlier than its event
  - a full update took about 1 minute

## Approval
- Not ⚠; built under the slice rules.
