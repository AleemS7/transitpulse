# Static schedule (GTFS) and dimensions (slice S5)

## Outcome
MARTA's published timetable, versioned and typed, plus the first dimensions of the gold star schema: `dim_date`, `dim_route` and `dim_stop`. Every delay metric after this slice compares realtime data against these tables.

## Users and systems
- **Input:** `landing/gtfs_static/<sha16>.zip`, landed daily by the collector only when the schedule changes.
- **Job task `extract_gtfs`:** runs before every pipeline refresh and unpacks new zips into the `gtfs_files` volume as `<table>/feed_version=<sha16>/<table>.txt`.
- **Pipeline outputs:**
  - `bronze.gtfs_<table>`, `silver.gtfs_<table>` for routes, trips, stops, stop_times, calendar and calendar_dates
  - `silver.gtfs_feed_versions`
  - `gold.dim_date`, `gold.dim_route`, `gold.dim_stop`

## Requirements

### R-001: Extract each new version once (GTFS-001)
WHEN a zip in `gtfs_static/` has no extracted files yet THE SYSTEM SHALL write its six tables to the versioned folders, through a temporary name so readers never see half a file. Re-running extracts nothing. A zip without routes, trips, stops, stop_times and calendar (or calendar_dates) fails the task.

### R-002: Typed silver per version (GTFS-002)
THE SYSTEM SHALL type each table in silver, keep `feed_version`, and convert:
- GTFS times (which can pass 24:00, e.g. `25:10:00`) to seconds after the start of the service day
- dates (`YYYYMMDD`) to `DATE`
- calendar day flags to booleans
- a blank `location_type` to 0

Quality rules are warn-only: rows are kept and pass rates are recorded. Silently dropping part of the official timetable would be worse than keeping an odd row.

### R-003: Version validity (GTFS-002)
THE SYSTEM SHALL keep `silver.gtfs_feed_versions` with these columns:
- `valid_from`: the earliest calendar `start_date`
- `valid_to`: the next version's `valid_from`, or NULL
- `last_service_date`
- `is_latest`

### R-004: Date dimension (MODEL-001)
THE SYSTEM SHALL keep `gold.dim_date`, with one row per day for 2026–2027:
- `date_key` (yyyyMMdd)
- weekday names and numbers (Monday = 1)
- `is_weekend`
- `is_schedule_exception`: the schedule in effect lists the date in `calendar_dates`, usually a holiday

### R-005: SCD2 route and stop dimensions (MODEL-002)
THE SYSTEM SHALL keep `gold.dim_route` and `gold.dim_stop`, with one row per entity per period its details stayed the same:
- a surrogate key
- `valid_from`, `valid_to`, `is_current`
- `dim_route` adds `transit_mode` (bus, rail, streetcar)

## Verification
- **V-001: extraction tests:**
  - versioned folders
  - idempotent re-run
  - a new version gets its own folders
  - a missing required table fails
- **V-002: transform tests:**
  - times past 24:00
  - malformed times
  - `feed_version` taken from the path, even when Auto Loader already inferred it
- **V-003: the SQL files run on local Spark against two hand-made versions:**
  - version hand-over
  - a renamed route (two rows), a dropped route (not current) and a new route
  - weekend and holiday flags
- **V-004:** a dev run against the real schedule (see Evidence).

## Evidence
- Dev run 2026-10-19: silver row counts match the zip exactly: 86 routes, 45,367 trips, 7,055 stops, 2,076,118 stop_times, 22 calendar rows, 6 calendar_dates.
- 0 unparsed arrival times; 77,800 stop times past midnight, latest 26:11:00 (94,260 s).
- `dim_route`: 81 bus, 4 rail, 1 streetcar, all current. `dim_date`: 730 days; exceptions on 2026-09-07, 2026-11-26 and 2026-11-27.
- Re-running the job extracted 0 new files (idempotent).
- The timetable confirmed ADR-0002 (rail DELAY = seconds behind schedule).
