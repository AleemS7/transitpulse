# Bus facts and reliability metrics (slice S7)

## Outcome
Bus reliability tables built from observed arrivals (S6) and the timetable (S5):
- on-time performance by route and hour
- delay by stop
- bunching
- ghost trips
- a one-row-per-day KPI table for dashboards and the API

## Users and systems
- **Inputs:**
  - `silver.bus_stop_arrivals`, `silver.bus_trips`, `silver.bus_vehicle_positions`
  - `silver.gtfs_*`, `silver.gtfs_feed_versions`
  - `gold.dim_date`, `gold.dim_route`, `gold.dim_stop`
  - `silver.rail_arrivals`
- **Outputs:**
  - `silver.bus_scheduled_trips`
  - `gold.fact_stop_arrival`, `gold.otp_route_hour`, `gold.stop_delay`, `gold.headway_bunching`, `gold.ghost_trips`, `gold.daily_kpis`
- **Runs in:** the `transitpulse` pipeline. All of these are materialized views written in SQL.

## Star schema

```
dim_date ──┐
dim_route ─┼── fact_stop_arrival   (grain: trip_id × service_date × stop_sequence, observed arrivals only)
dim_stop ──┘
```

The dimensions are SCD Type 2 (ADR-0004). The fact picks the dimension row whose `[valid_from, valid_to)` period contains the service date.

## Requirements

### R-001: Fully collected service days (BUS-008, BUS-009)
THE SYSTEM SHALL count a service day as fully collected when it falls strictly between the first and the latest service day seen in `silver.bus_vehicle_positions`. Measures that need a complete day use only those days:
- `bus_scheduled_trips`
- `ghost_trips`
- `daily_kpis`

### R-002: Scheduled bus trips (BUS-008)
THE SYSTEM SHALL list, for each fully collected day, the bus trips (`route_type = 3`) of the schedule version in effect that day. A trip's service counts as running that day when:
- its weekly calendar covers that weekday and date, or `calendar_dates` adds it (type 1)
- and `calendar_dates` doesn't remove it (type 2)

### R-003: Arrival fact (MODEL-003)
THE SYSTEM SHALL keep one row per observed arrival with these columns:
- keys: `date_key`, `route_sk`, `stop_sk`
- `stop_id` and `direction_id` from the timetable (the realtime stop ID is kept as `realtime_stop_id`)
- `arrival_ts`, `scheduled_arrival_ts`, `delay_seconds`, `hour_local`
- `arrival_status` (ADR-0006)
- `in_timetable`

### R-004: On-time performance by route and hour (BUS-005)
THE SYSTEM SHALL report, for each service day × route × Atlanta hour:
- `arrivals`, `on_time`, `early`, `late`
- `on_time_pct`: on time as a share of arrivals that have a status
- `avg_delay_seconds` and `p90_delay_seconds`

### R-005: Stop delay (BUS-006)
THE SYSTEM SHALL report, for each service day × stop:
- `arrivals` and `routes`
- `avg_delay_seconds` and `p90_delay_seconds`
- `late_pct`
- stop name and location

### R-006: Bunching (BUS-007)
For each arrival THE SYSTEM SHALL use LAG over (service day, route, direction, stop) ordered by arrival time to report:
- `headway_seconds`: the actual gap behind the previous bus
- `scheduled_headway_seconds`: the gap the timetable planned between the same two trips
- `is_bunched`: the actual gap is under 25% of a positive planned gap

### R-007: Ghost trips (BUS-008)
THE SYSTEM SHALL list scheduled trips on fully collected days that either:
- had a latest MARTA status of `CANCELED` (`reason = 'cancelled'`), or
- had no vehicle position and no observed arrival (`reason = 'no_bus_seen'`)

### R-008: Daily KPIs (BUS-009)
THE SYSTEM SHALL report, for each fully collected day:
- trips: `scheduled_trips`, `ghost_trips`, `cancelled_trips`, `ghost_trip_pct`
- bus arrivals: `observed_arrivals`, `bus_on_time_pct`, `bus_avg_delay_seconds`, `bunched_pct`
- rail: `rail_avg_delay_seconds`
- calendar labels: day name, weekend, schedule exception

## Constraints and assumptions
- **Observed means reported:** an observed arrival is MARTA's own reported time (ADR-0005). A stop that was never reported as passed has no row, so arrival counts are coverage, not ridership.
- **Two kinds of ghost trip:** on the first dev days, about 10% of scheduled bus trips were marked `CANCELED`. Another ~30% never appeared in either realtime feed. That 30% is spread evenly across hours and routes, and every trip the feed did list matched the timetable, so it isn't an ID mismatch. "Never appeared" can mean a trip that didn't run, or one that ran with no tracking. The two reasons are kept apart, so reports can show "cancelled" separately from "no realtime trace".
- **Clock times:** the GTFS time `scheduled_start_local` is a labelled local presentation value. It can exceed 24:00 for trips that run after midnight.
