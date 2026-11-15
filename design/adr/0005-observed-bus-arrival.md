# ADR-0005: Take a bus's observed arrival from stops MARTA already reports as passed

- **Status:** Proposed (to be confirmed against a full service day of data)
- **Date:** 2026-11-15
- **Related:** BUS-002, BUS-004, `design/specs/bus.md`, `src/transitpulse/bus/transforms.py`

## Context
On-time performance needs the time a bus actually reached each stop. GTFS-realtime has no "actual arrival" message: trip updates carry predictions, and vehicle positions carry a location and, optionally, the stop the bus is at.

Two snapshots of MARTA's feeds taken 30 seconds apart (2026-11-15, about 18:15 Atlanta time) showed:
- **Vehicle positions** (164 buses): every bus reports `IN_TRANSIT_TO` with no `stop_id`, so positions can't say when a bus was at a stop.
- **Trip updates** (451 trips, 21,697 stop updates): stops stay in a running trip's update after the bus has passed them. 1,166 stop updates had an arrival time at or before the snapshot's own timestamp.
- **Past times are stable:** of the stops already passed in the first snapshot and still present in the second, 1,084 of 1,109 (98%) kept exactly the same time. Stops passed between the two snapshots moved by under 30 s in 76 of 90 cases.
- **Scheduled time included:** each stop update also carries MARTA's scheduled arrival (`arrival.scheduled_time`).
- **Statuses:** 164 trips were marked `CANCELED`. 8,617 stop updates were `NO_DATA` (no time at all) and 394 were `SKIPPED`.

## Options considered
1. **Latest report of a passed stop.** A stop update whose arrival time is at or before the snapshot timestamp describes something that already happened. Keep the most recent such report per trip, stop and service day. Simple, uses MARTA's own figure, and later corrections win.
2. **Last prediction before the stop disappears.** Common in GTFS-RT studies, but it needs "when did this stop vanish", a stateful comparison across snapshots. With MARTA keeping passed stops in the feed, it adds complexity for no gain.
3. **Infer arrivals from GPS.** Snap positions to stop locations and pick the closest approach. Most independent of MARTA's predictions, but it's map-matching work well beyond this project's scope, and positions only come every 2 minutes.

## Decision
Option 1.
- An observed arrival requires:
  - stop status `SCHEDULED`
  - trip status not `CANCELED` or `DELETED`
  - arrival time ≤ the snapshot's header timestamp
- Silver `bus_stop_arrivals` keeps one row per `trip_id × service_date × stop_sequence`, through Auto CDC sequenced by the snapshot time, so the latest report wins.
- `delay_seconds` is the observed arrival minus MARTA's scheduled arrival from the same update.
- A stop that never shows up as passed gets no observed arrival. It stays NULL rather than a guess.

## Consequences
- **What "observed" means:** it's MARTA's reported arrival, not an independent measurement. Gold tables say so.
- **Coverage:** depends on how long MARTA keeps passed stops in the feed, relative to the collector's 2-minute polling. Coverage per route and hour is measured in gold, so gaps are visible rather than silent.
- **Cancelled trips:** they produce no arrivals, and they feed `ghost_trips` instead.

## How I'd explain it in an interview
"GTFS-realtime only gives predictions, so I had to define what an actual arrival is. I compared snapshots and saw MARTA keeps a stop in the trip update after the bus passes, with a stable time. So my rule is: once a stop's time is in the past relative to the snapshot, that's an observation, and I keep the latest one per trip and stop with a MERGE. Anything never observed stays NULL instead of being guessed."
