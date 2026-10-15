# ADR-0002: Treat the rail feed's DELAY as seconds behind schedule

- **Status:** Proposed (confirm against the static schedule in S5)
- **Date:** 2026-10-15
- **Related:** RAIL-002, RAIL-003, SPK-010, `design/specs/rail.md`

## Context
MARTA's rail arrivals feed has a `DELAY` field that its documentation doesn't define. `gold.rail_line_health` reports average and 90th-percentile delay from it, so the meaning has to be pinned down, or at least stated.

Observed in 6,660 records sampled from 25 snapshots (2026-10-14/15):
- The format is always `T<integer>S`, e.g. `T564S`, `T-45S`.
- It's present only on live-tracked rows (`IS_REALTIME = true`), never on timetable rows.
- Values range from -1,399 to +689. The median is 26, the 90th percentile 174, and 29% are exactly 0.

## Options considered
1. **Seconds behind schedule (negative = early).** Fits every observation: a duration-style format with an S suffix, only on tracked trains, mostly small positive values, a cluster at zero for on-time trains, and some early trains.
2. **Seconds until arrival.** Ruled out: `WAITING_SECONDS` already carries that, and the two don't match.
3. **Don't use the field.** Safe, but it throws away the only direct lateness signal in the rail feed.

## Decision
Option 1. Silver stores it as `delay_seconds` (an integer, NULL when absent or malformed), and gold describes it as "seconds behind schedule; negative = early", with a link to this ADR.

## Consequences
- Rail delay metrics are only as right as this reading. Until it's confirmed, the dashboard notes that DELAY is MARTA's own figure.
- To confirm it in S5: compare `next_arrival_ts - delay_seconds` with the scheduled rail times in the static GTFS for the same station and trip. If they line up, mark this ADR Accepted.

## How I'd explain it in an interview
"The rail feed had an undocumented DELAY field, so I profiled real data before using it. The values were small, centered near zero, sometimes negative and only on tracked trains, which reads as seconds behind schedule. I wrote that down as a provisional decision with a plan to verify it against the timetable."
