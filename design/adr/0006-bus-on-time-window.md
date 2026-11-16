# ADR-0006: A bus is on time from 1 minute early to 5 minutes late

- **Status:** Accepted
- **Date:** 2026-11-15
- **Related:** BUS-005, BUS-006, `design/specs/bus-gold.md`, `src/transitpulse/pipelines/fact_stop_arrival.sql`

## Context
On-time performance needs a window around the scheduled arrival. Agencies choose their own windows, and MARTA's published service standards weren't checked for this project ✱. The window has to be one number set in one place, so every gold table and dashboard agrees.

## Options considered
1. **−1 min / +5 min.** The window most often used by US bus agencies ✱. Early buses are penalised because a rider who arrives on time misses an early bus entirely.
2. **0 / +5 min.** Stricter on early buses. Most "early" arrivals in the data are 1–2 minutes, so this would mostly measure prediction noise.
3. **Report the delay only, no window.** Avoids the choice, but "percent on time" is the number people ask for first.

## Decision
Option 1.
- `fact_stop_arrival.arrival_status` is:
  - `early` when `delay_seconds < -60`
  - `late` when `delay_seconds > 300`
  - `on_time` otherwise
  - NULL when there is no scheduled time to compare with
- Every percentage divides by arrivals that have a status, not by all arrivals.
- `otp_route_hour`, `stop_delay` and `daily_kpis` use only `arrival_status`, so the window lives in one CASE expression.

## Consequences
- **Changing the window** is a one-line change in `fact_stop_arrival.sql`. The materialized views then recompute from silver.
- **If MARTA's own standard differs ✱,** the numbers here won't match its published reports; the window is stated next to every percentage.

## How I'd explain it in an interview
"On-time needs a definition, so I picked the common transit window, one minute early to five late, and put it in exactly one CASE statement in the fact table. Every metric reads that status column, so if the business changes the rule, it's a one-line change and everything downstream agrees."
