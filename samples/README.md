# Samples

Real outputs, exported on 2026-11-16, so the shapes can be seen without a workspace.

| File | What it is |
|---|---|
| `daily_kpis.csv` | `gold.daily_kpis`, every service day collected so far (prod) |
| `otp_route_hour_route_39.csv` | `gold.otp_route_hour` for route 39 on 2026-11-13, one row per Atlanta hour (prod) |
| `fact_stop_arrival_one_trip.csv` | `gold.fact_stop_arrival` for one trip on 2026-11-13: every observed stop arrival with its scheduled time and delay (prod) |
| `incident.json` | One `ops.incidents` record from a replayed schema-drift failure (dev): the agent's evidence, every tool result it cited, and the rules' classification next to it |

In `daily_kpis`, `ghost_trips` counts trips MARTA cancelled plus trips with no realtime trace (`no_bus_seen`). Some routes never appear in MARTA's realtime feed, so `cancelled_trips` is the measure of service MARTA actually dropped.
