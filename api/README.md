# TransitPulse API

Read-only HTTP API over the gold tables, deployed as a Databricks App by the bundle (`resources/api.yml`). It queries the SQL warehouse as the app's own service principal, which can only use the warehouse and `SELECT` the tables listed there.

| Endpoint | Returns |
|---|---|
| `GET /health` | `ok`, or 503 if the warehouse doesn't answer |
| `GET /routes` | current routes (paged) |
| `GET /routes/{route_id}/delays?start&end` | on-time performance by day and hour (default: last 7 days, max 92) |
| `GET /stops/{stop_id}/delays?start&end` | delay per day at a stop |
| `GET /rail/lines/{line}/health?service_date` | delay per station for RED, GOLD, BLUE or GREEN (default: latest day) |
| `GET /ghost-trips?service_date&route_id` | scheduled trips that were cancelled or never seen |
| `GET /kpis/daily?start&end` | headline numbers per day |

Every success is `{"data": ..., "meta": {...}}`, and every error is `{"error": {"code", "message"}}`. Paged lists take `limit` (1–500) and `offset`, and return `meta.next_offset`, which is `null` on the last page. The interactive docs are at `/docs`, and the schema is in `samples/openapi.json`.

Run it locally with in-memory data:

```
uv run uvicorn transitpulse_api.main:app --reload
```

Set `TP_BACKEND=databricks` plus `TP_WAREHOUSE_ID` (and Databricks credentials) to read real tables.

After changing dependencies, run `../scripts/export-requirements.ps1`: Databricks Apps installs from `requirements.txt`.

Uses MARTA open data.
