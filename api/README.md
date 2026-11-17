# TransitPulse API

HTTP API over the gold tables, deployed as a Databricks App by the bundle (`resources/api.yml`). It queries the SQL warehouse as the app's own service principal, which can only use the warehouse, `SELECT` the tables listed there, and read and acknowledge `ops.incidents`. Acknowledging an incident is the only write.

| Endpoint | Returns |
|---|---|
| `GET /health` | `ok`, or 503 if the warehouse doesn't answer |
| `GET /routes` | current routes (paged) |
| `GET /routes/{route_id}/delays?start&end` | on-time performance by day and hour (default: last 7 days, max 92) |
| `GET /stops/{stop_id}/delays?start&end` | delay per day at a stop |
| `GET /rail/lines/{line}/health?service_date` | delay per station for RED, GOLD, BLUE or GREEN (default: latest day) |
| `GET /ghost-trips?service_date&route_id` | scheduled trips that were cancelled or never seen |
| `GET /kpis/daily?start&end` | headline numbers per day |
| `POST /ask` | a Genie answer with the SQL and rows behind it |
| `GET /incidents?status` | incidents the triage step proposed, newest first (paged) |
| `GET /incidents/{id}` | one incident with its evidence and every tool result behind it |
| `POST /incidents/{id}/ack` | marks a proposed incident acknowledged by the signed-in user (409 if already done) |

Opening the app (`/`) shows the incidents page (`/ui/incidents.html`): the list, one incident's evidence with the tool results it cites, and an Acknowledge button that calls the endpoint above as the signed-in user. It is plain HTML and JavaScript on the same JSON endpoints, sets every value as text, and is served with a Content-Security-Policy that only allows its own files.

Every success is `{"data": ..., "meta": {...}}`, and every error is `{"error": {"code", "message"}}`. Paged lists take `limit` (1–500) and `offset`, and return `meta.next_offset`, which is `null` on the last page. The interactive docs are at `/docs`, and the schema is in `samples/openapi.json`.

Run it locally with in-memory data:

```
uv run uvicorn transitpulse_api.main:app --reload
```

Set `TP_BACKEND=databricks` plus `TP_WAREHOUSE_ID` (and Databricks credentials) to read real tables.

After changing dependencies, run `../scripts/export-requirements.ps1`: Databricks Apps installs from `requirements.txt`.

Uses MARTA open data.
