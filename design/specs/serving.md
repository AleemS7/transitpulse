# Serving gold: dashboard, Genie space and API (slice S9)

## Outcome
Three ways to read the reliability numbers, all from the same gold tables and all deployed by the bundle:
- **A dashboard** to look at.
- **A Genie space** to ask questions in plain English.
- **A read-only HTTP API** for other systems, with `/ask` passing questions to Genie.

## Components
| Resource | File | Notes |
|---|---|---|
| AI/BI dashboard | `dashboards/transitpulse.lvdash.json`, `resources/dashboard.yml` | Queries name tables without a schema; the bundle sets `dataset_schema` to `dev_gold` or `gold`. |
| Genie space | `resources/genie.yml` | Defined inline so table names follow the target. It holds descriptions for 8 gold tables, one instruction block (on-time window, UTC vs `hour_local`, what each ghost-trip reason means, routes by short name), 10 trusted example queries, and joins from the fact to the route and stop dimensions. |
| API | `api/`, `resources/api.yml` | FastAPI on Databricks Apps. Runs as the app's service principal, which gets `CAN_USE` on the warehouse, `CAN_RUN` on the Genie space, and `SELECT` on exactly the gold tables it serves. |

## API requirements

### R-001: One response shape (API-001)
- **Success:** `{"data": ..., "meta": {...}}`. `meta` holds the source table and when the response was generated.
- **Error:** `{"error": {"code", "message"}}` with the matching HTTP status: 400 bad range, 404 unknown route/stop/line, 422 invalid parameter, 501 `/ask` not configured, 502/503 upstream failure.
- **Upstream detail:** never leaks to the caller; it's logged with the request ID.

### R-002: Paging and ranges (API-001)
- **Paging:** lists take `limit` (1–500) and `offset`, and return `next_offset` (`null` on the last page).
- **Date ranges:** default to the last 7 days and are capped at 92.

### R-003: Endpoints (API-002)
- `/health`
- `/routes`
- `/routes/{route_id}/delays`
- `/stops/{stop_id}/delays`
- `/rail/lines/{line}/health`
- `/ghost-trips`
- `/kpis/daily`

Details are in `api/README.md`.

### R-004: Safe SQL (API-002)
- **Values:** every value is a bound parameter.
- **Table names:** the only formatted parts. They come from settings that are validated as identifiers at startup.

### R-005: `/ask` (API-003)
WHEN a question (3–500 characters) is posted THE SYSTEM SHALL ask the Genie space as the app's service principal and return:
- Genie's text answer
- the SQL it ran
- the columns and up to 100 rows of the result
- `meta.grounded = true` only when a query backs the answer

A Genie failure or timeout returns an error, never a made-up answer.

### R-006: Published contract (API-004)
`api/samples/openapi.json` is generated from the code; a test fails if they differ.

### R-007: Incidents page (API-004)
WHEN a signed-in person opens the app THE SYSTEM SHALL show `/ui/incidents.html`, built only on the JSON endpoints above:
- the incidents list, filterable by status;
- one incident's facts, recommended runbook step, evidence statements with the tool result each cites, and the raw tool results;
- an Acknowledge button for a proposed incident, which shows the 409 message when someone got there first.

Incident text comes from an LLM and from pipeline errors, so the page only ever sets it as text, never as HTML (a test checks the script). Its responses carry `Content-Security-Policy: default-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'` and `X-Content-Type-Options: nosniff`.

## Constraints and assumptions
- **Free Edition limits:** at most 3 apps, and app compute can stop after a day of runtime. The prod deploy redeploys and restarts the app, so the API is a demo endpoint, not a service with an SLA.
- **Python 3.11:** Databricks Apps runs Python 3.11 ✱, so `api/` targets 3.11 while the pipeline targets 3.12.
- **Access:** callers need a Databricks identity; Apps put OAuth in front of every request.
