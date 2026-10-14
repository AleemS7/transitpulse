# Feed collector (slice S1) ⚠

## Outcome
MARTA's live feeds are captured every ~2 minutes, around the clock, into a Databricks landing volume. Gaps happen only when MARTA itself is down, and each gap is explained by a heartbeat record. The collector is the only component that can't be re-run later (MARTA keeps no history), so it is built to keep collecting through Databricks outages and to never lose a payload it has fetched.

## Users and systems
- **MARTA feeds:** bus vehicle positions and trip updates (GTFS-RT protobuf), rail arrivals (JSON, port 18096, API key), static GTFS (zip).
- **Azure Function App** `tp-atl-pr-func-collector-01` (Flex Consumption, Python 3.12), with its storage account, Application Insights and Key Vault.
- **Databricks Files API** writing to the volume `workspace.bronze.landing`.
- **Downstream:** the bronze pipeline (S3, S6) and the `ops` freshness checks (S8) read what lands here.

## In scope / Out of scope
- **In:**
  - realtime timer (3 feeds) and static timer (daily)
  - landing naming and gzip
  - Files API upload
  - retry and timeouts
  - heartbeat
  - dead-letter and replay
  - local run mode
  - fixtures and tests
  - Bicep for the Azure resources
  - landing schema and volume setup script
  - first deploy by hand
- **Out:**
  - protobuf decoding (bronze, S6)
  - service-alerts scraper (deferred)
  - weather (deferred)
  - CI deploy (S4)
  - landing retention (S8)

## Requirements

### R-001: Realtime collection (COL-001)
WHEN the realtime timer fires (every 2 minutes, at second 0) THE SYSTEM SHALL fetch bus vehicle positions, bus trip updates and rail arrivals concurrently, each with its own timeout, and process each one independently.
- Given all three feeds answer / When the run completes / Then three landing files and one heartbeat exist.
- Given the rail API times out / When the run completes / Then the bus files still land and the heartbeat shows rail `failed` with the error.

### R-002: Retry with backoff (COL-001)
WHEN a fetch or upload fails with a timeout, connection error, HTTP 429 or 5xx THE SYSTEM SHALL retry up to 3 attempts in total, with exponential backoff (1 s, 2 s) plus jitter, and SHALL NOT retry other 4xx responses.
- Given HTTP 503 twice then 200 / Then the feed lands, with `attempts: 3`.
- Given HTTP 403 / Then there's no retry, and the heartbeat records `http_status: 403`.

### R-003: Landing files (COL-002)
WHEN a feed payload is fetched THE SYSTEM SHALL gzip it and upload it to `/Volumes/workspace/bronze/landing/{feed}/dt={YYYY-MM-DD}/{HHMMSS}Z_{sha16}.{ext}.gz`, where:
- the time is the feed's own timestamp in UTC: the GTFS-RT header timestamp for bus feeds, and the newest `EVENT_TIME` for rail (sent as Atlanta local time, converted to UTC). If the payload doesn't parse, the fetch time is used. Because the time comes from the content, identical content always gets an identical name.
- `sha16` is the first 16 hex characters of the SHA-256 of the **uncompressed** payload
- `feed` ∈ {`bus_vehicle_positions`, `bus_trip_updates`, `rail_arrivals`}
- `ext` ∈ {`pb`, `json`}

### R-004: Idempotent upload (COL-002)
WHEN the upload target already exists (Files API `AlreadyExists`, verified in SPK-002) THE SYSTEM SHALL treat it as success with status `unchanged`, without overwriting.
- Given MARTA serves the same snapshot twice / Then one file exists, and the second heartbeat entry says `unchanged`.

### R-005: Payload checks (COL-001)
- THE SYSTEM SHALL reject payloads over a size cap (bus vehicle positions 5 MB, trip updates 20 MB, rail 5 MB, static 100 MB) and empty payloads as `failed`.
- THE SYSTEM SHALL parse GTFS-RT payloads only far enough to read `header.timestamp` and the entity count. A payload that doesn't parse is still landed, using the fetch time, and flagged `unparsed: true` in the heartbeat.
- THE SYSTEM SHALL check that rail payloads are a JSON array. Anything else is still landed and flagged `unparsed: true`, the same as an unreadable GTFS-RT payload.

### R-006: Heartbeat (COL-003)
WHEN a run finishes, successful or not, THE SYSTEM SHALL write one JSON heartbeat to `/Volumes/workspace/bronze/landing/_heartbeat/dt={YYYY-MM-DD}/{HHMMSS}Z_{run_id}.json`. It contains:
- `run_id`, `kind` (`realtime` | `static`), `collector_version`, `started_at`, `finished_at`, `duration_ms`
- `deadletter_pending`, `replayed`
- per feed: `status` (`ok` | `unchanged` | `failed` | `deadlettered`), `http_status`, `attempts`, `bytes_raw`, `bytes_gz`, `feed_ts`, `entity_count`, `unparsed`, `path`, `error` (message, max 300 characters)

### R-007: Dead-letter (COL-004)
WHEN an upload to Databricks still fails after retries THE SYSTEM SHALL write the gzipped payload and its target path to the `deadletter` container in the Function's storage account, and mark the feed `deadlettered`.
- This includes the heartbeat itself.
- Fetch failures have no payload, so they are recorded in the heartbeat only.
- Given the Databricks token is revoked / Then every payload and the heartbeat go to dead-letter, and Application Insights logs an error.

### R-008: Replay (COL-004)
WHEN a run starts and the dead-letter container isn't empty THE SYSTEM SHALL replay up to 50 items, oldest first, to their original paths. It SHALL delete each blob after a successful or `AlreadyExists` upload and stop replaying at the first failure.
- Given 3 dead-lettered items and Databricks back up / Then all 3 land at their original paths, and the heartbeat says `replayed: 3`.

### R-009: Static schedule (COL-005)
WHEN the static timer fires (daily, 09:00 UTC) THE SYSTEM SHALL download the GTFS zip and upload it unchanged to `/Volumes/workspace/bronze/landing/gtfs_static/{sha16}.zip` using R-004. An unchanged schedule is therefore a no-op that needs no stored state. The heartbeat records the `Last-Modified` header.

### R-010: Requests identify the client (COL-001)
THE SYSTEM SHALL send `User-Agent: transitpulse-collector/{version} (+repo URL)` on every MARTA request. itsmarta.com returns 403 to the default Python user agent (checked 2026-10-14).

### R-011: Run budget
THE SYSTEM SHALL finish a realtime run in under 60 seconds: per-feed timeout of 20 s per attempt, and a run-level deadline after which remaining retries are skipped and recorded.

### R-012: Secrets (COL-007)
THE SYSTEM SHALL read the Databricks host, token and MARTA rail key from app settings backed by Key Vault references, through the Function's managed identity. It SHALL never log them; the rail URL is logged with the key redacted.

### R-013: Local mode (COL-007)
- `uv run python -m transitpulse_collector run-once` SHALL perform one realtime run with the same code, reading settings from environment variables.
- `--sink local --out <dir>` writes to a folder instead of Databricks.
- `loop --interval 120` repeats until stopped.

### R-014: Offline tests (COL-006)
THE SYSTEM SHALL have unit tests that use recorded fixtures and fake HTTP, sink and dead-letter implementations. They make no network calls and cover R-001 to R-011.

## Constraints and assumptions
- Azure Functions Flex Consumption free grant: 250k executions and 100k GB-s a month. Local runs took 1.3–2.1 s (2026-10-14), so 720 runs a day at 512 MB is about 21.6k executions and roughly 25k GB-s a month, well inside the grant. Re-measure after deploy.
- About 0.9 GB/day raw, mostly trip updates; gzip is expected to cut it 3–5x ✱.
- The rail API worked with a placeholder key on 2026-10-14; the real key is used anyway.
- The Databricks token lifetime is 730 days; its expiry date goes into the heartbeat config for the `ops` alert.
- Times are UTC everywhere in this component.
- No personal data is collected.

## Open questions and decisions
- D-001: Raw bytes land unchanged and decoding happens in bronze, so the decode logic can be fixed and re-run without data loss.
- D-002: Dead-letters go to Azure Blob, not the volume, because Databricks being unreachable is the failure they cover.
- D-003: Files API calls use `requests` behind a small `Sink` interface rather than `databricks-sdk`, to keep the deploy package and cold start small.
- D-004: The landing schema and volume are created once by `sql/setup/landing.sql`, not by the bundle, so a `bundle destroy` can never delete collected history.
- D-005: Telemetry uses the Functions host's built-in Application Insights logging; the OpenTelemetry distro isn't added (size and cold start).
- D-006: An external collector even though Databricks egress is open today (ADR-0001).
- D-007: Secrets are stored in Key Vault and read through the Function's managed identity; region East US.
- D-008: When no Blob dead-letter is configured (laptop runs against Databricks), dead-letters go to a local `.deadletter/` folder.

## Design summary
```
collector/
  function_app.py                 two timer triggers → runner (thin)
  host.json  pyproject.toml  uv.lock  requirements.txt (exported)  local.settings.example.json
  transitpulse_collector/
    settings.py      typed settings from env
    feeds.py         feed definitions and payload inspection (timestamp, entity count)
    landing.py       gzip, sha, path naming
    retry.py         backoff with a run deadline
    transport.py     HTTP with timeouts and a size cap
    sinks.py         Sink protocol: DatabricksFilesSink, LocalSink
    deadletter.py    DeadLetter protocol: BlobDeadLetter, LocalDeadLetter
    wiring.py        builds a collector from settings
    heartbeat.py     run record model
    runner.py        one realtime run / one static run (replay → fetch → land → heartbeat)
    __main__.py      run-once / loop CLI
  tests/  fixtures/ (trimmed real payloads)  fakes.py  test_*.py
infra/collector.bicep             storage, Function App (Flex), App Insights, Key Vault, role assignments
sql/setup/landing.sql             CREATE SCHEMA bronze; CREATE VOLUME bronze.landing
```
- **Errors:** configuration errors (missing settings) fail the run loudly; feed errors are recorded per feed.
- **Security:** managed identity for Blob and Key Vault; secrets only via Key Vault references.
- **Deploy:** `az deployment group create` for the Bicep, then `func azure functionapp publish`. CI takes over in S4.

## Tasks
- T-001: collector package scaffold, settings, ruff/mypy/pytest -> R-012, R-014
- T-002: http fetch with retry and run deadline -> R-001, R-002, R-010, R-011
- T-003: GTFS-RT header parse and payload checks -> R-005
- T-004: landing naming and gzip -> R-003
- T-005: Databricks Files sink with AlreadyExists handling -> R-004
- T-006: heartbeat -> R-006
- T-007: Blob dead-letter and replay -> R-007, R-008
- T-008: static run -> R-009
- T-009: CLI local mode -> R-013
- T-010: recorded fixtures and tests -> R-014
- T-011: `sql/setup/landing.sql`, Bicep, first deploy, port 18096 check from Azure -> R-012
- T-012: runbook `design/runbooks/collector-failure.md`

## Verification
- V-001: unit tests for R-001 to R-011 pass with no network (pytest with sockets blocked).
- V-002: `run-once --sink local` against the live feeds writes 3 files and a heartbeat.
- V-003: `run-once` against the workspace lands files in `workspace.bronze.landing`.
- V-004: after deploy, 30 minutes of runs produce 15 heartbeats with all three feeds `ok`/`unchanged`.
- V-005: dead-letter drill: point the Databricks host at an invalid address for one run, restore it, and the next run replays everything.
- V-006: the static run lands the zip once; a second run is `unchanged`.
- Evidence: test names, the landing listing, the heartbeat sample saved to `samples/`.

## Approval
- Owner: approved 2026-10-14
