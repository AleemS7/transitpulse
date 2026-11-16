# Incident triage (slice S11)

## Outcome
When an hourly run fails, a proposed incident already names the likely cause, cites the evidence, and points to a runbook step. Whoever is on call starts from a draft instead of from five queries, and decides whether it's right.

## Users and systems
- **On-call engineer:** reads the failure email and acknowledges incidents through the API.
- **Hourly job `transitpulse-refresh`:** runs the `triage` task after `health_checks`.
- **Foundation Model endpoint** (default `databricks-meta-llama-3-3-70b-instruct`): called pay-per-token as the job's identity.
- **`ops` tables and Unity Catalog functions:** the only data the triage can read.

## In scope / Out of scope
- **In:**
  - triage of failed checks and failed refreshes
  - the rules fallback
  - incident storage, acknowledgement and clearing
  - the labeled evaluation
- **Out:**
  - automatic fixes (the agent never changes anything)
  - triage of the collector when the job itself isn't running
  - live chaos injection (ADR-0008)

## Requirements

### R-001: Rules first (AGT-001)
WHEN a run has failed checks, or an earlier task failed, THE SYSTEM SHALL classify it with deterministic rules into one of:
- `source_outage`
- `collector_failure`
- `pipeline_failure`
- `schema_drift`
- `data_quality`
- `real_service_disruption`
- null when no rule fits

It SHALL attach evidence and a runbook step.

### R-002: Read-only tools (AGT-002)
THE SYSTEM SHALL expose data only through these functions:
- `ops.get_check_history(hours)`
- `ops.get_feed_freshness()`
- `ops.get_collector_runs(hours)`
- `ops.get_pipeline_updates(hours)`
- `ops.get_dq_failures(hours)`
- `ops.get_bus_service(hours)`

It SHALL also offer `search_runbooks(query)`.

Limits:
- `hours` SHALL be 1–168.
- A result SHALL hold at most 40 rows and 300 characters per text value.
- An unknown tool or a bad argument SHALL be reported back to the caller, not run.

### R-003: Runbook lookup (AGT-003)
`search_runbooks` SHALL return the three runbook entries that best match the query, where an entry is a section or a fix-table row. Each SHALL name its file and section.

### R-004: Agent loop and guardrails (AGT-004)
WHEN a model is configured THE SYSTEM SHALL let it call the tools and finish with `submit_incident`.

**Acceptance.** It SHALL accept the incident only if:
- it validates against the schema
- every evidence item cites a result ID from this triage
- every number in a statement appears in that result (rounding allowed)
- the runbook exists

**Rejection.** A rejected draft SHALL be returned to the model with the reasons.

**Fallback.** After 8 steps, 40,000 tokens or any model error, THE SYSTEM SHALL use the rules' incident.

**Recorded on every incident:**
- the classifier
- the endpoint and model version
- tokens and steps
- the rules' cause

### R-005: Incidents and the human decision (AGT-005)
**New problem.** THE SYSTEM SHALL write a new problem to `ops.incidents` as `proposed`, with every tool result it used.

**Repeated problem.** The same failing checks SHALL update the open incident (`last_seen_at`, `occurrences`) without calling the model.

**Recovery.** A run where every check passes SHALL set `cleared_at` on open incidents.

**Alert.** The triage task SHALL fail with the incident's summary, so the job's failure email carries it.

**Retries.** The task SHALL not be retried automatically. A repeat of the same job run SHALL NOT count another occurrence.

**API:**
- `GET /incidents?status=`
- `GET /incidents/{id}`: includes the evidence and tool results
- `POST /incidents/{id}/ack`: moves `proposed` to `acknowledged`, records the signed-in user from `X-Forwarded-Email`, and returns 409 if already acknowledged and 401 if no user

### R-006: Evaluation (AGT-006)
THE SYSTEM SHALL keep labeled cases (`transitpulse.agent.cases`), marked `replayed` or `chaos`.

**On demand**, the `transitpulse-triage-eval` job SHALL run the rules and the agent on every case and log to MLflow:
- the endpoint and model version
- root-cause accuracy for both
- acceptance rate
- average tokens and seconds
- the per-case table

## Security, data and oversight
- **Data sent to the model:**
  - operational metrics and runbook text only; no personal data exists in this project
  - error messages are truncated to 300 characters
- **Read-only:** the model can't run SQL, change data or call anything but the listed tools.
- **Untrusted text in tool results:** collector errors can carry text from MARTA's responses, so the model may read injected instructions. That text is capped at 300 characters. The model's only output is a checked draft, and a person decides, so the worst case is a wrong proposal.
- **Who acknowledged:** the user comes from `X-Forwarded-Email`, which the Databricks Apps proxy sets for signed-in users ✱. The API isn't reachable without that proxy.
- **The API's write access:** its service principal gets `USE SCHEMA` on `ops` and `SELECT, MODIFY` on `ops.incidents`, and nothing else in `ops`. The grant is made by the triage task, which creates the table.
- **Human in the loop:** no incident is final until a person acknowledges it.

## Monitoring
- `ops.incidents.agent_error` and `classifier = 'rules'` show when the agent fell back.
- `rules_cause_category` next to `cause_category` shows disagreements.
- Re-run the evaluation job after changing the model, prompt or tools.

## Tests
`tests/unit/test_agent.py` covers:
- number tracing
- the schema
- each rule on the labeled cases (one known miss)
- tool validation and runbook search
- the loop, using a scripted model: accept, reject then correct, every fallback
- the incident row

`api/tests/test_api.py` covers the incident endpoints and the acknowledgement SQL.
