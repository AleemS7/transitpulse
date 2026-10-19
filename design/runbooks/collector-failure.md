# Runbook: collector failure

## Symptom
- No new heartbeats in `landing/_heartbeat/` for more than 10 minutes, or
- heartbeats show feeds as `failed` or `deadlettered`, or
- `deadletter_pending` above 0 for more than one run.

## Check
1. **Is the Function running?**
   `az functionapp show -g tp-atl-pr-rg-02 -n tp-atl-pr-func-collector-02 --query state`
2. **What did the last runs say?** Application Insights → Logs:
   ```kusto
   traces
   | where timestamp > ago(1h) and message startswith "run="
   | project timestamp, message
   | order by timestamp desc
   ```
3. **Read the latest heartbeat** (`status`, `http_status`, `error` per feed):
   `databricks fs cat dbfs:/Volumes/workspace/bronze/landing/_heartbeat/dt=<today>/<latest>.json`
4. **How much is waiting in dead-letter?**
   `az storage blob list --account-name tpatlprstcollect02 -c deadletter --auth-mode login --query "length(@)"`

## Fix
| What you see | Cause | Fix |
|---|---|---|
| Fetch `failed` with `ConnectTimeout` on every feed while MARTA works from elsewhere | MARTA's network not accepting connections from the Function's region (seen in East US) | Deploy to another region (Central US works) and stop the old app. |
| Fetch `failed`, HTTP 5xx or timeouts on every feed | MARTA outage | Nothing to fix. Gaps are expected and explained by the heartbeat. Record it in the incident log. |
| Rail `failed` with HTTP 401/403 | Rail key rejected | Register or renew the key, then `az keyvault secret set ... --name marta-rail-api-key`, then restart the app. |
| Every feed `deadlettered`, upload HTTP 401/403 | Databricks token expired or revoked | Create a new token in Databricks (Settings → Developer → Access tokens), then `az keyvault secret set ... --name databricks-token`, then restart the app. Items replay on the next run. |
| Every feed `deadlettered`, upload 5xx or timeouts | Databricks unavailable | Wait. Items replay automatically, up to 50 per run. |
| `deadletter_pending` stuck above 0, log shows `replay stopped at ...` with an SSL or connection error | An item that keeps failing blocks replay (it stops at the first failure) | Find the item in the log; fix the cause and redeploy; replay drains on the next run. |
| Upload HTTP 404 | Landing volume missing | Re-run `sql/setup/landing.sql`. |
| Runs fail with `VersionError: Detected incompatible Protobuf Gencode/Runtime versions` | A dependency upgrade pulled protobuf or `gtfs-realtime-bindings` past what the Functions worker supports | Restore the pins in `collector/pyproject.toml` (spec D-009), run `scripts/export-requirements.ps1`, and redeploy. |
| No heartbeats and no logs | Function stopped or failing to start | `az functionapp restart ...`; if it still won't start, redeploy with `func azure functionapp publish tp-atl-pr-func-collector-02 --python`. |

Restart: `az functionapp restart -g tp-atl-pr-rg-02 -n tp-atl-pr-func-collector-02`

Secrets live in Key Vault `tp-atl-pr-kv-02`.

## Verify
The next heartbeat shows all feeds `ok` or `unchanged`, and `deadletter_pending` falls to 0 within a few runs.

## Incident log
| Date | What happened | Root cause | Follow-up |
|---|---|---|---|
| 2026-10-14 | First deployment (East US) fetched nothing: every MARTA request timed out, while uploads to Databricks worked | MARTA unreachable from that region's outbound addresses | Redeployed in Central US; all feeds `ok` from 02:48 UTC on 2026-10-15 |
| 2026-10-15 to 2026-10-19 | `deadletter_pending` stuck at 5; every run logged `replay stopped at gtfs_static/...zip: SSLError` | The unchanged 18.6 MB schedule zip was re-uploaded daily; Databricks dropped the connection mid-body instead of answering 409, so it was dead-lettered and blocked replay | Uploads now check for the file first (HEAD) and send nothing if it exists |
