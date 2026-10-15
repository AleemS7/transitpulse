# Collector

An Azure Function that polls MARTA's open data feeds and lands the raw files in a Databricks Unity Catalog volume. Spec: [`design/specs/collector.md`](../design/specs/collector.md). Decision: [`design/adr/0001-external-collector.md`](../design/adr/0001-external-collector.md).

| Timer | Schedule (UTC) | Does |
|---|---|---|
| `realtime` | every 2 minutes | bus vehicle positions, bus trip updates, rail arrivals → `landing/{feed}/dt=YYYY-MM-DD/` |
| `static_schedule` | daily 09:00 | GTFS zip → `landing/gtfs_static/` (only when it changed) |

Every run writes a heartbeat to `landing/_heartbeat/`. Uploads that fail go to the `deadletter` blob container and are replayed on the next run.

## Run locally

```powershell
cd collector
uv sync
$env:TP_RAIL_API_KEY = Read-Host 'MARTA rail key' -MaskInput
uv run python -m transitpulse_collector run-once --local ../.landing-local   # write to a folder
uv run python -m transitpulse_collector run-once --static --local ../.landing-local
```

To write to Databricks instead, also set `TP_DATABRICKS_HOST` and `TP_DATABRICKS_TOKEN` and drop `--local`. Failed uploads then go to `collector/.deadletter/`.

## Deploy

Requires the Azure CLI, Azure Functions Core Tools and the Databricks CLI.

Region: **Central US**. A deployment in East US couldn't open connections to either MARTA host (every request timed out), while the same code in Central US reached both.

1. **Landing zone in Databricks** (once): run [`sql/setup/landing.sql`](../sql/setup/landing.sql) in the SQL editor.
2. **Resource group and resources:**
   ```powershell
   az group create -n tp-atl-pr-rg-02 -l centralus --tags app=transitpulse env=pr owner=transitpulse costCenter=portfolio
   az deployment group create -g tp-atl-pr-rg-02 -f infra/collector.bicep `
     -p suffix=02 databricksHost=https://<workspace>.cloud.databricks.com alertEmail=<address> `
        deployerPrincipalId=$(az ad signed-in-user show --query id -o tsv)
   ```
3. **Secrets** (typed, never stored in files or shell history):
   ```powershell
   $kv = 'tp-atl-pr-kv-02'
   az keyvault secret set --vault-name $kv --name databricks-token --value (Read-Host 'Databricks token' -MaskInput) --output none
   az keyvault secret set --vault-name $kv --name marta-rail-api-key --value (Read-Host 'MARTA rail key' -MaskInput) --output none
   ```
   Role assignments can take a few minutes to apply; retry if Key Vault returns 403.
4. **Code:**
   ```powershell
   cd collector
   func azure functionapp publish tp-atl-pr-func-collector-02 --python
   ```
5. **Verify:** heartbeats appear every 2 minutes.
   ```powershell
   databricks fs ls dbfs:/Volumes/workspace/bronze/landing/_heartbeat/dt=$((Get-Date).ToUniversalTime().ToString('yyyy-MM-dd'))/
   ```

After changing dependencies, run `scripts/export-requirements.ps1`; the Functions build installs from `requirements.txt`.
