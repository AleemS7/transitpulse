# Deploy on merge (slice S4b)

## Outcome
Merging to `main` deploys what changed: the Databricks bundle to `prod`, and the collector to its Function App. Nobody deploys prod from a laptop. The gate is the required PR check on `main`; there's no manual approval step (ADR-0003).

## Users and systems
- **GitHub Actions** `deploy.yml`, using the GitHub Environment `prod` (deployments allowed from `main` only).
- **Databricks:** `databricks bundle deploy -t prod`.
- **Azure:** the collector is published to `tp-atl-pr-func-collector-02` through OIDC workload identity federation, with no Azure secret stored in GitHub.

## Requirements

### R-001: Deploy only what changed (OPS-002)
WHEN a push to `main` changes `src/`, `resources/` or `databricks.yml` THE SYSTEM SHALL deploy the bundle to `prod`. WHEN it changes `collector/` THE SYSTEM SHALL publish the collector. Both can run on the same push.

### R-002: Only main deploys (OPS-002)
THE SYSTEM SHALL allow `prod` deployments only from `main`. `main` accepts changes only through pull requests whose `scripts/check.ps1` run passed (repository ruleset).

### R-003: Checks first
THE SYSTEM SHALL have passed `scripts/check.ps1` on the pull request, and SHALL run `databricks bundle validate -t prod` before deploying the bundle, stopping on failure.

### R-004: No long-lived Azure secret (OPS-002)
THE SYSTEM SHALL sign in to Azure with OIDC: an Entra app registration with a federated credential limited to `repo:AleemS7/transitpulse:environment:prod`, holding only the *Website Contributor* role on the Function App.

### R-005: Databricks credential (OPS-002)
Free Edition has no account console, so there is no OIDC federation to Databricks. THE SYSTEM SHALL use a dedicated Databricks token stored as the `prod` environment secret `DATABRICKS_TOKEN`, separate from the collector's token, with its expiry tracked (ADR-0003).

### R-006: Smoke check after deploy
WHEN the bundle deploys THE SYSTEM SHALL run the `transitpulse-refresh` job once and fail the workflow if the run fails. WHEN the collector deploys THE SYSTEM SHALL confirm that a new heartbeat lands within 5 minutes.

## Constraints and assumptions
- The repository is public, so environment protection rules and branch rulesets are available on the free plan ✱.
- `Azure/functions-action` supports Flex Consumption with a remote build ✱.

## Tasks
- T-001: Entra app registration, federated credential, role assignments -> R-004
- T-002: GitHub Environment `prod` (main only); secrets and variables -> R-002, R-005
- T-003: `deploy.yml` with path filters, checks, bundle and collector jobs, smoke checks -> R-001, R-003, R-006
- T-004: ADR-0003 (Databricks token in CI) -> R-005
- T-005: branch ruleset on `main` requiring the PR validation check

## Verification
- V-001: a merge touching only `src/` runs the bundle job and not the collector job.
- V-002: a deploy from any branch other than `main` is refused by the environment.
- V-003: the post-deploy job run and the heartbeat check both pass.
