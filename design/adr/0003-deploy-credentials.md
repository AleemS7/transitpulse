# ADR-0003: Keyless sign-in to Azure, a dedicated token for Databricks, and no approval gate

- **Status:** Accepted
- **Date:** 2026-10-16
- **Related:** OPS-002, GOV-002, SPK-004, `design/specs/deploy.md`

## Context
Merges to `main` should deploy themselves, which means GitHub Actions needs credentials for Azure (the collector) and Databricks (the bundle). Long-lived secrets in CI are the usual weak spot. Databricks Free Edition has no account console, so the OIDC federation Databricks offers elsewhere isn't available.

## Options considered
1. **Azure: OIDC workload identity federation.** An Entra app with a federated credential trusts GitHub's token for this repo's `prod` environment only. No secret exists to leak. Its only role is *Website Contributor* on the Function App.
2. **Azure: a client secret or publish profile in GitHub.** Simpler, but it's a long-lived secret.
3. **Databricks: a dedicated token as an environment secret.** It's separate from the collector's token, so either can be revoked without breaking the other.
4. **Databricks: reuse the collector's token.** One less token, but revoking it would stop collection and deploys together.
5. **Required reviewer on the `prod` environment.** One click per deploy.

## Decision
Options 1 and 3. The `prod` environment accepts deployments only from `main`, and `main` accepts only pull requests that passed `scripts/check.ps1`. There's no manual approval gate (option 5 rejected), so the required check is the gate.

## Consequences
- No Azure secret exists anywhere. Moving the collector to another app means updating one role assignment.
- The Databricks deploy token is the one long-lived CI secret. It's scoped to the `prod` environment, so pull-request workflows never see it. Its expiry is tracked along with the collector's token in `ops`.
- Without an approval gate, a merged PR goes live within minutes. Mistakes are fixed forward, or by reverting the merge, which triggers the same deploy.

## How I'd explain it in an interview
"CI signs in to Azure with OIDC, so there's no Azure secret at all, and the identity can only update one Function App. Databricks Free Edition doesn't support federation, so it gets one dedicated, environment-scoped token. Main only accepts PRs that passed the full check, and that check is the gate before prod."
