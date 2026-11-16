# ADR-0008: A small tool-calling triage agent, checked against a rules baseline

- **Status:** Accepted
- **Date:** 2026-11-16
- **Related:** AGT-001–006, SPK-005, ADR-0007, `design/specs/triage.md`, `src/transitpulse/agent/`

## Context
A failed health check says *what* is wrong (ADR-0007), not *why*. Finding the cause means reading the collector runs, the pipeline's event log, the quality metrics and the runbooks, which is the same few lookups every time. An LLM can do those lookups and draft an incident. But a model can also invent a cause or a number. So the design has to make a wrong answer cheap: the draft is checked, a deterministic answer always exists, and a person decides.

Free Edition limits what can run: serverless only, a daily compute quota, and one AI Search endpoint. Pay-per-token Foundation Model endpoints are available. Llama 3.3 70B, gpt-oss and Qwen all answered tool calls in about a second ✱ (SPK-005).

## Options considered
1. **Framework (Mosaic AI Agent Framework, LangGraph).** It gives tracing and serving. But it adds dependencies and concepts to a loop that only needs "call the model, run the tool it asked for, repeat".
2. **A plain loop over the chat-completions API.** About 150 lines. Every guardrail is visible in one file and unit-tested with a scripted model.
3. **Runbook retrieval with AI Search (vector index).** It uses the workspace's single endpoint and compute quota, for two runbooks with about 30 entries.
4. **Keyword search over runbook sections and fix-table rows.** It's deterministic and testable, and enough at this size.
5. **No model: rules only.** Predictable, but it only knows what it was written for. That's the baseline any agent has to beat.

## Decision
Options 2 and 4, with option 5 kept as the fallback and the baseline.

**Tools:**
- six read-only Unity Catalog SQL functions (`ops.get_*`), which take at most an hour window, so neither the rules nor the model write SQL
- runbook search

**The incident:**
- The model finishes by calling `submit_incident`.
- The result is accepted only if it passes the Pydantic schema and every number in its evidence appears in the tool result it cites.
- A rejected draft goes back to the model with the reason.

**Limits:**
- 8 steps and 40,000 tokens.
- Running out, or any model error, means the rules' incident is used instead.

**Model:** Llama 3.3 70B, as the default endpoint, set by the `triage_model` bundle variable. On the 14 labeled cases:

| | Rules | Llama 3.3 70B | gpt-oss-120b |
|---|---|---|---|
| Root cause correct | 13/14 | 13/14 | 10/14 |
| Incident accepted | 14/14 | 14/14 | 12/14 |
| Average tokens, time | none | 13.4k, 12 s | 11.2k, 11 s |

After the prompt was changed to make the runbook step an action rather than a section title, the logged MLflow run scored Llama 12/14. This time it also called connection timeouts a collector failure, so one prompt change moved one answer. The evaluation job exists for exactly this: re-run it after every prompt or model change.

Llama corrected all 5 of its drafts that were rejected for an untraced number. gpt-oss was the only one to catch the decode-error case that the rules miss, but it ran out of steps twice and called a quality-rule failure a service disruption.

## Consequences
- **Who decides:** incidents land in `ops.incidents` as `proposed`. Only a signed-in person moves one to `acknowledged`, through `POST /incidents/{id}/ack`.
- **Model calls:**
  - The same failing checks update the open incident rather than calling the model again.
  - When every check passes, open incidents are marked cleared.
- **How the alert carries the incident:** the triage task runs with `run_if: ALL_DONE`, so it also covers a failed refresh. It fails with the incident's summary, so the existing failure email carries the proposed cause and runbook step.
- **No automatic retries:**
  - Serverless retries a failed task automatically ✱, and `health_checks` and `triage` fail on purpose. On dev, that ran triage twice per failure and double-counted occurrences.
  - Both tasks now set `max_retries: 0` and `disable_auto_optimization: true`.
  - The occurrence update skips a repeat of the same job run.
- **Accuracy:** with rules at 13/14 on cases written for them, the agent's measured value today is matching them, plus a second opinion: `rules_cause_category` is stored next to the agent's answer. Disagreements are the cases worth labeling next.
- **Evaluation set:**
  - The cases are tool outputs. Four are replayed from real failures (MARTA's overnight rail HTTP 500s, connection timeouts from the first Azure region, and two pipeline failures on dev). Ten model the chaos tests (stop the collector, corrupt a file, rename a field, revoke a key).
  - They haven't been injected live: stopping the collector is an Azure change, and a corrupt file would land in the production landing volume.
- **Cost:**
  - The model is called at most once per new problem.
  - Evaluation runs on demand (`transitpulse-triage-eval`) and logs accuracy, acceptance, tokens and time to MLflow.

## How I'd explain it in an interview
"When an hourly health check fails, a small agent reads the same tables I would through read-only Unity Catalog functions, checks the runbooks, and proposes an incident. It can't write SQL, it has a step and token budget, and I reject its answer if any number in the evidence isn't in a tool result it cites. A rules classifier always runs too. It's the fallback and the baseline, and on 14 labeled failure cases the agent matched or slightly trailed it, which I report as is. Nothing is final until a person acknowledges it through the API."
