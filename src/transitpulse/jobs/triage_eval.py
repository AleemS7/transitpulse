"""Job task (run on demand): score the agent against the rules on the labeled cases and log it to MLflow.

Each run records the endpoint and model version, root-cause accuracy for both, how often the agent's incident
was accepted, tokens and time per case, plus the per-case table.
"""

import argparse
import sys
from dataclasses import asdict
from pathlib import Path


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--src-root", required=True)
    parser.add_argument("--runbooks", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--experiment", required=True, help="workspace path of the MLflow experiment")
    args = parser.parse_args(argv)

    sys.path.insert(0, args.src_root)
    import mlflow

    from transitpulse.agent.cases import CASES
    from transitpulse.agent.evaluate import run_cases, summary
    from transitpulse.agent.loop import Limits, ServingChat
    from transitpulse.agent.tools import Runbooks

    limits = Limits()
    results = run_cases(CASES, Runbooks.load(Path(args.runbooks)), ServingChat(args.model), limits)
    scores = summary(results)
    versions = sorted({r.model for r in results if r.model})

    experiment = mlflow.set_experiment(args.experiment)
    with mlflow.start_run(experiment_id=experiment.experiment_id, run_name=f"triage-{args.model}"):
        mlflow.log_params(
            {
                "model_endpoint": args.model,
                "model_version": ",".join(versions) or "none",
                "max_steps": limits.max_steps,
                "max_tokens": limits.max_tokens,
                "cases": len(results),
            }
        )
        mlflow.log_metrics(scores)
        rows = [asdict(r) for r in results]
        mlflow.log_table({key: [row[key] for row in rows] for key in rows[0]}, "cases.json")
    for r in results:
        print(f"{r.case}: expected {r.expected}, rules {r.rules_cause}, agent {r.agent_cause}")
    print(scores)


if __name__ == "__main__":
    main()
