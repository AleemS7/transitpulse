"""What started a triage: the health checks that failed in one job run, or no checks at all because an
earlier task in the job failed.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass

from transitpulse.agent.tools import Row


@dataclass(frozen=True)
class FailedCheck:
    name: str
    subject: str
    observed: str
    expected: str


@dataclass(frozen=True)
class Trigger:
    run_id: str
    failed: tuple[FailedCheck, ...]
    # health_checks never wrote a row for this run, so the pipeline (or GTFS extract) task failed first.
    upstream_failed: bool = False

    @property
    def needs_triage(self) -> bool:
        return self.upstream_failed or bool(self.failed)

    def fingerprint(self) -> str:
        """Same failing checks, same fingerprint: an ongoing problem updates its open incident."""
        keys = ["upstream_task_failed"] if self.upstream_failed else []
        keys += sorted(f"{c.name}:{c.subject}" for c in self.failed)
        return hashlib.sha256("|".join(keys).encode()).hexdigest()[:16]

    def rows(self) -> list[Row]:
        if self.upstream_failed:
            return [{"event": "health checks did not run: an earlier task in the job failed"}]
        return [asdict(c) for c in self.failed]
