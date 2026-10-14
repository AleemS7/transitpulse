from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

from transitpulse_collector import __version__

REPO_URL = "https://github.com/AleemS7/transitpulse"


class SettingsError(Exception):
    pass


@dataclass(frozen=True)
class Settings:
    rail_api_key: str
    landing_root: str = "/Volumes/workspace/bronze/landing"
    databricks_host: str | None = None
    databricks_token: str | None = None
    deadletter_account_url: str | None = None
    deadletter_container: str = "deadletter"
    fetch_timeout_s: float = 20.0
    max_attempts: int = 3
    run_budget_s: float = 60.0
    replay_limit: int = 50

    @property
    def user_agent(self) -> str:
        return f"transitpulse-collector/{__version__} (+{REPO_URL})"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        env = os.environ if env is None else env
        rail_key = env.get("TP_RAIL_API_KEY", "").strip()
        if not rail_key:
            raise SettingsError("TP_RAIL_API_KEY is not set")
        return cls(
            rail_api_key=rail_key,
            landing_root=env.get("TP_LANDING_ROOT", cls.landing_root).rstrip("/"),
            databricks_host=(env.get("TP_DATABRICKS_HOST") or "").rstrip("/") or None,
            databricks_token=env.get("TP_DATABRICKS_TOKEN") or None,
            deadletter_account_url=env.get("TP_DEADLETTER_ACCOUNT_URL") or None,
            deadletter_container=env.get("TP_DEADLETTER_CONTAINER", cls.deadletter_container),
        )

    def secrets(self) -> list[str]:
        return [s for s in (self.rail_api_key, self.databricks_token) if s]
