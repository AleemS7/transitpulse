from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal

Layer = Literal["bronze", "silver", "gold", "ops"]

# Keys set in the pipeline configuration (resources/transitpulse.yml). SQL files read the same keys as ${key}.
KEYS = {
    "catalog": "tp_catalog",
    "bronze": "tp_bronze",
    "silver": "tp_silver",
    "gold": "tp_gold",
    "ops": "tp_ops",
    "landing_root": "tp_landing_root",
}


@dataclass(frozen=True)
class PipelineConfig:
    catalog: str = "workspace"
    bronze: str = "bronze"
    silver: str = "silver"
    gold: str = "gold"
    ops: str = "ops"
    landing_root: str = "/Volumes/workspace/bronze/landing"

    def table(self, layer: Layer, name: str) -> str:
        """Fully qualified table name, e.g. workspace.dev_silver.rail_arrivals."""
        schema: str = getattr(self, layer)
        return f"{self.catalog}.{schema}.{name}"

    def landing(self, feed: str) -> str:
        return f"{self.landing_root.rstrip('/')}/{feed}/"

    @classmethod
    def from_conf(cls, get: Callable[[str, str], str]) -> PipelineConfig:
        """Build from a getter shaped like spark.conf.get(key, default)."""
        defaults = cls()
        return cls(**{field: get(key, getattr(defaults, field)) for field, key in KEYS.items()})
