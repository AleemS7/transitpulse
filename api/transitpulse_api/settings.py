from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

Backend = Literal["databricks", "fake"]

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class Settings:
    backend: Backend = "fake"
    warehouse_id: str | None = None
    catalog: str = "workspace"
    gold_schema: str = "gold"
    ops_schema: str = "ops"
    genie_space_id: str | None = None

    def __post_init__(self) -> None:
        # Table names are the one part of a query that can't be a bound parameter, so they're checked here.
        for value in (self.catalog, self.gold_schema, self.ops_schema):
            if not _IDENTIFIER.match(value):
                raise ValueError(f"not a valid identifier: {value!r}")
        if self.backend == "databricks" and not self.warehouse_id:
            raise ValueError("TP_WAREHOUSE_ID is required when TP_BACKEND=databricks")

    def table(self, name: str, schema: str | None = None) -> str:
        if not _IDENTIFIER.match(name):
            raise ValueError(f"not a valid identifier: {name!r}")
        return f"`{self.catalog}`.`{schema or self.gold_schema}`.`{name}`"

    @classmethod
    def from_env(cls, env: Mapping[str, str] = os.environ) -> Settings:
        backend = env.get("TP_BACKEND", "fake")
        if backend not in ("databricks", "fake"):
            raise ValueError(f"TP_BACKEND must be databricks or fake, not {backend!r}")
        return cls(
            backend=backend,  # type: ignore[arg-type]
            warehouse_id=env.get("TP_WAREHOUSE_ID") or None,
            catalog=env.get("TP_CATALOG", "workspace"),
            gold_schema=env.get("TP_GOLD_SCHEMA", "gold"),
            ops_schema=env.get("TP_OPS_SCHEMA", "ops"),
            genie_space_id=env.get("TP_GENIE_SPACE_ID") or None,
        )
