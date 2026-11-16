"""Turns tools.sql into CREATE FUNCTION statements for one environment's schemas."""

from __future__ import annotations

import re
from pathlib import Path

from transitpulse.common.config import PipelineConfig

SQL_FILE = Path(__file__).with_name("tools.sql")
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def statements(cfg: PipelineConfig, text: str | None = None) -> list[str]:
    values = {"tp_catalog": cfg.catalog, "tp_ops": cfg.ops, "tp_gold": cfg.gold}
    # Schema names can't be bound parameters in DDL, so they're checked before being placed.
    for value in values.values():
        if not _IDENTIFIER.match(value):
            raise ValueError(f"not a valid identifier: {value!r}")
    text = SQL_FILE.read_text(encoding="utf-8") if text is None else text
    for key, value in values.items():
        text = text.replace("${" + key + "}", value)
    if "${" in text:
        raise ValueError("tools.sql has a placeholder with no value")
    chunks = re.split(r";\s*$", text, flags=re.MULTILINE)
    starts = [re.search(r"^CREATE ", c, flags=re.MULTILINE) for c in chunks]
    return [c[m.start() :].strip() for c, m in zip(chunks, starts, strict=True) if m]
