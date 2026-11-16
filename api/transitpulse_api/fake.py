"""In-memory Repository for tests and local runs (TP_BACKEND=fake). Same filtering as the SQL, in Python."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, date, datetime

from transitpulse_api.repository import Row


@dataclass
class FakeRepository:
    routes_rows: list[Row] = field(default_factory=list)
    stops: set[str] = field(default_factory=set)
    otp_rows: list[Row] = field(default_factory=list)
    stop_rows: list[Row] = field(default_factory=list)
    rail_rows: list[Row] = field(default_factory=list)
    ghost_rows: list[Row] = field(default_factory=list)
    kpi_rows: list[Row] = field(default_factory=list)
    incident_rows: list[Row] = field(default_factory=list)
    healthy: bool = True

    def routes(self, limit: int, offset: int) -> list[Row]:
        return self.routes_rows[offset : offset + limit]

    def route_exists(self, route_id: str) -> bool:
        return any(r["route_id"] == route_id for r in self.routes_rows)

    def route_delays(self, route_id: str, start: date, end: date, limit: int, offset: int) -> list[Row]:
        rows = [r for r in self.otp_rows if r["route_id"] == route_id and start <= r["service_date"] <= end]
        return [{k: v for k, v in r.items() if k != "route_id"} for r in rows][offset : offset + limit]

    def stop_exists(self, stop_id: str) -> bool:
        return stop_id in self.stops

    def stop_delays(self, stop_id: str, start: date, end: date, limit: int, offset: int) -> list[Row]:
        rows = [r for r in self.stop_rows if r["stop_id"] == stop_id and start <= r["service_date"] <= end]
        return [{k: v for k, v in r.items() if k != "stop_id"} for r in rows][offset : offset + limit]

    def rail_line_health(self, line: str, service_date: date | None) -> list[Row]:
        rows = [r for r in self.rail_rows if r["line"] == line]
        if not rows:
            return []
        day = service_date or max(r["service_date"] for r in rows)
        return [{k: v for k, v in r.items() if k != "line"} for r in rows if r["service_date"] == day]

    def ghost_trips(self, service_date: date, route_id: str | None, limit: int, offset: int) -> list[Row]:
        rows = [
            r
            for r in self.ghost_rows
            if r["service_date"] == service_date and (route_id is None or r["route_id"] == route_id)
        ]
        return rows[offset : offset + limit]

    def daily_kpis(self, start: date, end: date) -> list[Row]:
        return [r for r in self.kpi_rows if start <= r["service_date"] <= end]

    def incidents(self, status: str | None, limit: int, offset: int) -> list[Row]:
        rows = [r for r in self.incident_rows if status is None or r["status"] == status]
        rows.sort(key=lambda r: (r["opened_at"], r["incident_id"]), reverse=True)
        return rows[offset : offset + limit]

    def incident(self, incident_id: str) -> Row | None:
        return next((dict(r) for r in self.incident_rows if r["incident_id"] == incident_id), None)

    def acknowledge(self, incident_id: str, who: str) -> bool:
        for r in self.incident_rows:
            if r["incident_id"] == incident_id and r["status"] == "proposed":
                r |= {"status": "acknowledged", "acknowledged_by": who, "acknowledged_at": datetime.now(UTC)}
                return True
        return False

    def ping(self) -> None:
        if not self.healthy:
            raise ConnectionError("warehouse unreachable")
