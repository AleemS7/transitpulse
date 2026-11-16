"""Read access to the gold tables. The API depends on the Repository protocol; SqlRepository runs SQL on a
Databricks SQL warehouse, and tests use FakeRepository so they never touch the network.
"""

from __future__ import annotations

from collections.abc import Callable
from datetime import date
from typing import Any, Protocol

from transitpulse_api.settings import Settings

Row = dict[str, Any]


class Repository(Protocol):
    def routes(self, limit: int, offset: int) -> list[Row]: ...
    def route_exists(self, route_id: str) -> bool: ...
    def route_delays(self, route_id: str, start: date, end: date, limit: int, offset: int) -> list[Row]: ...
    def stop_exists(self, stop_id: str) -> bool: ...
    def stop_delays(self, stop_id: str, start: date, end: date, limit: int, offset: int) -> list[Row]: ...
    def rail_line_health(self, line: str, service_date: date | None) -> list[Row]: ...
    def ghost_trips(self, service_date: date, route_id: str | None, limit: int, offset: int) -> list[Row]: ...
    def daily_kpis(self, start: date, end: date) -> list[Row]: ...
    def ping(self) -> None: ...


Query = Callable[[str, dict[str, Any]], list[Row]]


class SqlRepository:
    """Every value is a bound parameter; only table names (validated in Settings) are formatted in."""

    def __init__(self, settings: Settings, query: Query) -> None:
        self._s = settings
        self._query = query

    def _t(self, name: str) -> str:
        return self._s.table(name)

    def routes(self, limit: int, offset: int) -> list[Row]:
        return self._query(
            f"SELECT route_id, route_short_name, route_long_name, transit_mode FROM {self._t('dim_route')}"
            " WHERE is_current ORDER BY transit_mode, route_short_name LIMIT :limit OFFSET :offset",
            {"limit": limit, "offset": offset},
        )

    def route_exists(self, route_id: str) -> bool:
        rows = self._query(
            f"SELECT 1 AS found FROM {self._t('dim_route')} WHERE route_id = :route_id LIMIT 1",
            {"route_id": route_id},
        )
        return bool(rows)

    def route_delays(self, route_id: str, start: date, end: date, limit: int, offset: int) -> list[Row]:
        return self._query(
            "SELECT service_date, hour_local, route_short_name, arrivals, on_time, early, late, on_time_pct,"
            f" avg_delay_seconds, p90_delay_seconds FROM {self._t('otp_route_hour')}"
            " WHERE route_id = :route_id AND service_date BETWEEN :start AND :end"
            " ORDER BY service_date, hour_local LIMIT :limit OFFSET :offset",
            {"route_id": route_id, "start": start, "end": end, "limit": limit, "offset": offset},
        )

    def stop_exists(self, stop_id: str) -> bool:
        rows = self._query(
            f"SELECT 1 AS found FROM {self._t('dim_stop')} WHERE stop_id = :stop_id LIMIT 1",
            {"stop_id": stop_id},
        )
        return bool(rows)

    def stop_delays(self, stop_id: str, start: date, end: date, limit: int, offset: int) -> list[Row]:
        return self._query(
            "SELECT service_date, stop_name, arrivals, routes, avg_delay_seconds, p90_delay_seconds, late_pct"
            f" FROM {self._t('stop_delay')}"
            " WHERE stop_id = :stop_id AND service_date BETWEEN :start AND :end"
            " ORDER BY service_date LIMIT :limit OFFSET :offset",
            {"stop_id": stop_id, "start": start, "end": end, "limit": limit, "offset": offset},
        )

    def rail_line_health(self, line: str, service_date: date | None) -> list[Row]:
        table = self._t("rail_line_health")
        return self._query(
            "SELECT service_date, station, direction, SUM(predictions) AS predictions,"
            " ROUND(AVG(avg_delay_seconds)) AS avg_delay_seconds,"
            " MAX(p90_delay_seconds) AS p90_delay_seconds,"
            " MAX(longest_wait_seconds) AS longest_wait_seconds"
            f" FROM {table} WHERE line = :line"
            f" AND service_date = COALESCE(:service_date, (SELECT MAX(service_date) FROM {table}))"
            " GROUP BY service_date, station, direction ORDER BY station, direction",
            {"line": line, "service_date": service_date},
        )

    def ghost_trips(self, service_date: date, route_id: str | None, limit: int, offset: int) -> list[Row]:
        return self._query(
            "SELECT service_date, trip_id, route_id, route_short_name, direction_id, trip_headsign,"
            f" scheduled_start_local, reason FROM {self._t('ghost_trips')}"
            " WHERE service_date = :service_date AND (:route_id IS NULL OR route_id = :route_id)"
            " ORDER BY scheduled_start_local, trip_id LIMIT :limit OFFSET :offset",
            {"service_date": service_date, "route_id": route_id, "limit": limit, "offset": offset},
        )

    def daily_kpis(self, start: date, end: date) -> list[Row]:
        return self._query(
            f"SELECT * FROM {self._t('daily_kpis')} WHERE service_date BETWEEN :start AND :end"
            " ORDER BY service_date",
            {"start": start, "end": end},
        )

    def ping(self) -> None:
        self._query("SELECT 1 AS ok", {})


def databricks_query(settings: Settings) -> Query:
    """Runs queries on the SQL warehouse as the app's service principal (credentials injected by Apps)."""
    from databricks import sql
    from databricks.sdk.core import Config, oauth_service_principal

    config = Config()

    def credentials() -> Any:
        return oauth_service_principal(config)

    def run(statement: str, params: dict[str, Any]) -> list[Row]:
        with (
            sql.connect(
                server_hostname=(config.host or "").removeprefix("https://"),
                http_path=f"/sql/1.0/warehouses/{settings.warehouse_id}",
                credentials_provider=credentials,
            ) as conn,
            conn.cursor() as cur,
        ):
            cur.execute(statement, params)
            columns = [c[0] for c in cur.description or []]
            return [dict(zip(columns, row, strict=True)) for row in cur.fetchall()]

    return run
