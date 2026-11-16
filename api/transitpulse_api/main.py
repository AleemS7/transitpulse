"""Read-only API over the gold tables.

Every success is {"data": ..., "meta": {...}}; every error is {"error": {"code", "message"}} with a matching
HTTP status. List endpoints page with limit/offset and return next_offset (null on the last page).
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from typing import Annotated, Any

from fastapi import FastAPI, Query, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from transitpulse_api.fake import FakeRepository
from transitpulse_api.genie import Asker, AskError, FakeAsker, GenieAsker
from transitpulse_api.repository import Repository, Row, SqlRepository, databricks_query
from transitpulse_api.settings import Settings

log = logging.getLogger("transitpulse_api")

RAIL_LINES = ("RED", "GOLD", "BLUE", "GREEN")
DEFAULT_DAYS = 7
MAX_DAYS = 92

Limit = Annotated[int, Query(ge=1, le=500)]
Offset = Annotated[int, Query(ge=0)]


class Question(BaseModel):
    question: str = Field(min_length=3, max_length=500)


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def _error(status: int, code: str, message: str) -> JSONResponse:
    return JSONResponse(status_code=status, content={"error": {"code": code, "message": message}})


def _page(rows: list[Row], source: str, limit: int | None = None, offset: int = 0) -> dict[str, Any]:
    meta: dict[str, Any] = {
        "count": len(rows),
        "source": source,
        "generated_at": datetime.now(UTC).isoformat(timespec="seconds"),
    }
    if limit is not None:
        meta |= {
            "limit": limit,
            "offset": offset,
            "next_offset": offset + limit if len(rows) == limit else None,
        }
    return {"data": rows, "meta": meta}


def _date_range(start: date | None, end: date | None) -> tuple[date, date]:
    end = end or datetime.now(UTC).date()
    start = start or end - timedelta(days=DEFAULT_DAYS - 1)
    if start > end:
        raise ApiError(400, "invalid_range", "start must be on or before end")
    if (end - start).days >= MAX_DAYS:
        raise ApiError(400, "invalid_range", f"a range can cover at most {MAX_DAYS} days")
    return start, end


def create_app(repo: Repository, settings: Settings | None = None, asker: Asker | None = None) -> FastAPI:
    settings = settings or Settings()
    app = FastAPI(
        title="TransitPulse API",
        version="0.1.0",
        description="Atlanta transit reliability from MARTA open data (uses MARTA open data).",
    )

    def source(table: str) -> str:
        return f"{settings.catalog}.{settings.gold_schema}.{table}"

    @app.middleware("http")
    async def request_id(request: Request, call_next: Callable[[Request], Awaitable[Response]]) -> Response:
        rid = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
        response = await call_next(request)
        response.headers["x-request-id"] = rid
        log.info("request=%s %s %s -> %s", rid, request.method, request.url.path, response.status_code)
        return response

    @app.exception_handler(ApiError)
    async def api_error(_: Request, exc: ApiError) -> JSONResponse:
        return _error(exc.status, exc.code, exc.message)

    @app.exception_handler(RequestValidationError)
    async def invalid_request(_: Request, exc: RequestValidationError) -> JSONResponse:
        first = exc.errors()[0] if exc.errors() else {}
        where = ".".join(str(p) for p in first.get("loc", []))
        return _error(422, "invalid_parameter", f"{where}: {first.get('msg', 'invalid')}")

    @app.exception_handler(StarletteHTTPException)
    async def http_error(_: Request, exc: StarletteHTTPException) -> JSONResponse:
        return _error(
            exc.status_code, "not_found" if exc.status_code == 404 else "http_error", str(exc.detail)
        )

    @app.exception_handler(Exception)
    async def unexpected(_: Request, exc: Exception) -> JSONResponse:
        log.exception("unhandled error")
        return _error(503, "upstream_unavailable", "the data warehouse could not answer; try again shortly")

    @app.get("/health")
    def health() -> dict[str, Any]:
        try:
            repo.ping()
        except Exception as exc:
            log.warning("health check failed: %s", exc)
            raise ApiError(503, "warehouse_unreachable", "the SQL warehouse did not answer") from exc
        return {"data": {"status": "ok"}, "meta": {}}

    @app.get("/routes")
    def routes(limit: Limit = 100, offset: Offset = 0) -> dict[str, Any]:
        return _page(repo.routes(limit, offset), source("dim_route"), limit, offset)

    @app.get("/routes/{route_id}/delays")
    def route_delays(
        route_id: str,
        start: date | None = None,
        end: date | None = None,
        limit: Limit = 200,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        first, last = _date_range(start, end)
        if not repo.route_exists(route_id):
            raise ApiError(404, "route_not_found", f"no route {route_id!r}")
        rows = repo.route_delays(route_id, first, last, limit, offset)
        return _page(rows, source("otp_route_hour"), limit, offset)

    @app.get("/stops/{stop_id}/delays")
    def stop_delays(
        stop_id: str,
        start: date | None = None,
        end: date | None = None,
        limit: Limit = 200,
        offset: Offset = 0,
    ) -> dict[str, Any]:
        first, last = _date_range(start, end)
        if not repo.stop_exists(stop_id):
            raise ApiError(404, "stop_not_found", f"no stop {stop_id!r}")
        return _page(
            repo.stop_delays(stop_id, first, last, limit, offset), source("stop_delay"), limit, offset
        )

    @app.get("/rail/lines/{line}/health")
    def rail_health(line: str, service_date: date | None = None) -> dict[str, Any]:
        if line.upper() not in RAIL_LINES:
            raise ApiError(404, "line_not_found", f"line must be one of {', '.join(RAIL_LINES)}")
        return _page(repo.rail_line_health(line.upper(), service_date), source("rail_line_health"))

    @app.get("/ghost-trips")
    def ghost_trips(
        service_date: date, route_id: str | None = None, limit: Limit = 200, offset: Offset = 0
    ) -> dict[str, Any]:
        rows = repo.ghost_trips(service_date, route_id, limit, offset)
        return _page(rows, source("ghost_trips"), limit, offset)

    @app.get("/kpis/daily")
    def daily_kpis(start: date | None = None, end: date | None = None) -> dict[str, Any]:
        first, last = _date_range(start, end)
        return _page(repo.daily_kpis(first, last), source("daily_kpis"))

    @app.post("/ask")
    def ask(body: Question) -> dict[str, Any]:
        if asker is None:
            raise ApiError(501, "ask_not_configured", "no Genie space is configured for this deployment")
        try:
            answer = asker.ask(body.question.strip())
        except AskError as exc:
            log.warning("genie failed: %s", exc)
            raise ApiError(503, "genie_unavailable", "Genie could not answer right now; try again") from exc
        if answer.status != "COMPLETED":
            raise ApiError(502, "genie_no_answer", f"Genie finished with status {answer.status}")
        return {
            "data": {
                "answer": answer.text,
                # The SQL and its rows back every number in the answer; without a query there are no numbers.
                "sql": answer.sql,
                "columns": answer.columns,
                "rows": answer.rows,
            },
            "meta": {
                "source": "genie",
                "conversation_id": answer.conversation_id,
                "grounded": answer.sql is not None,
            },
        }

    return app


def build() -> FastAPI:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    repo: Repository
    asker: Asker | None = None
    if settings.backend == "databricks":
        repo = SqlRepository(settings, databricks_query(settings))
        if settings.genie_space_id:
            asker = GenieAsker(settings.genie_space_id)
    else:
        repo = FakeRepository()
        asker = FakeAsker()
    return create_app(repo, settings, asker)


app = build()
