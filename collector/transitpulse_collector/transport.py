from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Protocol

import requests

from transitpulse_collector.retry import PermanentError, RetryableError

RETRYABLE_STATUS = frozenset({408, 429, 500, 502, 503, 504})


@dataclass(frozen=True)
class Response:
    status: int
    body: bytes
    headers: Mapping[str, str] = field(default_factory=dict)


class TooLargeError(PermanentError):
    pass


class Transport(Protocol):
    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout: float,
        body: bytes | None = None,
        max_bytes: int | None = None,
    ) -> Response: ...


class RequestsTransport:
    """HTTP over requests, with timeouts and connection errors mapped to RetryableError."""

    def __init__(self) -> None:
        self._session = requests.Session()

    def request(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout: float,
        body: bytes | None = None,
        max_bytes: int | None = None,
    ) -> Response:
        try:
            with self._session.request(
                method, url, headers=dict(headers), data=body, timeout=timeout, stream=True
            ) as resp:
                chunks: list[bytes] = []
                size = 0
                for chunk in resp.iter_content(chunk_size=64 * 1024):
                    size += len(chunk)
                    if max_bytes is not None and size > max_bytes:
                        raise TooLargeError(f"response over {max_bytes} bytes", resp.status_code)
                    chunks.append(chunk)
                return Response(resp.status_code, b"".join(chunks), dict(resp.headers))
        except (requests.Timeout, requests.ConnectionError) as exc:
            raise RetryableError(f"{type(exc).__name__}: {exc}") from exc


def raise_for_status(resp: Response, what: str) -> None:
    if 200 <= resp.status < 300:
        return
    if resp.status in RETRYABLE_STATUS:
        raise RetryableError(f"{what}: HTTP {resp.status}", resp.status)
    raise PermanentError(f"{what}: HTTP {resp.status}", resp.status)
