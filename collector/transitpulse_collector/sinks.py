from __future__ import annotations

from pathlib import Path
from typing import Literal, Protocol
from urllib.parse import quote

from transitpulse_collector.transport import Transport, raise_for_status

PutResult = Literal["created", "exists"]


class Sink(Protocol):
    def put(self, path: str, data: bytes) -> PutResult: ...


class DatabricksFilesSink:
    """Writes to a Unity Catalog volume through the Databricks Files API, never overwriting."""

    def __init__(self, host: str, token: str, transport: Transport, timeout_s: float = 30.0) -> None:
        self._host = host.rstrip("/")
        self._token = token
        self._transport = transport
        self._timeout_s = timeout_s

    def put(self, path: str, data: bytes) -> PutResult:
        url = f"{self._host}/api/2.0/fs/files{quote(path)}"
        auth = {"Authorization": f"Bearer {self._token}"}
        # Check first: Databricks drops the connection mid-body when a large file already exists,
        # which surfaces as a connection error instead of a clean 409.
        head = self._transport.request("HEAD", url, headers=auth, timeout=self._timeout_s)
        if 200 <= head.status < 300:
            return "exists"
        if head.status != 404:
            raise_for_status(head, "exists check")
        resp = self._transport.request(
            "PUT",
            f"{url}?overwrite=false",
            headers={**auth, "Content-Type": "application/octet-stream"},
            body=data,
            timeout=self._timeout_s,
        )
        if resp.status == 409:
            return "exists"
        raise_for_status(resp, "upload")
        return "created"


class LocalSink:
    """Mirrors volume paths under a local folder; used for dry runs and local collection."""

    def __init__(self, out_dir: Path) -> None:
        self._out_dir = out_dir

    def put(self, path: str, data: bytes) -> PutResult:
        target = self._out_dir / path.lstrip("/")
        if target.exists():
            return "exists"
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)
        return "created"
