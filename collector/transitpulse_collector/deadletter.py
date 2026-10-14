from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Protocol

from transitpulse_collector.landing import sha16


@dataclass(frozen=True)
class DeadLetterItem:
    key: str
    target_path: str
    data: bytes


class DeadLetter(Protocol):
    def save(self, target_path: str, data: bytes, error: str) -> None: ...
    def pending(self, limit: int) -> list[DeadLetterItem]: ...
    def remove(self, item: DeadLetterItem) -> None: ...
    def count(self) -> int: ...


def _key(target_path: str) -> str:
    # Timestamp first so listing order is oldest first.
    return f"{datetime.now(UTC):%Y%m%dT%H%M%S%f}Z_{sha16(target_path.encode())}"


def _ascii(text: str, limit: int) -> str:
    return text.encode("ascii", "replace").decode()[:limit]


class BlobDeadLetter:
    """Dead-letter store in the Function's own storage account, reachable when Databricks isn't."""

    def __init__(self, account_url: str, container: str) -> None:
        from azure.identity import DefaultAzureCredential
        from azure.storage.blob import ContainerClient

        self._client = ContainerClient(account_url, container, credential=DefaultAzureCredential())

    def save(self, target_path: str, data: bytes, error: str) -> None:
        self._client.upload_blob(
            _key(target_path),
            data,
            metadata={"target_path": _ascii(target_path, 1024), "error": _ascii(error, 300)},
        )

    def pending(self, limit: int) -> list[DeadLetterItem]:
        items: list[DeadLetterItem] = []
        for blob in self._client.list_blobs(include=["metadata"]):
            if len(items) >= limit:
                break
            data = self._client.download_blob(blob.name).readall()
            items.append(DeadLetterItem(blob.name, blob.metadata["target_path"], data))
        return items

    def remove(self, item: DeadLetterItem) -> None:
        self._client.delete_blob(item.key)

    def count(self) -> int:
        return sum(1 for _ in self._client.list_blob_names())


class LocalDeadLetter:
    def __init__(self, folder: Path) -> None:
        self._folder = folder
        folder.mkdir(parents=True, exist_ok=True)

    def save(self, target_path: str, data: bytes, error: str) -> None:
        key = _key(target_path)
        (self._folder / f"{key}.bin").write_bytes(data)
        (self._folder / f"{key}.json").write_text(json.dumps({"target_path": target_path, "error": error}))

    def pending(self, limit: int) -> list[DeadLetterItem]:
        items = []
        for meta in sorted(self._folder.glob("*.json"))[:limit]:
            key = meta.stem
            target = json.loads(meta.read_text())["target_path"]
            items.append(DeadLetterItem(key, target, (self._folder / f"{key}.bin").read_bytes()))
        return items

    def remove(self, item: DeadLetterItem) -> None:
        (self._folder / f"{item.key}.bin").unlink(missing_ok=True)
        (self._folder / f"{item.key}.json").unlink(missing_ok=True)

    def count(self) -> int:
        return len(list(self._folder.glob("*.json")))
