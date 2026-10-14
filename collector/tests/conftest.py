from __future__ import annotations

import socket
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture(autouse=True)
def _no_network(monkeypatch: pytest.MonkeyPatch) -> None:
    def refuse(*args: object, **kwargs: object) -> None:
        raise RuntimeError("network access in unit tests")

    monkeypatch.setattr(socket.socket, "connect", refuse)


@pytest.fixture
def vehicle_pb() -> bytes:
    return (FIXTURES / "vehiclepositions.pb").read_bytes()


@pytest.fixture
def trip_updates_pb() -> bytes:
    return (FIXTURES / "tripupdates.pb").read_bytes()


@pytest.fixture
def rail_json() -> bytes:
    return (FIXTURES / "rail_arrivals.json").read_bytes()
