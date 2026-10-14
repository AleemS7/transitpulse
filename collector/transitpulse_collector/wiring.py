from __future__ import annotations

from pathlib import Path

from transitpulse_collector.deadletter import BlobDeadLetter, DeadLetter, LocalDeadLetter
from transitpulse_collector.runner import Collector, Deps
from transitpulse_collector.settings import Settings, SettingsError
from transitpulse_collector.sinks import DatabricksFilesSink, LocalSink, Sink
from transitpulse_collector.transport import RequestsTransport


def build_collector(settings: Settings, *, local_out: Path | None = None) -> Collector:
    """Databricks + Blob dead-letter by default; a local folder for both when local_out is given."""
    transport = RequestsTransport()
    sink: Sink
    deadletter: DeadLetter
    if local_out is not None:
        sink = LocalSink(local_out)
        deadletter = LocalDeadLetter(local_out / "_deadletter")
    else:
        if not settings.databricks_host or not settings.databricks_token:
            raise SettingsError("TP_DATABRICKS_HOST and TP_DATABRICKS_TOKEN are required")
        sink = DatabricksFilesSink(settings.databricks_host, settings.databricks_token, transport)
        if settings.deadletter_account_url:
            deadletter = BlobDeadLetter(settings.deadletter_account_url, settings.deadletter_container)
        else:
            deadletter = LocalDeadLetter(Path(".deadletter"))
    return Collector(Deps(settings=settings, transport=transport, sink=sink, deadletter=deadletter))
