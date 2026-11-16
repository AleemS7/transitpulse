"""Natural-language questions answered by the Genie space over the gold tables.

An answer always carries the SQL Genie ran and the rows it got back, so every number in the text can be
checked against a query result. If Genie produced no query, the answer says so instead of guessing.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any, Protocol

MAX_ROWS = 100


@dataclass(frozen=True)
class Answer:
    status: str
    text: str | None
    sql: str | None
    columns: list[str] = field(default_factory=list)
    rows: list[list[Any]] = field(default_factory=list)
    conversation_id: str | None = None


class AskError(Exception):
    pass


class Asker(Protocol):
    def ask(self, question: str) -> Answer: ...


@dataclass
class FakeAsker:
    answer: Answer | None = None
    fail: bool = False
    questions: list[str] = field(default_factory=list)

    def ask(self, question: str) -> Answer:
        self.questions.append(question)
        if self.fail:
            raise AskError("genie unavailable")
        return self.answer or Answer(status="COMPLETED", text=None, sql=None)


class GenieAsker:
    """Calls the Genie Conversation API as the app's service principal."""

    def __init__(self, space_id: str, timeout: timedelta = timedelta(seconds=60)) -> None:
        from databricks.sdk import WorkspaceClient

        self._w = WorkspaceClient()
        self._space_id = space_id
        self._timeout = timeout

    def ask(self, question: str) -> Answer:
        try:
            msg = self._w.genie.start_conversation_and_wait(self._space_id, question, timeout=self._timeout)
        except Exception as exc:
            raise AskError(str(exc)) from exc
        status = msg.status.value if msg.status else "UNKNOWN"
        text: str | None = None
        sql: str | None = None
        columns: list[str] = []
        rows: list[list[Any]] = []
        for att in msg.attachments or []:
            if att.text and att.text.content:
                text = att.text.content
            if att.query and att.query.query and att.attachment_id:
                sql = att.query.query
                result = self._w.genie.get_message_attachment_query_result(
                    self._space_id, msg.conversation_id, msg.message_id, att.attachment_id
                ).statement_response
                if result and result.manifest and result.manifest.schema and result.manifest.schema.columns:
                    columns = [c.name or "" for c in result.manifest.schema.columns]
                if result and result.result and result.result.data_array:
                    rows = [list(r) for r in result.result.data_array[:MAX_ROWS]]
        return Answer(status, text, sql, columns, rows, msg.conversation_id)
