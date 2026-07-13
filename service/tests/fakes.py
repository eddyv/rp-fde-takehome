"""Shared test doubles: no network, no broker, no Postgres.

FakeClient replays scripted model outputs; items may be strings (returned as
a text block), Exception instances (raised from create()), block lists, or a
SimpleNamespace built with make_response() to script a custom stop_reason.
Real SDK errors are built by make_status_error().
"""

from types import SimpleNamespace

import anthropic
import httpx
from app import db


def make_status_error(status_code: int) -> anthropic.APIStatusError:
    """Build a real SDK error backed by an httpx.Response, as the SDK would."""
    request = httpx.Request("POST", "https://api.anthropic.com/v1/messages")
    response = httpx.Response(
        status_code, request=request, json={"error": {"message": f"http {status_code}"}}
    )
    if status_code == 429:
        return anthropic.RateLimitError(
            f"http {status_code}", response=response, body=None
        )
    return anthropic.APIStatusError(f"http {status_code}", response=response, body=None)


def make_block(block_type: str, text: str) -> SimpleNamespace:
    """A response content block, for scripting multi-block model replies."""
    return SimpleNamespace(type=block_type, text=text)


def make_response(blocks: list, stop_reason: str = "end_turn") -> SimpleNamespace:
    """A full response, for scripting a custom stop_reason (e.g. refusal)."""
    return SimpleNamespace(content=blocks, stop_reason=stop_reason)


class FakeClient:
    """Stands in for anthropic.Anthropic; replays scripted outputs.

    Each output may be a string (one text block), an Exception (raised from
    create()), a list of blocks built with make_block() for responses that
    mix text and non-text content, or a make_response() SimpleNamespace
    passed through verbatim.
    """

    def __init__(self, outputs: list):
        self.calls: list[str] = []  # prompt contents, in order
        self.kwargs: list[dict] = []  # full create() kwargs, in order
        self._outputs = list(outputs)
        self.messages = self

    def create(self, **kwargs) -> SimpleNamespace:
        self.calls.append(kwargs["messages"][0]["content"])
        self.kwargs.append(kwargs)
        item = self._outputs.pop(0)
        if isinstance(item, Exception):
            raise item
        if isinstance(item, SimpleNamespace):
            return item
        if isinstance(item, list):
            return SimpleNamespace(content=item, stop_reason="end_turn")
        return SimpleNamespace(
            content=[SimpleNamespace(type="text", text=item)], stop_reason="end_turn"
        )


class FakeFuture:
    def __init__(self):
        self.get_timeout = None  # records the ack-wait bound passed to get()

    def get(self, timeout=None) -> SimpleNamespace:
        self.get_timeout = timeout
        return SimpleNamespace(topic="t", partition=0, offset=0)


class FakeProducer:
    """Records sends; shares an event log with FakeConsumer to pin ordering."""

    def __init__(self, log: list | None = None):
        self.sent: list[SimpleNamespace] = []
        self.log = log if log is not None else []

    def send(self, topic, value=None, key=None) -> FakeFuture:
        future = FakeFuture()
        self.sent.append(
            SimpleNamespace(topic=topic, value=value, key=key, future=future)
        )
        self.log.append(("publish", topic))
        return future


class FakeConsumer:
    def __init__(self, log: list | None = None):
        self.commits = 0
        self.log = log if log is not None else []

    def commit(self, offsets=None) -> None:
        self.commits += 1
        self.log.append(("commit",))


class FakeConn:
    """Records SQL; shares the ordering log; optionally raises on execute.

    `statuses` maps edit ids to row statuses for the worker's redelivery
    pre-check; ids not present read as an absent row. Status reads land in
    `status_reads`, not `executed`/`log` (the log pins the ordering of side
    effects, which a read is not), and they bypass `fail_with` so failure
    injection keeps hitting the write even though the pre-check runs first.
    """

    def __init__(
        self,
        log: list | None = None,
        fail_with: Exception | None = None,
        statuses: dict[str, str] | None = None,
    ):
        self.executed: list[tuple[str, dict]] = []
        self.status_reads: list[str] = []
        self.log = log if log is not None else []
        self.fail_with = fail_with
        self.statuses = statuses if statuses is not None else {}

    def execute(self, sql, params=None):
        if sql is db.STATUS_SQL:
            self.status_reads.append(params["id"])
            row = (
                (self.statuses[params["id"]],)
                if params["id"] in self.statuses
                else None
            )
            return SimpleNamespace(fetchone=lambda: row)
        if self.fail_with is not None:
            raise self.fail_with
        self.executed.append((sql, params))
        self.log.append(("db",))


def make_message(
    value: bytes, topic: str = "wiki.edits.raw", partition: int = 0, offset: int = 7
) -> SimpleNamespace:
    return SimpleNamespace(topic=topic, partition=partition, offset=offset, value=value)
