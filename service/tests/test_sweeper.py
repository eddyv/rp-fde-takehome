"""Sweeper drain semantics: snapshot boundary, per-message commit, routing.

main() is driven end-to-end with fakes injected via monkeypatch; the fake
consumer replays scripted DLQ records and records commits/pauses.
"""

import base64
import json

import psycopg
import pytest
from app import db, failures, infra, sweeper
from app.config import settings
from kafka import TopicPartition
from kafka.structs import OffsetAndMetadata

from tests.fakes import (
    FakeClient,
    FakeConn,
    FakeProducer,
    make_message,
    make_status_error,
)

EDIT = {"id": "7", "title": "Z", "comment": "", "byte_delta": 12}
GOOD_JSON = '{"label": "vandalism", "confidence": 0.95, "reasoning": "blanked"}'
FIRST_FAILED = "2020-01-01T00:00:00+00:00"
TP = TopicPartition(settings.kafka_dlq_topic, 0)


class FakeSweeperConsumer:
    """Replays scripted records; snapshots, commits, and pauses like the real one."""

    def __init__(self, messages, end_offset: int | None = None, snapshot_empty=False):
        self.messages = list(messages)
        self.committed: dict = {}
        self.paused: list = []
        self.partition_queries: list = []
        self._end_offset = end_offset
        self._snapshot_empty = snapshot_empty

    def __iter__(self):
        return iter(self.messages)

    def partitions_for_topic(self, topic):
        self.partition_queries.append(topic)
        return {message.partition for message in self.messages} or None

    def end_offsets(self, partitions):
        if self._snapshot_empty:
            return {}
        if self._end_offset is not None:
            return {tp: self._end_offset for tp in partitions}
        return {
            tp: max(m.offset for m in self.messages if m.partition == tp.partition) + 1
            for tp in partitions
        }

    def pause(self, tp):
        self.paused.append(tp)

    def commit(self, offsets):
        self.committed.update(offsets)


def dlq_message(offset: int = 0, **overrides) -> object:
    envelope = {
        "schema": 1,
        "reason": failures.REASON_PARSE_FAILED,
        "error": "e",
        "source": "worker",
        "attempts": 2,
        "first_failed_at": FIRST_FAILED,
        "last_failed_at": FIRST_FAILED,
        "edit": EDIT,
        "kafka": {"topic": "wiki.edits.raw", "partition": 0, "offset": 1},
    }
    envelope.update(overrides)
    return make_message(
        json.dumps(envelope).encode(), topic=settings.kafka_dlq_topic, offset=offset
    )


def run_sweeper(monkeypatch, consumer, client, conn=None, argv=(), client_kwargs=None):
    def fake_anthropic(**kwargs):
        if client_kwargs is not None:
            client_kwargs.update(kwargs)
        return client

    monkeypatch.setattr("sys.argv", ["sweeper", *argv])
    monkeypatch.setattr(infra, "Anthropic", fake_anthropic)
    monkeypatch.setattr(db, "connect", lambda: conn if conn is not None else FakeConn())
    monkeypatch.setattr(sweeper, "make_consumer", lambda: consumer)
    producer = FakeProducer()
    monkeypatch.setattr(failures, "make_producer", lambda: producer)
    sweeper.main()
    return producer


def test_reclassify_success_upserts_and_commits_past_the_record(monkeypatch):
    conn = FakeConn()
    consumer = FakeSweeperConsumer([dlq_message(offset=3)])
    client = FakeClient([GOOD_JSON])

    producer = run_sweeper(monkeypatch, consumer, client, conn=conn)

    [(sql, params)] = conn.executed
    assert params["status"] == "classified"
    assert params["label"] == "vandalism"
    assert params["id"] == "7"
    assert producer.sent == []
    assert consumer.partition_queries == [settings.kafka_dlq_topic]
    assert consumer.committed[TP] == OffsetAndMetadata(4, "", -1), (
        "commit is the explicit next offset, not the consumer position"
    )


def test_client_owns_no_retries_and_bounds_each_request(monkeypatch):
    client_kwargs: dict = {}
    consumer = FakeSweeperConsumer([dlq_message()])

    run_sweeper(
        monkeypatch,
        consumer,
        FakeClient([GOOD_JSON]),
        client_kwargs=client_kwargs,
    )

    assert client_kwargs["api_key"] == settings.anthropic_api_key.get_secret_value()
    assert client_kwargs["max_retries"] == 0, "classifier.py owns retry/backoff"
    assert client_kwargs["timeout"] == 60.0, "a hung call must not stall the drain"


def test_default_model_falls_back_to_settings(monkeypatch):
    client = FakeClient([GOOD_JSON])
    consumer = FakeSweeperConsumer([dlq_message()])

    run_sweeper(monkeypatch, consumer, client)

    assert client.kwargs[0]["model"] == settings.anthropic_model


def test_sweeper_model_setting_beats_the_default(monkeypatch):
    monkeypatch.setattr(settings, "sweeper_model", "claude-opus-4-8")
    client = FakeClient([GOOD_JSON])
    consumer = FakeSweeperConsumer([dlq_message()])

    run_sweeper(monkeypatch, consumer, client)

    assert client.kwargs[0]["model"] == "claude-opus-4-8"


def test_model_flag_overrides_the_classifier_model(monkeypatch):
    client = FakeClient([GOOD_JSON])
    consumer = FakeSweeperConsumer([dlq_message()])

    run_sweeper(monkeypatch, consumer, client, argv=["--model", "claude-sonnet-4-5"])

    assert client.kwargs[0]["model"] == "claude-sonnet-4-5"


def test_still_transient_requeues_to_dlq_tail_with_attempts_bumped(monkeypatch):
    consumer = FakeSweeperConsumer([dlq_message(offset=0)])
    client = FakeClient([make_status_error(429)] * 3)

    producer = run_sweeper(monkeypatch, consumer, client)

    [sent] = producer.sent
    assert sent.topic == settings.kafka_dlq_topic
    assert sent.value["reason"] == "transient_exhausted"
    assert sent.value["source"] == "sweeper"
    assert sent.value["error"] == "http 429"
    assert sent.value["attempts"] == 3
    assert sent.value["first_failed_at"] == FIRST_FAILED
    assert sent.value["edit"] == EDIT
    assert consumer.committed[TP].offset == 1, "requeued original is consumed"


def test_still_unparseable_requeues_as_parse_failed(monkeypatch):
    consumer = FakeSweeperConsumer([dlq_message()])
    client = FakeClient(["no json"])

    producer = run_sweeper(monkeypatch, consumer, client)

    [sent] = producer.sent
    assert sent.value["reason"] == "parse_failed"
    assert sent.value["source"] == "sweeper"


def test_config_error_aborts_the_sweep_uncommitted(monkeypatch):
    consumer = FakeSweeperConsumer([dlq_message()])
    client = FakeClient([make_status_error(401)])

    with pytest.raises(SystemExit) as excinfo:
        run_sweeper(monkeypatch, consumer, client)

    assert excinfo.value.code == 1
    assert consumer.committed == {}, "the record must be retried by the next sweep"


def test_malformed_reason_is_skipped_with_raw_surfaced(monkeypatch):
    raw = base64.b64encode(b"original bytes").decode("ascii")
    consumer = FakeSweeperConsumer(
        [dlq_message(reason=failures.REASON_MALFORMED, raw=raw)]
    )
    client = FakeClient([])

    producer = run_sweeper(monkeypatch, consumer, client)

    assert client.calls == [], "nothing classifiable in a malformed envelope"
    assert producer.sent == []
    assert consumer.committed[TP].offset == 1


def test_bad_raw_field_does_not_abort_the_sweep(monkeypatch):
    consumer = FakeSweeperConsumer(
        [dlq_message(reason=failures.REASON_MALFORMED, raw="not base64!!!")]
    )

    run_sweeper(monkeypatch, consumer, FakeClient([]))

    assert consumer.committed[TP].offset == 1, "a bad raw field must not wedge"


def test_undecodable_dlq_record_is_skipped_and_committed(monkeypatch):
    message = make_message(b"garbage", topic=settings.kafka_dlq_topic, offset=5)
    consumer = FakeSweeperConsumer([message])

    run_sweeper(monkeypatch, consumer, FakeClient([]))

    assert consumer.committed[TP].offset == 6


def test_unexpected_envelope_schema_version_is_skipped_and_committed(monkeypatch):
    consumer = FakeSweeperConsumer([dlq_message(schema=2)])
    client = FakeClient([])  # any classify call would blow up the fake

    producer = run_sweeper(monkeypatch, consumer, client)

    assert client.calls == [], "unknown versions must be skipped before classify"
    assert producer.sent == []
    assert consumer.committed[TP].offset == 1, "skipped like an undecodable record"


def test_envelope_without_usable_edit_is_skipped(monkeypatch):
    consumer = FakeSweeperConsumer([dlq_message(edit={"title": "no id"})])
    client = FakeClient([])

    run_sweeper(monkeypatch, consumer, client)

    assert client.calls == []
    assert consumer.committed[TP].offset == 1


def test_records_at_or_past_the_snapshot_are_paused_not_committed(monkeypatch):
    consumer = FakeSweeperConsumer([dlq_message(offset=0)], end_offset=0)
    client = FakeClient([])

    run_sweeper(monkeypatch, consumer, client)

    assert consumer.paused == [TP]
    assert consumer.committed == {}, "the tail stays for the next sweep"
    assert client.calls == []


def test_limit_stops_processing_and_leaves_the_rest_uncommitted(monkeypatch):
    consumer = FakeSweeperConsumer(
        [dlq_message(offset=0), dlq_message(offset=1), dlq_message(offset=2)]
    )
    client = FakeClient([GOOD_JSON, GOOD_JSON])

    run_sweeper(monkeypatch, consumer, client, argv=["--limit", "2"])

    assert len(client.calls) == 2, "limit counts processed records exactly"
    assert consumer.committed[TP].offset == 2, "only consumed records are committed"


def test_one_bad_record_does_not_end_the_sweep(monkeypatch):
    conn = FakeConn()
    consumer = FakeSweeperConsumer(
        [
            make_message(b"garbage", topic=settings.kafka_dlq_topic, offset=0),
            dlq_message(reason=failures.REASON_MALFORMED, edit=None, offset=1),
            dlq_message(offset=2),
        ]
    )
    client = FakeClient([GOOD_JSON])

    run_sweeper(monkeypatch, consumer, client, conn=conn)

    assert len(client.calls) == 1, "the classifiable record still gets swept"
    [(sql, params)] = conn.executed
    assert params["status"] == "classified"
    assert consumer.committed[TP].offset == 3, "skips commit too, drain continues"


def test_requeued_record_does_not_end_the_sweep(monkeypatch):
    conn = FakeConn()
    consumer = FakeSweeperConsumer([dlq_message(offset=0), dlq_message(offset=1)])
    client = FakeClient([make_status_error(429)] * 3 + [GOOD_JSON])

    producer = run_sweeper(monkeypatch, consumer, client, conn=conn)

    assert len(producer.sent) == 1, "first record requeued"
    [(sql, params)] = conn.executed
    assert params["status"] == "classified", "second record still swept"
    assert consumer.committed[TP].offset == 2


def test_missing_end_offset_snapshot_pauses_instead_of_processing(monkeypatch):
    # A partition with no snapshot entry must be treated as boundary 0.
    consumer = FakeSweeperConsumer([dlq_message(offset=0)], snapshot_empty=True)
    client = FakeClient([])

    run_sweeper(monkeypatch, consumer, client)

    assert consumer.paused == [TP]
    assert client.calls == []
    assert consumer.committed == {}


def test_schema_mismatch_rows_are_skipped_so_the_dlq_stays_drainable(monkeypatch):
    conn = FakeConn(fail_with=psycopg.DataError("invalid input for type integer"))
    consumer = FakeSweeperConsumer([dlq_message(offset=0), dlq_message(offset=1)])

    producer = run_sweeper(
        monkeypatch, consumer, FakeClient([GOOD_JSON, GOOD_JSON]), conn=conn
    )

    assert producer.sent == []
    assert consumer.committed[TP].offset == 2, "both skipped, neither wedges the run"
