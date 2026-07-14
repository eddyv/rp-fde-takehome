"""Shared factory guardrails: client kwargs, consumer retry loop, exhaustion.

Pins the kwargs and retry arithmetic ONCE for all three binaries (worker,
retrier, sweeper's client) that previously duplicated or left them unasserted
(see test_sweeper.py:118-131 for the pattern this mirrors).
"""

from types import SimpleNamespace

import pytest
from app import infra
from app.config import settings
from kafka.errors import KafkaError


def test_classifier_client_owns_no_retries_and_bounds_each_request(monkeypatch):
    # anthropic_base_url defaults to None, so it must be pinned to a distinct
    # value here -- otherwise a mutation hardcoding base_url=None would be
    # indistinguishable from correctly forwarding settings.anthropic_base_url.
    monkeypatch.setattr(settings, "anthropic_base_url", "https://distinct.example")
    client_kwargs: dict = {}

    def fake_anthropic(**kwargs):
        client_kwargs.update(kwargs)
        return SimpleNamespace()

    monkeypatch.setattr(infra, "Anthropic", fake_anthropic)

    infra.make_classifier_client()

    assert client_kwargs["api_key"] == settings.anthropic_api_key.get_secret_value()
    assert client_kwargs["base_url"] == "https://distinct.example"
    assert client_kwargs["max_retries"] == 0, "classifier.py owns retry/backoff"
    assert client_kwargs["timeout"] == 60.0, "a hung call must not stall the group"


def test_classifier_client_threads_a_custom_timeout(monkeypatch):
    client_kwargs: dict = {}

    def fake_anthropic(**kwargs):
        client_kwargs.update(kwargs)
        return SimpleNamespace()

    monkeypatch.setattr(infra, "Anthropic", fake_anthropic)

    infra.make_classifier_client(timeout=5.0)

    assert client_kwargs["timeout"] == 5.0


def test_make_consumer_retries_until_broker_up_then_returns(monkeypatch):
    # Two brokers, so a mutation to split(",") (e.g. split(None) or
    # split("XX,XX")) yields a different, distinguishable list -- a
    # single-broker default would split identically either way.
    monkeypatch.setattr(settings, "kafka_brokers", "h1:9092,h2:9092")
    sleeps: list = []
    monkeypatch.setattr(infra.time, "sleep", lambda s: sleeps.append(s))

    calls: list = []
    sentinel = SimpleNamespace()

    def fake_consumer(*args, **kwargs):
        calls.append((args, kwargs))
        if len(calls) < 3:
            raise KafkaError("not up yet")
        return sentinel

    monkeypatch.setattr(infra, "KafkaConsumer", fake_consumer)

    result = infra.make_consumer("a-topic", "a-group", retries=5, delay=2.0)

    assert result is sentinel
    assert len(calls) == 3, "two failures then a success"
    assert sleeps == [2.0, 2.0], "one sleep per failed attempt, none after success"

    args, kwargs = calls[0]
    assert args == ("a-topic",)
    assert kwargs["bootstrap_servers"] == ["h1:9092", "h2:9092"]
    assert kwargs["group_id"] == "a-group"
    assert kwargs["enable_auto_commit"] is False
    assert kwargs["auto_offset_reset"] == "earliest"
    assert kwargs["max_poll_interval_ms"] == 600_000 == infra.MAX_POLL_INTERVAL_MS
    # every attempt must carry identical kwargs
    assert all(c == calls[0] for c in calls)


def test_make_consumer_default_retries_and_delay(monkeypatch):
    # Explicit retries/delay kwargs are pinned above; these are the *defaults*
    # (retries=30, delay=2.0), which the other tests never exercise because
    # they always pass explicit values.
    sleeps: list = []
    monkeypatch.setattr(infra.time, "sleep", lambda s: sleeps.append(s))
    calls: list = []

    def always_fails(*args, **kwargs):
        calls.append((args, kwargs))
        raise KafkaError("never up")

    monkeypatch.setattr(infra, "KafkaConsumer", always_fails)

    with pytest.raises(KafkaError, match="never up"):
        infra.make_consumer("a-topic", "a-group")

    assert len(calls) == 30, "default retries must stay 30"
    assert sleeps == [2.0] * 29, "default delay must stay 2.0s, none after the last"


def test_make_consumer_raises_after_exhausting_retries(monkeypatch):
    sleeps: list = []
    monkeypatch.setattr(infra.time, "sleep", lambda s: sleeps.append(s))

    def always_fails(*args, **kwargs):
        raise KafkaError("never up")

    monkeypatch.setattr(infra, "KafkaConsumer", always_fails)

    with pytest.raises(KafkaError, match="never up"):
        infra.make_consumer("a-topic", "a-group", retries=2, delay=1.0)

    assert sleeps == [1.0], "sleep between attempts, none after the last failure"
