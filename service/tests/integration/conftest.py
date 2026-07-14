"""Integration harness: real Redpanda + Postgres (+ Ollama for the llm-marked
test), driven via testcontainers.

Deselected by default (see pyproject.toml `addopts = "-m 'not integration'"`).
Run with:

    uv run pytest -m "integration and not llm"   # Redpanda + Postgres only
    uv run pytest -m integration                  # also the Ollama E2E test

All `integration`-marked items are skipped outright if Docker is unreachable,
before any fixture (and therefore any container) is touched.

Containers are session-scoped and reused across tests; isolation comes from
giving every test its own topic names and consumer groups (`wire_settings`)
rather than from restarting containers.
"""

import json
import os
import time
import uuid
from pathlib import Path

import docker
import pytest
from app import db, failures, retrier, worker
from app.config import settings
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
from testcontainers.kafka import RedpandaContainer
from testcontainers.postgres import PostgresContainer

from tests.fakes import make_message

REPO_ROOT = Path(__file__).resolve().parents[3]
SCHEMA_SQL = REPO_ROOT / "sql" / "schema.sql"

# Match the compose stack's pinned tag rather than testcontainers' ancient
# default image.
REDPANDA_IMAGE = "docker.redpanda.com/redpandadata/redpanda:v25.3.15"

# testcontainers' default (ollama/ollama:0.1.44) predates the Anthropic-compat
# /v1/messages endpoint (added in 0.12); a 404 from it masquerades as a
# ModelConfigError. Pin something modern and verify below.
OLLAMA_IMAGE = "ollama/ollama:0.31.2"
OLLAMA_MIN_VERSION = (0, 12)

OLLAMA_MODEL = os.environ.get("OLLAMA_TEST_MODEL", "llama3.2:1b")

POLL_TIMEOUT_SECONDS = 30


def _docker_available() -> bool:
    try:
        docker.from_env().ping()
        return True
    except Exception:
        return False


def pytest_collection_modifyitems(config, items):
    if _docker_available():
        return
    skip_docker = pytest.mark.skip(reason="Docker is not available")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_docker)


# --------------------------------------------------------------------------
# Session-scoped containers
# --------------------------------------------------------------------------


@pytest.fixture(scope="session")
def kafka_bootstrap():
    with RedpandaContainer(REDPANDA_IMAGE) as redpanda:
        yield redpanda.get_bootstrap_server()


@pytest.fixture(scope="session")
def postgres_dsn():
    with PostgresContainer("postgres:16", driver=None) as postgres:
        dsn = postgres.get_connection_url()
        engine = create_engine(db.normalize_dsn(dsn), isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.exec_driver_sql(SCHEMA_SQL.read_text())  # multi-statement DDL
        engine.dispose()
        yield dsn


@pytest.fixture(scope="session")
def ollama_base_url():
    """Prefer a host Ollama (OLLAMA_BASE_URL) that already has the model
    pulled — the ~13 GB gpt-oss:20b pull is a poor fit for a per-run
    container. Falls back to a container that bind-mounts ~/.ollama so a
    once-pulled model is reused across runs."""
    host_url = os.environ.get("OLLAMA_BASE_URL")
    if host_url:
        import httpx

        try:
            response = httpx.get(f"{host_url}/api/tags", timeout=5.0)
            response.raise_for_status()
        except httpx.HTTPError:
            pytest.skip(f"OLLAMA_BASE_URL={host_url} is not reachable")
        _require_anthropic_capable(host_url)
        names = {m.get("name") for m in response.json().get("models", [])}
        matches = (n == OLLAMA_MODEL or n.startswith(f"{OLLAMA_MODEL}-") for n in names)
        if not any(matches):
            pytest.skip(
                f"model {OLLAMA_MODEL!r} is not pulled on host Ollama "
                f"({host_url}); run `ollama pull {OLLAMA_MODEL}` or set "
                f"OLLAMA_TEST_MODEL to a model you have"
            )
        yield host_url
        return

    from testcontainers.ollama import OllamaContainer

    with OllamaContainer(OLLAMA_IMAGE, ollama_home=Path.home() / ".ollama") as ollama:
        _require_anthropic_capable(ollama.get_endpoint())
        existing = {m.get("name") for m in ollama.list_models()}
        if not any(
            n == OLLAMA_MODEL or n.startswith(f"{OLLAMA_MODEL}-") for n in existing
        ):
            ollama.pull_model(OLLAMA_MODEL)
        yield ollama.get_endpoint()


def _require_anthropic_capable(base_url: str) -> None:
    """Fail loudly if the Ollama server predates /v1/messages: against an old
    server the anthropic client gets a 404, which classify() maps to
    ModelConfigError -> SystemExit and the E2E test would silently skip."""
    import httpx

    version = httpx.get(f"{base_url}/api/version", timeout=5.0).json()["version"]
    parts = tuple(int(p) for p in version.split(".")[:2] if p.isdigit())
    if parts < OLLAMA_MIN_VERSION:
        pytest.fail(
            f"Ollama {version} at {base_url} predates the Anthropic-compat "
            f"/v1/messages endpoint (needs >= {'.'.join(map(str, OLLAMA_MIN_VERSION))})"
        )


# --------------------------------------------------------------------------
# Per-test isolation
# --------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def wire_settings(request, kafka_bootstrap, postgres_dsn, monkeypatch):
    """Point the `settings` singleton at the containers, with per-test-unique
    topic/group names so tests never see each other's messages despite
    sharing one broker for the whole session."""
    suffix = uuid.uuid4().hex[:8]
    monkeypatch.setattr(settings, "kafka_brokers", kafka_bootstrap)
    monkeypatch.setattr(settings, "kafka_topic", f"wiki.edits.raw.{suffix}")
    monkeypatch.setattr(settings, "kafka_retry_topic", f"wiki.edits.retry.{suffix}")
    monkeypatch.setattr(settings, "kafka_dlq_topic", f"wiki.edits.dlq.{suffix}")
    monkeypatch.setattr(settings, "consumer_group", f"reasoning-service.{suffix}")
    monkeypatch.setattr(
        settings, "retrier_consumer_group", f"reasoning-service-retrier.{suffix}"
    )
    monkeypatch.setattr(
        settings, "sweeper_consumer_group", f"reasoning-service-sweeper.{suffix}"
    )
    monkeypatch.setattr(settings, "postgres_dsn", postgres_dsn)
    monkeypatch.setattr(settings, "retry_backoff_base_seconds", 0)

    admin = KafkaAdminClient(bootstrap_servers=kafka_bootstrap.split(","))
    try:
        topics = [
            NewTopic(name=settings.kafka_topic, num_partitions=1, replication_factor=1),
            NewTopic(
                name=settings.kafka_retry_topic, num_partitions=1, replication_factor=1
            ),
            NewTopic(
                name=settings.kafka_dlq_topic, num_partitions=1, replication_factor=1
            ),
        ]
        try:
            admin.create_topics(topics)
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()


@pytest.fixture
def pg_conn(postgres_dsn):
    """Real SQLAlchemy connection (AUTOCOMMIT); table truncated per test."""
    engine = create_engine(
        db.normalize_dsn(postgres_dsn),
        isolation_level="AUTOCOMMIT",
        poolclass=NullPool,
    )
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE edits"))
        yield conn
    engine.dispose()


# --------------------------------------------------------------------------
# Harness helpers
# --------------------------------------------------------------------------


def fetch_edit_row(pg_conn, edit_id: str) -> dict | None:
    row = (
        pg_conn.execute(text("SELECT * FROM edits WHERE id = :id"), {"id": edit_id})
        .mappings()
        .fetchone()
    )
    return dict(row) if row is not None else None


def count_edits(pg_conn) -> int:
    return pg_conn.execute(text("SELECT count(*) FROM edits")).scalar_one()


def make_raw_producer() -> KafkaProducer:
    """A producer with no value serializer, for pushing raw bytes (malformed
    payloads included) the way an upstream Bloblang pipeline would."""
    brokers = settings.kafka_brokers.split(",")
    return KafkaProducer(bootstrap_servers=brokers, acks="all")


def produce(topic: str, value: bytes, key: bytes | None = None) -> None:
    producer = make_raw_producer()
    try:
        producer.send(topic, value=value, key=key).get(timeout=30)
    finally:
        producer.close()


def seed_envelope(topic: str, edit: dict, **envelope_kwargs) -> dict:
    """Publish a retry/DLQ-shaped envelope built via failures.make_envelope
    (envelope_kwargs must supply `reason` and `error`; see its signature)."""
    fake_message = make_message(b"", topic=topic, partition=0, offset=0)
    envelope = failures.make_envelope(
        source="test", message=fake_message, edit=edit, **envelope_kwargs
    )
    produce(topic, json.dumps(envelope).encode(), key=str(edit["id"]).encode())
    return envelope


def poll_one(consumer: KafkaConsumer, timeout: float = POLL_TIMEOUT_SECONDS):
    """Poll until at least one record is available (group join + rebalance
    can take real seconds) or raise once the deadline passes."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        batches = consumer.poll(timeout_ms=1000)
        for records in batches.values():
            if records:
                return records[0]
    topics = consumer.subscription()
    raise TimeoutError(f"no message received on {topics} within {timeout}s")


def read_envelopes(
    topic: str, timeout: float = POLL_TIMEOUT_SECONDS, allow_empty: bool = False
) -> list[dict]:
    """Read whatever envelopes are currently on `topic` (a fresh reader group,
    from the beginning). With `allow_empty=True`, a timeout yields `[]`
    instead of raising -- useful when a message could legitimately have
    landed on one of several topics (see test_pipeline_e2e's dual branch)."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_brokers.split(","),
        group_id=f"test-reader-{uuid.uuid4().hex[:8]}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    try:
        try:
            first = poll_one(consumer, timeout=timeout)
        except TimeoutError:
            if allow_empty:
                return []
            raise
        envelopes = [json.loads(first.value)]
        for records in consumer.poll(timeout_ms=1000).values():
            envelopes.extend(json.loads(record.value) for record in records)
        return envelopes
    finally:
        consumer.close()


def read_records(
    topic: str, expected_count: int, timeout: float = POLL_TIMEOUT_SECONDS
):
    """Read up to `expected_count` raw ConsumerRecords from `topic` (fresh
    reader group, from the beginning), polling until the count is reached or
    the deadline passes.

    Unlike `read_envelopes`, this does NOT rely on the poll-once-more shortcut
    (which stops after the first record plus one extra 1s poll) and it hands
    back the records themselves -- with `.offset` and raw `.value` -- so callers
    can reason about broker positions. The sweeper drain needs both: several
    envelopes at once, and where they sit relative to the snapshot boundary.
    Single-partition topics (the harness default) yield records in offset order.
    """
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=settings.kafka_brokers.split(","),
        group_id=f"test-reader-{uuid.uuid4().hex[:8]}",
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    try:
        records = []
        deadline = time.monotonic() + timeout
        while len(records) < expected_count and time.monotonic() < deadline:
            for batch in consumer.poll(timeout_ms=1000).values():
                records.extend(batch)
        return records
    finally:
        consumer.close()


def end_offset(topic: str, partition: int = 0) -> int:
    """The current high-water (end) offset for one partition -- i.e. the
    snapshot boundary the sweeper takes at start. Uses `assign` (not
    `subscribe`) so there is no group rebalance to wait on."""
    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_brokers.split(","),
        group_id=f"test-reader-{uuid.uuid4().hex[:8]}",
        enable_auto_commit=False,
    )
    try:
        tp = TopicPartition(topic, partition)
        consumer.assign([tp])
        return consumer.end_offsets([tp])[tp]
    finally:
        consumer.close()


def committed_offset(group_id: str, topic: str, partition: int = 0):
    """The broker-side committed offset for `group_id` on one partition, read
    with a throwaway consumer that assigns the partition directly. Returns None
    if the group has never committed."""
    consumer = KafkaConsumer(
        bootstrap_servers=settings.kafka_brokers.split(","),
        group_id=group_id,
        enable_auto_commit=False,
    )
    try:
        tp = TopicPartition(topic, partition)
        consumer.assign([tp])
        return consumer.committed(tp)
    finally:
        consumer.close()


def run_worker_once(client):
    """Build real collaborators from production factories, process exactly
    one message from `settings.kafka_topic`, and report what happened."""
    conn = db.connect()
    consumer = worker.make_consumer()
    producer = failures.make_producer()
    breaker = failures.CircuitBreaker(settings.breaker_threshold)
    try:
        message = poll_one(consumer)
        worker.handle_message(client, conn, consumer, producer, breaker, message)
        committed = consumer.committed(TopicPartition(message.topic, message.partition))
        return message, committed
    finally:
        consumer.close()
        producer.close()
        conn.close()


def run_retrier_once(client):
    """Same shape as run_worker_once, for `settings.kafka_retry_topic`."""
    conn = db.connect()
    consumer = retrier.make_consumer()
    producer = failures.make_producer()
    breaker = failures.CircuitBreaker(settings.breaker_threshold)
    try:
        message = poll_one(consumer)
        retrier.handle_envelope(client, conn, consumer, producer, breaker, message)
        committed = consumer.committed(TopicPartition(message.topic, message.partition))
        return message, committed
    finally:
        consumer.close()
        producer.close()
        conn.close()
