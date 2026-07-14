# Manual Testing (Humans)

Everything runs except `connect` (Redpanda Connect, the Wikimedia SSE ingest
pipeline) — with it down there's no live feed, so edits are hand-produced
straight onto `wiki.edits.raw` instead. Same `docker run`-against-the-compose-
network pattern as the operator runbook in [`DESIGN.md`](DESIGN.md). Every
command below is self-contained — copy/paste blocks in order, no setup step
required first.

```sh
docker compose up --build -d redpanda topic-init postgres worker retrier api
```

`id` is the only required field (`sql/schema.sql` — everything else is
nullable); the `id`s below (`manual-1`, `manual-pg-1`, ...) are unique
per scenario so re-running one doesn't collide with a prior UPSERT.

## 1. Happy path

```sh
echo '{"id":"manual-1","title":"Test Article","user":"1.2.3.4","comment":"fixed a typo","byte_delta":3,"event_time":"2026-07-14T00:00:00Z"}' \
  | docker run --rm -i --network redpanda-fde-takehome_default \
    docker.redpanda.com/redpandadata/redpanda:v25.3.15 \
    topic produce wiki.edits.raw -X brokers=redpanda:9092
```

```sh
docker compose logs -f worker   # edit manual-1 <label> -> classified
```

```sh
uv run --directory service edits-tui
```

## 2. Bad input — malformed messages

Each of these is parked straight to `wiki.edits.dlq` (`reason=malformed`)
without ever calling the classifier:

```sh
echo 'not json at all' \
  | docker run --rm -i --network redpanda-fde-takehome_default \
    docker.redpanda.com/redpandadata/redpanda:v25.3.15 \
    topic produce wiki.edits.raw -X brokers=redpanda:9092
```

```sh
echo '[1,2,3]' \
  | docker run --rm -i --network redpanda-fde-takehome_default \
    docker.redpanda.com/redpandadata/redpanda:v25.3.15 \
    topic produce wiki.edits.raw -X brokers=redpanda:9092
```

```sh
echo '{"title":"no id here"}' \
  | docker run --rm -i --network redpanda-fde-takehome_default \
    docker.redpanda.com/redpandadata/redpanda:v25.3.15 \
    topic produce wiki.edits.raw -X brokers=redpanda:9092
```

```sh
docker compose logs worker | grep "malformed message"
```

```sh
docker run --rm -it --network redpanda-fde-takehome_default \
  docker.redpanda.com/redpandadata/redpanda:v25.3.15 \
  topic consume wiki.edits.dlq -X brokers=redpanda:9092
```

## 3. Simulated network failure -> retry -> DLQ

Point one-off `worker`/`retrier` containers at an address nothing listens
on, to force `APIConnectionError` -> `ModelUnavailableError` without
touching real API quota. Fast backoff overrides keep the whole arc under a
minute.

Stop the standing services first:

```sh
docker compose stop worker retrier
```

Run the worker in its own terminal, leave it running:

```sh
docker compose run --rm -e ANTHROPIC_BASE_URL=http://127.0.0.1:1 \
  -e RETRY_BACKOFF_BASE_SECONDS=5 -e RETRY_BACKOFF_MAX_SECONDS=10 \
  worker python -m app.worker
```

Run the retrier in another terminal, leave it running:

```sh
docker compose run --rm -e ANTHROPIC_BASE_URL=http://127.0.0.1:1 \
  -e RETRY_BACKOFF_BASE_SECONDS=5 -e RETRY_BACKOFF_MAX_SECONDS=10 \
  retrier python -m app.retrier
```

In a third terminal, produce an edit onto `wiki.edits.raw`:

```sh
echo '{"id":"manual-net-1","title":"Net Test","user":"1.2.3.4","comment":"test","byte_delta":10,"event_time":"2026-07-14T00:00:00Z"}' \
  | docker run --rm -i --network redpanda-fde-takehome_default \
    docker.redpanda.com/redpandadata/redpanda:v25.3.15 \
    topic produce wiki.edits.raw -X brokers=redpanda:9092
```

Watch: the worker exhausts 3 bounded attempts and parks to
`wiki.edits.retry` (`status='failed'` + breaker++); the retrier picks it up,
retries, and once `1 + MAX_RETRY_PASSES` (default 3) attempts are spent,
promotes it to `wiki.edits.dlq` with `reason=retries_exhausted`.

```sh
curl "http://localhost:8000/edits?status=failed"
```

Ctrl-C both one-off containers, then restore normal service:

```sh
docker compose up -d worker retrier
```

## 4. Postgres connection failure

Warm-up retry loop (`connect()`'s startup path). `--no-deps` is required —
plain `docker compose up worker` honors `depends_on: condition:
service_healthy` and silently starts Postgres for you first.

```sh
docker compose stop postgres worker
```

```sh
docker compose run -d --rm --no-deps --name worker-warmuptest worker python -m app.worker
```

```sh
docker logs -f worker-warmuptest  # "postgres not ready (...), retrying..." every 2s
```

```sh
docker compose start postgres     # connects once healthy, no crash
```

```sh
docker stop worker-warmuptest && docker compose up -d worker  # back to normal
```

Mid-flight drop (worker already running normally) — produce an edit right
after issuing the restart so the write lands mid-restart, not before or
after:

```sh
docker compose restart postgres & sleep 1
echo '{"id":"manual-pg-1","title":"PG Test","user":"1.2.3.4","comment":"test","byte_delta":5,"event_time":"2026-07-14T00:00:00Z"}' \
  | docker run --rm -i --network redpanda-fde-takehome_default \
    docker.redpanda.com/redpandadata/redpanda:v25.3.15 \
    topic produce wiki.edits.raw -X brokers=redpanda:9092
```

```sh
docker compose logs -f worker  # "postgres connection lost, reconnecting", then classifies normally
```

Offsets only commit after a successful write, so at worst an in-flight
message is redelivered, never dropped.

## 5. Sweeping

With at least one entry parked on the DLQ (from scenario 2 or 3), drain it:

```sh
docker compose run --rm worker python -m app.sweeper --model claude-sonnet-4-5
```

Watch the `reclassified/requeued/skipped` summary log line, then confirm a
previously-`failed` row flips to `classified`:

```sh
curl "http://localhost:8000/edits?status=classified"
```

Lowering `BREAKER_THRESHOLD` alongside scenario 3's bad `ANTHROPIC_BASE_URL`
demonstrates the circuit breaker crash-loop and Docker's restart-backoff
half-open probe, if you want to go one step further.
