# Wikipedia Edit Classifier

Wikipedia recent-changes SSE firehose → Redpanda Connect (filter/project) →
Redpanda topic `wiki.edits.raw` → Python reasoning service (Claude LLM loop) →
Postgres → thin JSON API.

```
Wikimedia SSE ──> Redpanda Connect ──> wiki.edits.raw ──> worker (Claude) ──> Postgres ──> GET /edits
                  (plumbing only:                          (classify each        │
                  strip SSE frames,                        edit: vandalism /     │
                  filter bots + non-                       substantive /         │
                  articles, project                        trivia / unclear)     │
                  small schema)                                 │                │
                                          transient failures    │   exhausted /  │
                                                                v   unretryable  │
                                                        wiki.edits.retry         │
                                                                │                │
                                                                v                v
                                                             retrier ──────> wiki.edits.dlq
                                                          (delayed, bounded      │
                                                          re-attempts)           v
                                                                          manual sweeper
                                                                          (on-demand, can use
                                                                          a stronger model)
```

## Run it

```sh
cp .env.example .env       # put your ANTHROPIC_API_KEY in .env
docker compose up --build -d
```

For faster iteration on the service, run only the infra in Docker and the
service natively (the config defaults already point at the host-mapped ports):

```sh
docker compose up --build -d redpanda topic-init connect postgres
uv run --env-file .env python -m app.worker      # worker
uv run uvicorn app.api:app --port 8000           # API (separate terminal)
```

Watch it classify:

```sh
docker compose logs -f worker
```

Query results:

```sh
curl "http://localhost:8000/edits?label=vandalism"
curl "http://localhost:8000/edits?status=failed&limit=5"
```

Without a valid API key the worker deliberately **crash-loops** (visible in
`docker compose ps`) instead of draining the topic into fake data: a missing
or rejected key is a deterministic failure, so offsets stay uncommitted and
every message is redelivered once the key is fixed. See "Failure handling"
below for the full taxonomy.

Run the tests (no network, no Docker needed):

```sh
uv run pytest
```

Lint / format:

```sh
uv run ruff check . --fix
uv run ruff format
```

Mutation testing (~30s; `--directory service` because mutmut must run where
the `app` package lives — see the comment in `service/pyproject.toml`):

```sh
uv run --directory service mutmut run
uv run --directory service mutmut results   # list surviving mutants
```

## Layout

- `pyproject.toml` / `uv.lock` — [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/)
  root: one lockfile, one `.venv` for the whole repo; `service/` is a member
- `docker-compose.yml` — single-node Redpanda (dev-container mode), Connect,
  Postgres 16, worker + retrier + API (same image, three commands)
- `connect/pipeline.yaml` — SSE ingest, filtering, field projection. No LLM
  calls in Connect.
- `sql/schema.sql` — one `edits` table, `id TEXT` primary key, label +
  `status` (`classified` | `failed`) columns
- `service/app/classifier.py` — the LLM loop as named stages: prompt build →
  API call (bounded retry + backoff) → parse first `{...}` block → normalize
  label to the enum → retry once on parse failure → second-pass prompt when
  confidence is below threshold. Failures raise a typed taxonomy
  (`ModelConfigError` / `ModelUnavailableError` / `ClassificationParseError`)
  instead of fabricating an `unclear` row.
- `service/app/failures.py` — shared failure plumbing: retry/DLQ envelopes,
  backoff schedule, broker-acked publish, circuit breaker
- `service/app/worker.py` — single Kafka consumer; routes each failure class
  to crash / retry topic / DLQ; commits offsets only after the Postgres write
  and envelope publish (at-least-once + idempotent UPSERT)
- `service/app/retrier.py` — always-on consumer of `wiki.edits.retry`:
  delayed, bounded re-attempts; promotes exhausted messages to the DLQ
- `service/app/sweeper.py` — manual, on-demand DLQ drain (`--model` override)
- `service/app/api.py` — `GET /edits?label=&status=&limit=`
- `service/tests/` — classifier taxonomy + parse paths, envelope/breaker
  units, and `handle_message` / `handle_envelope` routing with fakes (no
  network, no broker, no Postgres)

## Failure handling

Every failure class has an explicit destination — nothing is silently
converted into data:

| Class | Detection | Action | Destination |
|---|---|---|---|
| Config/deterministic | missing key (SDK `TypeError`); 4xx except 408/409 | log CRITICAL, exit(1), **no commit** | crash loop; redelivered after the fix |
| Transient exhausted | 429 / 5xx / 408 / 409 / network, after 3 in-process attempts (1s/2s/4s) | `status='failed'` row + envelope, commit, breaker++ | `wiki.edits.retry` |
| Parse failure | unusable output after the format-reminder retry | `status='failed'` row + envelope, commit, breaker reset | `wiki.edits.dlq` |
| Malformed message | Kafka value isn't JSON | envelope with base64 raw, commit | `wiki.edits.dlq` |
| Success | — | `status='classified'` row, commit, breaker reset | Postgres |

The retrier consumes `wiki.edits.retry` continuously: each envelope carries a
`not_before` timestamp (30s → 60s → 120s cap, computed at publish time), and
after `1 + MAX_RETRY_PASSES` total attempts the message is promoted to
`wiki.edits.dlq` with reason `retries_exhausted`. The DLQ is terminal
(`retention.ms=-1`) and is drained only by the manual sweeper.

A **circuit breaker** (25 consecutive transient-exhausted outcomes) crashes
the worker/retrier; Docker's restart backoff is the automatic half-open
probe, so a recovered API heals the pipeline with no manual intervention.
A parse failure or success resets the breaker (both prove the API is up).

Consistency notes:

- The row key is Wikimedia's `rc_id` — unique per change event, so a retried
  old event can never clobber a newer edit to the same article (that is a
  different `rc_id` and a different row). Same-id writes only arise from
  redelivery of the *same* event.
- Envelopes are published with a synchronous broker ack **before** the source
  offset is committed. A crash in between produces a duplicate envelope, which
  is harmless: the UPSERT is idempotent and a failed-row write is guarded by
  `WHERE edits.status IS DISTINCT FROM 'classified'`, so a stale failure can
  never downgrade a row that a later attempt already classified.

### Operator runbook

- **Worker/retrier crash-looping** (`docker compose ps` shows restarts):
  check `docker compose logs worker` and `docker compose logs retrier` for
  the CRITICAL line — a bad key crash-loops both. Either the model config is
  bad (fix `.env`, then `docker compose up -d worker retrier` so both pick up
  the new env) or the circuit breaker tripped during an outage — the restart
  loop probes automatically and consumption resumes when the API recovers.
  Offsets are never committed on these paths, so no data is lost either way.
- **Inspect failures**: `curl "localhost:8000/edits?status=failed"` for rows;
  `docker compose exec redpanda rpk topic consume wiki.edits.retry` (or
  `wiki.edits.dlq`) for envelopes — each carries the failure reason,
  attempts, timestamps, and source-offset provenance, plus the original edit
  (or, for malformed payloads, the raw bytes base64-encoded).
- **Drain the DLQ** (after an incident, optionally with a stronger model):

  ```sh
  docker compose run --rm worker python -m app.sweeper --model claude-sonnet-4-5
  ```

  Malformed payloads are logged and skipped; still-failing envelopes are
  requeued to the DLQ tail for the next sweep; the run exits with a
  reclassified/requeued/skipped summary.
- **Schema changes** (e.g. picking up the `status` column on an existing dev
  stack): the Postgres volume is anonymous and only initialized once — run
  `docker compose down -v && docker compose up --build -d`.

## Filtering (before the model, on purpose)

The firehose is ~50 events/sec. Connect keeps only `enwiki`, namespace 0
(articles), `type=edit`, `bot=false` — roughly a few events/sec — so the LLM
spend tracks human article edits, not bot churn.

## Surprises hit while building

- The SSE stream interleaves non-JSON heartbeat/comment lines with `data:`
  frames. Bloblang does not fail closed inside `if`, so `parse_json()` needs
  `.catch(deleted())` or a raw heartbeat string sails through and breaks the
  topic schema.
- Wikimedia 403s clients with no `User-Agent`; the pipeline sets one.
- Wikipedia's `rc_id` is a large number; Bloblang's `.string()` on a parsed
  JSON number renders float64 scientific notation (`"2.04e+09"`). Cast
  `.int64()` first.
- Redpanda Connect's `redpanda` output compresses with **snappy by default**,
  and `kafka-python` cannot decode snappy without an extra C library. The
  output sets `compression: none` (the filtered stream is low-volume).
- Wikipedia timestamps are epoch **seconds**; Postgres `TIMESTAMPTZ` rejects
  raw epochs, so the pipeline formats to ISO with `ts_format`.

## Production-failure notes

- **Wikimedia disconnects long-lived SSE clients** (~15 min). Connect's
  `http_client` input reconnects, but there is no `Last-Event-ID` resume here,
  so a reconnect can drop or duplicate a few events. The UPSERT absorbs
  duplicates; drops are acceptable for this use case.
- **Anthropic rate limits / outages**: the worker retries transient errors
  (429, 5xx, 408/409, network) 3× with backoff, then marks the row
  `status='failed'` and parks the message on `wiki.edits.retry` for delayed,
  bounded re-attempts — provenance is never overloaded onto the `unclear`
  label, and poison messages can't wedge the consumer. A sustained outage
  trips the circuit breaker (see "Failure handling").
- **Postgres restarts**: the worker reconnects and retries the write once;
  offsets aren't committed until the write succeeds, so nothing is lost.
- **Worker restarts / redelivery**: at-least-once delivery + UPSERT on `id`
  keeps the table consistent (last write wins, except that a redelivered
  stale failure can never overwrite an already-classified row).
- **Cost control**: filtering happens before the topic, `max_tokens=256`, and
  the second-pass prompt fires only below the confidence threshold. If enwiki
  spikes, consumer lag grows rather than spend exploding per-event.

## Tradeoffs

TODO — to be written by the author.
