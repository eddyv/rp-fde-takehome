# Wikipedia Edit Classifier

Wikipedia recent-changes SSE firehose → Redpanda Connect (filter/project) →
Redpanda topic `wiki.edits.raw` → Python reasoning service (Claude LLM loop) →
Postgres → thin JSON API.

```
Wikimedia SSE ──> Redpanda Connect ──> wiki.edits.raw ──> worker (Claude) ──> Postgres ──> GET /edits
                  (plumbing only:                          (classify each
                  strip SSE frames,                        edit: vandalism /
                  filter bots + non-                       substantive /
                  articles, project                        trivia / unclear)
                  small schema)
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
curl "http://localhost:8000/edits?limit=5"
```

Without an API key the stack still runs end-to-end: every model call fails
fast, the service falls back, and rows land with `label=unclear` and low
confidence. The UPSERT means a later reprocess (same consumer group reset, or
redelivery) fixes them once a key is present.

Run the tests (no network, no Docker needed):

```sh
uv run pytest
```

Lint / format:

```sh
uv run ruff check . --fix
uv run ruff format
```

## Layout

- `pyproject.toml` / `uv.lock` — [uv workspace](https://docs.astral.sh/uv/concepts/projects/workspaces/)
  root: one lockfile, one `.venv` for the whole repo; `service/` is a member
- `docker-compose.yml` — single-node Redpanda (dev-container mode), Connect,
  Postgres 16, worker + API (same image, two commands)
- `connect/pipeline.yaml` — SSE ingest, filtering, field projection. No LLM
  calls in Connect.
- `sql/schema.sql` — one `edits` table, `id TEXT` primary key, label column
- `service/app/classifier.py` — the LLM loop as named stages: prompt build →
  API call (bounded retry + backoff) → parse first `{...}` block → normalize
  label to the enum → retry once on parse failure → fallback to `unclear` →
  second-pass prompt when confidence is below threshold
- `service/app/worker.py` — single Kafka consumer; commits offsets only after
  the Postgres write (at-least-once + idempotent UPSERT)
- `service/app/api.py` — `GET /edits?label=&limit=`
- `service/tests/test_classifier.py` — malformed-LLM-output path: asserts one
  retry then fallback, and recovery when the retry returns dirty-but-parseable
  JSON

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
  (429, 5xx, network) 3× with backoff, then writes the row as `unclear`
  instead of crashing or blocking the partition. Poison messages can't wedge
  the consumer.
- **Postgres restarts**: the worker reconnects and retries the write once;
  offsets aren't committed until the write succeeds, so nothing is lost.
- **Worker restarts / redelivery**: at-least-once delivery + UPSERT on `id`
  keeps the table consistent (last write wins).
- **Cost control**: filtering happens before the topic, `max_tokens=256`, and
  the second-pass prompt fires only below the confidence threshold. If enwiki
  spikes, consumer lag grows rather than spend exploding per-event.

## Tradeoffs

TODO — to be written by the author.
