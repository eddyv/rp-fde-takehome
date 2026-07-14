# Design Notes

Implementation depth that didn't need to be in the README: file layout, the
full failure taxonomy, the operator runbook, and the filtering rationale.
Run instructions, tradeoffs, surprises, and production-failure notes are in
[`README.md`](README.md).

## Notes

- Pagination is provided by fastapi-pagination (backed by sqlakeyset) over a
keyset on the composite `(processed_at, id)` (id breaks timestamp ties). A
time-ordered UUIDv7 primary key would collapse that to a single column, but
native `uuidv7()` needs Postgres 18 or an extension — we kept Postgres 16 as
is for now.

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
  API call (bounded retry + backoff, structured outputs pin the JSON schema
  and label enum) → parse + normalize (confidence clamped; refusal/truncation
  rejected) → second-pass prompt when confidence is below threshold. Failures
  raise a typed taxonomy (`ModelConfigError` / `ModelUnavailableError` /
  `ClassificationParseError`) instead of fabricating an `unclear` row.
- `service/app/failures.py` — shared failure plumbing: retry/DLQ envelopes,
  backoff schedule, broker-acked publish, circuit breaker
- `service/app/worker.py` — single Kafka consumer; routes each failure class
  to crash / retry topic / DLQ; commits offsets only after the Postgres write
  and envelope publish (at-least-once + idempotent UPSERT)
- `service/app/retrier.py` — always-on consumer of `wiki.edits.retry`:
  delayed, bounded re-attempts; promotes exhausted messages to the DLQ
- `service/app/sweeper.py` — manual, on-demand DLQ drain (`--model` override)
- `service/app/api.py` — `GET /edits?label=&status=&size=&cursor=`
  (fastapi-pagination `CursorPage`)
- `service/tests/` — classifier taxonomy + parse paths, envelope/breaker
  units, and `handle_message` / `handle_envelope` routing with fakes (no
  network, no broker, no Postgres)

## Failure handling

Every failure class has an explicit destination — nothing is silently
converted into data:

| Class                 | Detection                                                                                         | Action                                                  | Destination                            |
| ---------------------- | --------------------------------------------------------------------------------------------------| ----------------------------------------------------------| -----------------------------------------|
| Config/deterministic  | missing key (SDK `TypeError`); 4xx except 408/409                                                | log CRITICAL, exit(1), **no commit**                    | crash loop; redelivered after the fix  |
| Transient exhausted   | 429 / 5xx / 408 / 409 / network, after 3 in-process attempts (1s/2s backoff between attempts, none after the last) | `status='failed'` row + envelope, commit, breaker++     | `wiki.edits.retry`                     |
| Parse failure         | refusal, truncation, or non-conforming output (single call; structured outputs, no format retry) | `status='failed'` row + envelope, commit, breaker reset | `wiki.edits.dlq`                       |
| Malformed message     | Kafka value isn't JSON                                                                            | envelope with base64 raw, commit                        | `wiki.edits.dlq`                       |
| Success               | —                                                                                                  | `status='classified'` row, commit, breaker reset        | Postgres                               |

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
- **Inspect redpanda **:
  ```sh
  # consume from a topic
  docker run --rm -it --network redpanda-fde-takehome_default docker.redpanda.com/redpandadata/redpanda:v25.3.15 topic consume wiki.edits.raw -X brokers=redpanda:9092
  # temporary container to inspect a consumer group
  docker run --rm -it --network redpanda-fde-takehome_default docker.redpanda.com/redpandadata/redpanda:v25.3.15 group describe reasoning-service -X brokers=redpanda:9092
  ```
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
