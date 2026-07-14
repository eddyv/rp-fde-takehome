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

Implementation depth — file layout, the full failure taxonomy, the operator
runbook — lives in [`DESIGN.md`](DESIGN.md). This file sticks to running it,
the tradeoffs, and what broke.

## Run it

```sh
cp .env.example .env       # put your ANTHROPIC_API_KEY in .env
docker compose up --build -d
```

For faster iteration on the service, run only the infra in Docker and the
service natively (requires [`uv`](https://docs.astral.sh/uv/getting-started/installation/)
locally; the config defaults already point at the host-mapped ports; `uv run`
syncs the venv from the lockfile automatically, no separate `uv sync` needed):

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
curl "http://localhost:8000/edits?status=failed&size=5"
# responses are CursorPage envelopes (fastapi-pagination):
# {"items": [...], "total": null, "current_page": ..., "current_page_backwards": ...,
#  "previous_page": ..., "next_page": "eyJwIjoi..."}
# pass next_page back as ?cursor= (with the same filters) until it is null
curl "http://localhost:8000/edits?size=5&cursor=eyJwIjoi..."

curl "http://localhost:8000/stats"
# {"total": ..., "by_label": {...}, "by_status": {...}} — label/status counts
```

Browse `/edits` interactively instead of hand-copying cursors:

```sh
uv run --directory service edits-tui
# EDITS_API_URL=http://localhost:8000 (default) — override if the API is elsewhere
# n next page, p previous page, r reset, arrow keys scroll the table's columns
# (free-text columns are last) — enter on a row shows the full record
```

## Development

```sh
uv run pytest                              # unit tests, no network/Docker
uv run pytest -m "integration"             # + Redpanda/Postgres/Ollama via testcontainers
uv run ruff check . --fix && uv run ruff format # lint & format
uv run ty check service/app                # type check (scoped: test tier uses fakes on purpose)
uv run --directory service mutmut run      # mutation testing (~30s)
```

## Manual Testing (Humans)

Walkthrough for hand-driving every happy/failure path (network failure, bad
input, retry, sweeping) against a running stack, with `connect` intentionally
left out: [`MANUAL_TESTING.md`](MANUAL_TESTING.md).

## Tradeoffs

### One topic with a label column vs. topic-per-label routing

Chose one topic with a label column.

In general, event streams are easier to split than to piece back together. A raw events stream in Redpanda can easily be split into **topic-per-label** downstream using microservices or any stream processing layer of choice.

This also leans into the **medallion architecture** pattern: data lands raw in the lakehouse (bronze), gets cleaned/augmented into a new stream (silver), then aggregated into business-level views (gold).

I'd choose topic-per-label routing upfront only if we know events of different labels never need to be processed in order relative to each other. i.e., labels are mutually exclusive, split by label.

This exercise is also simple enough that it doesn't carry the consistency requirements of a more serious system like payment or order processing. Were this a **saga-based system**, I'd introduce a workflow orchestrator like **Temporal**, to make resume/replay/recovery of events straightforward.

### How you bound LLM cost/latency — filter in Connect first, batch in the service, or gate on confidence

Primarily done through filters in Connect first, excluding events deemed not interesting (e.g., bots).

LLMs carry real cost and latency overhead, so in production I'd scrutinize whether every event needs one. Traditional rule-based classification should be the first pass, only route to an LLM when rules are insufficient or return low confidence, i.e. falling back to the LLM only when our confidence threshold is below a value.

Currently, topics are configured to be single partitioned and the latency for processing each individual event can cause a fiar amount of consumer lag. Setting a proper partition count along with parallelized workers could decrease our overall processing speed (or routing to local LLMs / faster models).

Beyond the data itself, I'd weigh required freshness. **[Message Batch APIs](https://platform.claude.com/docs/en/build-with-claude/batch-processing)** cut cost at the expense of latency. Not suitable at this exercise's volume, but worth considering in production.

### Surprises hit while building

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

### Where would this break in production?

- **Missing Parallization**: Wiki recentchanges as a source (once filtered) is 
  relatively low throughput so a single partition + a hosted LLM can perform *fine*. 
  However, realistically you'd want to parallelize your proccesses so determining a good 
  partition count would help.
- **Missing/invalid API key**: the worker deliberately crash-loops instead of
  draining the topic into fake data — a deterministic failure, so offsets
  stay uncommitted and every message is redelivered once the key's fixed.
  Full taxonomy and the operator runbook: [`DESIGN.md`](DESIGN.md).
- **Wikimedia disconnects long-lived SSE clients** (~15 min). Connect's
  `http_client` input reconnects, but there is no `Last-Event-ID` resume here,
  so a reconnect can drop or duplicate a few events. The UPSERT absorbs
  duplicates; drops are acceptable for this use case.
- **Anthropic rate limits / outages**: the worker retries transient errors
  (429, 5xx, 408/409, network) 3× with backoff, then marks the row
  `status='failed'` and parks the message on `wiki.edits.retry` for delayed,
  bounded re-attempts — provenance is never overloaded onto the `unclear`
  label, and poison messages can't wedge the consumer. A sustained outage
  trips a circuit breaker (25 consecutive failures) that crash-loops the
  worker; Docker's restart backoff acts as the half-open probe.
- **Postgres restarts**: the worker reconnects and retries the write once;
  offsets aren't committed until the write succeeds, so nothing is lost.
- **Worker restarts / redelivery**: at-least-once delivery + UPSERT on `id`
  keeps the table consistent (last write wins, except that a redelivered
  stale failure can never overwrite an already-classified row).
- **Cost control**: filtering happens before the topic (~50 events/sec down
  to a few/sec — enwiki, namespace 0, `type=edit`, `bot=false`), `max_tokens=256`,
  and the second-pass prompt fires only below the confidence threshold. If
  enwiki spikes, consumer lag grows rather than spend exploding per-event.