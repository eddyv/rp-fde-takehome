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

### Surprises Encountered During the Build

The build proceeded smoothly overall. Feeding `HINTS.md` to the LLM sped up development and avoided common pitfalls. The main gotcha involved discrepancies between hosted and local model providers: Anthropic supports [Structured Outputs](https://platform.claude.com/docs/en/build-with-claude/structured-outputs), requiring [Ollama v0.14+](https://github.com/ollama/ollama/releases?page=7#release-v0.14.0) for local parity. Additionally, since Postgres 16 lacks native UUIDv7 support, cursor-based pagination required a composite `(processed_at, id)` key instead of a single time-ordered column.

Other issues worth noting:
- Wikipedia's `rc_id` is large enough that Bloblang's `.string()` renders it in scientific notation (`"2.04e+09"`); casting to `.int64()` first avoids this.
- Redpanda Connect's `redpanda` output defaults to snappy compression, which `kafka-python` can't decode without an extra C library — resolved by setting `compression: none` given the low-volume filtered stream.

### Where would this break in production?

- **Missing parallelization**: Filtered Wikipedia recentchanges traffic is low enough that a single partition and hosted LLM perform fine here, but a real deployment would need parallelized processing & tuning partition count and topic config (`retention.ms`, etc.) accordingly.
- **Missing / Invalid API key**: The worker deliberately crash-loops rather than draining the topic into fake data. This is a deterministic failure, so offsets stay uncommitted and every message is redelivered once the key is fixed. Full taxonomy and the operator runbook: [`DESIGN.md`](DESIGN.md).
- **Wikimedia disconnects long-lived SSE clients** (~15 min). Connect's `http_client` input reconnects but doesn't support `Last-Event-ID` resume, so reconnects can drop or duplicate events. The UPSERT absorbs duplicates; drops are acceptable for this use case.
- **Anthropic rate limits / outages**: Transient errors (429, 5xx, 408/409, network) are retried 3× with backoff, then the row is marked `status='failed'` and parked on `wiki.edits.retry` for delayed, bounded re-attempts — provenance is never overloaded onto the `unclear` label, and poison messages can't wedge the consumer. A sustained outage trips a circuit breaker (25 consecutive failures) that crash-loops the worker, with Docker's restart backoff acting as the half-open probe.
- **Circuit breakers**: The current breaker is a hand-rolled implementation relying on Docker for recovery. Production would need a proper library like [pybreaker](https://pypi.org/project/pybreaker/) with real half-open/self-healing support.
- **Cost control**: Filtering happens before the topic (~50 events/sec down to a few/sec via enwiki, namespace 0, `type=edit`, `bot=false`), `max_tokens=256` caps responses, and the second-pass prompt only fires below the confidence threshold. If enwiki spikes, consumer lag grows instead of spend scaling per-event.
- **LLM prompt injection**: No explicit protection exists beyond a rudimentary guard wrapping text in `<<>>` — no dangerous-pattern detection or human-in-the-loop controls. A production system would apply techniques from the [OWASP LLM Prompt Injection Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/LLM_Prompt_Injection_Prevention_Cheat_Sheet.html#primary-defenses). Acceptable for this exercise's scope, but worth flagging.
- **Observability / Metrics**: The stack lacks proper observability (Prometheus + Grafana would suffice) to track application health (consumer lag [regular & dead-letter-topics], transient errors, crash loops, etc. are all invisible). The only insight available is a `/stats` endpoint showing event counts and their labels/status.