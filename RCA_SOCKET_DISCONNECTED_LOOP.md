# RCA: the `socket disconnected` ERROR loop (kafka-python × Redpanda)

**TL;DR** — When the stack boots fresh, the worker's and retrier's Kafka
producers send their first-ever `InitProducerId` in the same millisecond. One
triggers Redpanda's lazy `id_allocator` initialization and waits; the broker
fast-fails the concurrent other with a retriable `COORDINATOR_NOT_AVAILABLE`.
kafka-python 3.0.7 mishandles that error for idempotent-only producers: it
sends a `FindCoordinator` request with a **null** key, which is
protocol-invalid. The broker closes the connection without responding,
kafka-python interprets the close as a retriable disconnect and resends the
same request forever — a ~10 Hz `socket disconnected` ERROR loop that never
recovers in-process. Data flow is unaffected: the wedged process crashes on
its first publish (60 s timeout, uncommitted), the container restart heals it,
and at-least-once redelivery completes the retry → DLQ arc.

Root cause: **kafka-python bug** (unfixed as of 3.0.9). Redpanda behaves
identically to Apache Kafka here (verified). One-line client fix, verified
end-to-end; one-line service mitigation available today.

---

## Symptom

- One consumer process (the retrier, in the observed run) spams, at ERROR
  level, ~10 times per second, indefinitely:

  ```
  ERROR <KafkaTCPTransport [...]>: socket disconnected
  ERROR <KafkaConnection node_id=0 ...>: Connection lost: KafkaConnectionError: socket disconnected
  ERROR Metadata refresh: failed KafkaConnectionError: socket disconnected
  ```

- The other process (the worker), connected to the same broker, is silent.
- Functionality is unaffected: a failed edit still walks
  `wiki.edits.retry` → DLQ (`retries_exhausted`) after 1 worker + 3 retrier
  attempts.

## Affected versions

| Component | Version | Role |
|---|---|---|
| kafka-python | 3.0.7 (pinned) — **latest 3.0.9 also affected** | root cause |
| Redpanda | v25.3.15 | trigger + amplifier (behavior matches Apache Kafka) |
| service | `failures.make_producer()` — idempotence on by default in kafka-python 3.x | exposure |

## Causal chain

Five links, each observed directly (evidence in italics).

**1. Trigger — concurrent first-ever `InitProducerId` on a fresh cluster.**
Redpanda creates its `kafka_internal/id_allocator` topic lazily when the
first `InitProducerId` arrives; that request waits ~330 ms and succeeds.
Requests arriving concurrently (sub-millisecond) fast-fail in ~5 ms with
retriable `COORDINATOR_NOT_AVAILABLE` (error_code=16). Requests even 6 ms
later wait politely and succeed.
*Lazy creation observed directly: on a fresh broker,
`rpk cluster partitions list --all` shows no partitions and the broker log
never mentions id_allocator — until the instant the first `InitProducerId`
arrives, when the broker logs*

```
WARN  cluster - id_allocator_frontend.cc:269 - can't find {kafka_internal/id_allocator} in the metadata cache
INFO  cluster - topics_frontend.cc:376 - Create topics [{kafka_internal/id_allocator} ...]
INFO  raft - [{kafka_internal/id_allocator/0}] consensus.cc:1485 - Starting ... term 0 initial_state true
```

*and the client's success lands 364 ms after the "can't find" line — the
~330 ms first-call latency is the create-topic + raft-election window.
The concurrency requirement was measured with a raw-socket blast of 40
simultaneous frames: exactly 1 success + 39 × error 16, while arrivals 6+ ms
apart all succeed.*

*The fast-fail branch is visible in the broker log. In a 6-producer
collision, all six requests log the `can't find` WARN and all six attempt
the topic create; 2 ms later the five losers of the create race each log*

```
WARN cluster - id_allocator_frontend.cc:246 - can not create kafka_internal/id_allocator topic - error: The topic has already exists
```

*— exactly one line per fast-failed producer (5 = the 5 wedged clients).
The winner's create succeeds and its request waits for the raft election;
the losers' create returns "already exists" and the frontend answers
`COORDINATOR_NOT_AVAILABLE` immediately instead of waiting. That asymmetry
is also why arrivals 6+ ms later are safe: by then the topic is in the
metadata cache, so they never enter the create-race branch. This is why the
bug requires concurrent producer startup, and why it looks random: docker
compose starts worker + retrier at the same instant, and whichever
producer's request lands concurrent-but-second loses.*

**2. Root cause — kafka-python's null-key coordinator lookup.**
On `NOT_COORDINATOR` / `COORDINATOR_NOT_AVAILABLE`,
`InitProducerIdHandler.handle_response`
(`kafka/producer/transaction_manager.py:953`) unconditionally runs:

```python
self.transaction_manager._lookup_coordinator(CoordinatorType.TRANSACTION, self.transactional_id)
```

For an idempotent-only producer `transactional_id` is `None` — there is no
transaction coordinator to find. The lookup is enqueued anyway, as
`FindCoordinatorRequest(key=None, key_type=1, coordinator_keys=[None])`.

**3. The request is protocol-invalid.** In the Kafka message spec
(`FindCoordinatorRequest.json`, shipped verbatim inside kafka-python),
`CoordinatorKeys` is `[]string` with no `nullableVersions` — and array
elements can never be null in the spec. On the wire the null becomes a
0-varint compact string inside a 42-byte frame.

**4. The broker closes the connection — standard behavior, not a Redpanda
quirk.** Redpanda fails to decode the null and slams the socket with no
response:

```
kafka - connection_context.cc:1116 - Disconnected ... (short read),
std::out_of_range (Asked to read a 0 byte flex string)
```

*Apache Kafka 3.9 was tested with the identical frame and does the same:
`InvalidRequestException: Error getting request for apiKey: FIND_COORDINATOR`
→ `Closing socket ... because of error`, no response.*

**5. The infinite loop.** kafka-python sees only a `KafkaConnectionError`,
treats it as retriable, re-enqueues the *same* doomed `FindCoordinator`, and
retries every ~100 ms forever. `FindCoordinator` has queue priority 0, so the
`InitProducerId` retry is blocked behind it permanently: the producer id
stays `-1`, the producer's metadata refreshes die on the same connection
(`Metadata refresh: failed`), and the ERROR spam continues until the process
dies. *Measured: 239 client disconnects vs 243 broker parse-slams over one
6 s window — one per loop iteration, ~8/s per wedged producer. The
disconnects are caused by the `FindCoordinator` frames, not by
`InitProducerId`: in the `--with-fix` control the identical collision still
produces the error-16 responses but zero disconnects (a failed
`InitProducerId` is a well-formed response on a healthy connection), and
conversely `wedge-poison-direct.py` triggers the disconnect with a single
bare `FindCoordinator` frame and no `InitProducerId` anywhere.*

## Why it looked like "retrier vs worker"

Nothing in the retrier is different. The two processes' producers raced; the
worker's request happened to be the one that triggered allocator creation
(its `InitProducerId` succeeded after 327 ms — exactly the creation latency),
and the retrier's landed concurrently and fast-failed 4 ms in. On another
boot the roles could swap, or neither/both could lose.

## Why it was still "functionally fine"

The consumer is a separate Kafka client and keeps consuming normally. The
wedge only matters on the first publish: `send().get(timeout=30)` can never
complete (no producer id, no metadata), the service raises
`KafkaTimeoutError`, and the process crashes **uncommitted** — which is the
service's designed failure mode. `restart: unless-stopped` respawns it, the
fresh producer initializes against the now-warm broker (the *lazy-creation*
window is one-shot: allocator state persists across broker restarts — ids
are handed out in blocks of `id_allocator_batch_size:1000`, and a
post-restart producer observably received id 1001; see "Exposure beyond
first boot" for windows other than creation), the in-flight envelope is
redelivered, and the
retry → DLQ arc completes. Cost: log spam plus one crash/restart cycle and
~60 s of added latency on the first post-wedge publish.

## Why consumers are unaffected

Consumers also call `FindCoordinator`, but the null-key frame is unreachable
from their side. kafka-python has exactly three `FindCoordinator` call sites:

| Call site | Key | Can the key be null? |
|---|---|---|
| `producer/transaction_manager.py:1075` (the bug) | `transactional_id` | **Yes** — it is `None` by definition for idempotent-only producers |
| `coordinator/base.py:826` (consumer group lookup) | `group_id` | No — a consumer without a group id runs no group coordination at all, so the request is never sent |
| `admin/client.py` (AdminClient) | caller-supplied group ids | No (and unused by this service) |

Both frames appear side-by-side in the captured evidence: consumers sent
`FindCoordinatorRequest(key_type=0, coordinator_keys=['reasoning-service-retrier'])`
— valid, answered instantly — while the wedged producer sent
`coordinator_keys=[None]`. The original incident is itself the strongest
proof: the wedged retrier's consumer joined its group, fetched envelopes, and
committed offsets normally the whole time the producer client in the same
process looped.

Consumers even hit the same class of retriable coordinator errors at fresh
boot (`COORDINATOR_NOT_AVAILABLE` while the group coordinator initializes)
and handle it correctly: they retry the *same valid request* until it
succeeds. The producer path is broken precisely because its error handler
does not retry the failed request — it fabricates a different, invalid one.

## Exposure beyond first boot

The loop is entered by any *fast* retriable coordinator-error response to
`InitProducerId`. On this single-node dev stack, only the fresh-cluster
first-init window was ever observed (plain broker restarts tested clean,
6/6 — boot-window connection failures do **not** trigger it, only an error
*response* does). In multi-node production clusters, coordinator errors
during `id_allocator` partition leadership movement could in principle open
the same window; the loop's shape and its restart-heals property would be
identical.

## Reproduction

All scripts live at the repo root; Docker required.

| Command | What it shows | Deterministic? |
|---|---|---|
| `./wedge-repro.sh` | Full chain, narrated stage-by-stage with timestamps, code locations, and both client- and broker-side evidence. Boots a throwaway fresh Redpanda; a TCP relay releases all 6 producers' `InitProducerId` frames simultaneously (the same collision compose creates naturally). 5 of 6 producers wedge, every run. | Yes |
| `./wedge-repro.sh --with-fix` | Identical collision with the proposed client fix patched in: 5 fast-fails, all 6 recover by plain retry in ~440 ms, zero poison requests. | Yes |
| `uv run python wedge-poison-direct.py <host:port>` | Broker half only: replays the exact 42-byte null-key frame against **any** running Redpanda → silent connection slam. | Yes |
| `docker compose down -v && ANTHROPIC_BASE_URL=http://127.0.0.1:1 docker compose up` | The natural, unassisted occurrence. | No (same-ms coin flip per fresh boot) |

The relay synchronizes only *when* frames arrive; producers, broker
responses, and the loop are unmodified real behavior.

## Fixes

### 1. Upstream kafka-python (the real fix) — file with the repro above

```diff
 # kafka/producer/transaction_manager.py, InitProducerIdHandler.handle_response
         elif issubclass(error_type, Errors.RetriableError):
             if error_type in (Errors.NotCoordinatorError, Errors.CoordinatorNotAvailableError):
-                self.transaction_manager._lookup_coordinator(CoordinatorType.TRANSACTION, self.transactional_id)
+                if self.transaction_manager.is_transactional():
+                    # Idempotent-only producers have no transaction coordinator;
+                    # a null-key FindCoordinator is protocol-invalid.
+                    self.transaction_manager._lookup_coordinator(CoordinatorType.TRANSACTION, self.transactional_id)
             self.reenqueue()
```

Verified end-to-end by `./wedge-repro.sh --with-fix` (applied as an
equivalent runtime guard). Secondary hardening worth mentioning upstream:
(a) the protocol encoder should refuse to serialize `None` into a
non-nullable field instead of emitting invalid bytes; (b) an identical
transactional request re-enqueued on every disconnect deserves a retry
bound — it converted one bad response into an infinite loop.

### 2. This service, today (recommended until upstream ships)

Disable idempotence in `service/app/failures.py` (`make_producer`):

```python
return KafkaProducer(
    bootstrap_servers=settings.kafka_broker_list,
    acks="all",
    retries=5,
    enable_idempotence=False,  # see RCA_SOCKET_DISCONNECTED_LOOP.md
    value_serializer=JsonSerializer(),
)
```

Safe because the pipeline is at-least-once by design: envelope publishes are
broker-acked before offsets commit, duplicates are explicitly tolerated
(idempotent UPSERT, guarded failed-row writes), so the idempotent-producer
feature adds nothing here. Removing it removes `InitProducerId` — the entire
chain becomes unreachable.

### 3. Alternative service mitigation (keeps idempotence)

Apply the stage-2 guard as a small startup shim (see the `--with-fix` block
in `wedge-repro.py`) — a monkeypatch to remove once upstream fixes it.
Workable, but option 2 is simpler and has no upstream coupling.

### Non-fix: Redpanda

Closing the connection on an unparseable request matches Apache Kafka;
no behavioral change warranted. A cosmetic upstream suggestion: include the
API key in the parse-failure log line (Kafka does), which would have made
this diagnosable from the broker log alone.

## Evidence inventory

- Narrated live run: `./wedge-repro.sh` output; full client DEBUG log in
  `wedge-repro.log`.
- The poison frame (captured via tcpdump from the originally wedged retrier,
  byte-identical to what `wedge-poison-direct.py` replays):
  `0000 0026` (len 38) · `000a 0004` (FindCoordinator v4) · corr id ·
  `"kafka-python-producer-1"` · `01` (key_type=TRANSACTION) · `02` (1-element
  array) · `00` (**null key**) · `00`.
- Spec: `kafka/protocol/schemas/resources/FindCoordinatorRequest.json`
  (inside the installed kafka-python package).
- Buggy code: `kafka/producer/transaction_manager.py:953`; loop mechanics:
  `on_complete` disconnect handling (~:856) + `Priority.FIND_COORDINATOR = 0`.
- Redpanda slam: broker log `connection_context.cc:1116`, one line per loop
  iteration, count matches client-side disconnect count.
- Lazy `id_allocator` creation: before any producer,
  `rpk cluster partitions list --all` is empty and the broker log has zero
  id_allocator mentions; at the first `InitProducerId` the broker logs the
  `id_allocator_frontend.cc:269` "can't find ... in the metadata cache" WARN,
  the `topics_frontend.cc:376` create, and the raft-group bootstrap for
  `{kafka_internal/id_allocator/0}`; the client's `ProducerId set to 1` lands
  364 ms after the WARN. Allocator persistence across restarts:
  `id_allocator_batch_size:1000` (boot config dump) and a post-restart
  producer receiving id 1001.
- Fast-fail branch (per-victim broker log signature): in a 6-producer
  collision, 6 × `id_allocator_frontend.cc:269` "can't find" WARNs and
  6 × topic creates at T, then 5 × `id_allocator_frontend.cc:246`
  "can not create ... The topic has already exists" WARNs at T+2 ms —
  one per fast-failed producer, matching the 5 wedged clients and the
  5 client-side error-16 responses 1:1.
- Disconnect causality controls: `--with-fix` (error-16 responses present,
  null lookup guarded → no disconnect loop) and `wedge-poison-direct.py`
  (bare null-key frame, no `InitProducerId` → immediate disconnect).
