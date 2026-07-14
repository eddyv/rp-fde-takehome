# Plan 008: Consolidate the duplicated Kafka-consumer and Anthropic-client factories

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 2ab6013..HEAD -- service/app service/tests`
> Plans 006/007 edit `classifier.py` (not touched here); on any mismatch in
> the worker/retrier/sweeper excerpts below, STOP.

## Status

- **Priority**: P3
- **Effort**: M
- **Risk**: LOW-MED (touches process startup of all three binaries; no logic change)
- **Depends on**: none (land before plan 009, which edits the same files)
- **Category**: tech-debt
- **Planned at**: commit `2ab6013`, 2026-07-14

## Why this matters

Three infra constructions are copy-pasted and can silently diverge:

1. `make_consumer` — byte-identical retry-until-broker-up loops in
   `worker.py:50-66` and `retrier.py:51-67` (only topic + group differ).
2. `MAX_POLL_INTERVAL_MS = 600_000` — declared separately in `worker.py:47`
   and `retrier.py:47`; the retry-backoff cap in `config.py:35-38` documents
   an invariant against it ("must stay well under the retrier's
   max_poll_interval_ms").
3. `anthropic.Anthropic(api_key=…, base_url=…, max_retries=0, timeout=60.0)`
   — triplicated in `worker.py:189-194`, `retrier.py:243-248`,
   `sweeper.py:69-74`. These kwargs are the documented guardrail that keeps
   one hung call from evicting the consumer group — and they are test-pinned
   only for the sweeper (`test_sweeper.py:118-131`); a mutation flipping them
   in the worker or retrier survives today.

Bonus fix: unit/integration tests currently monkeypatch the **shared**
`anthropic` module (`sweeper.anthropic.Anthropic`), which the sweeper-drain
test's own docstring flags as a global-blast-radius hack. A module-local
factory gives tests a clean seam.

## Current state

`service/app/worker.py:50-66` (retrier.py:51-67 is identical except
`settings.kafka_retry_topic` / `settings.retrier_consumer_group`):

```python
def make_consumer(retries: int = 30, delay: float = 2.0) -> KafkaConsumer:
    for attempt in range(retries):
        try:
            return KafkaConsumer(
                settings.kafka_topic,
                bootstrap_servers=settings.kafka_brokers.split(","),
                group_id=settings.consumer_group,
                enable_auto_commit=False,
                auto_offset_reset="earliest",
                max_poll_interval_ms=MAX_POLL_INTERVAL_MS,
            )
        except KafkaError as error:  # broker not up yet at stack boot
            if attempt == retries - 1:
                raise
            logger.info("kafka not ready (%s), retrying...", type(error).__name__)
            time.sleep(delay)
    raise RuntimeError("unreachable")
```

Client construction, identical in all three `main()`s (comment included):

```python
    # SDK retries are disabled: this service owns retry/backoff (classifier.py).
    # The explicit request timeout (SDK default is 600s) keeps one hung call
    # from blowing past max_poll_interval_ms and evicting us from the group.
    client = anthropic.Anthropic(
        api_key=settings.anthropic_api_key.get_secret_value(),
        base_url=settings.anthropic_base_url,
        max_retries=0,
        timeout=60.0,
    )
```

**Seams that MUST keep working** (callers of the current names):

- `service/tests/integration/conftest.py:343` → `worker.make_consumer()` and
  `:360` → `retrier.make_consumer()` — zero-arg production factories used by
  the integration harness. Keep these module-level functions as thin
  delegates so the integration tests need no changes.
- `service/tests/test_sweeper.py:84-97` (`run_sweeper`) →
  `monkeypatch.setattr(sweeper.anthropic, "Anthropic", fake_anthropic)` and
  `monkeypatch.setattr(sweeper, "make_consumer", lambda: consumer)`.
- `service/tests/integration/test_sweeper_drain.py:120` → patches
  `sweeper.anthropic.Anthropic` (its docstring at lines 20-25 already asks
  for exactly the seam this plan introduces).
- The sweeper's own consumer (`sweeper.py:44-52`) differs deliberately
  (`consumer_timeout_ms=10_000`, no `max_poll_interval_ms`) — it does NOT
  join the shared consumer factory; only the client factory.

House conventions: AGENTS.md — descriptive names, tests pin data (kwargs,
exit codes), run mutmut on touched modules, extend `tests/fakes.py` rather
than inventing new doubles.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Tests | `uv run pytest` | all pass |
| Integration (needs Docker) | `uv run pytest -m "integration and not llm"` | all pass |
| Lint | `uv run ruff check .` | no new errors |
| Types (if plan 005 landed) | `uv run ty check service/app` | 0 diagnostics |
| Mutation gate | `uv run --directory service mutmut run && uv run --directory service mutmut results` | no non-acceptable survivors in the new module |

## Scope

**In scope**:
- `service/app/infra.py` (create — shared factories + `MAX_POLL_INTERVAL_MS`)
- `service/app/worker.py`, `service/app/retrier.py`, `service/app/sweeper.py`
  (delegate to the factories; no behavior change)
- `service/tests/test_infra.py` (create)
- `service/tests/test_sweeper.py`, `service/tests/integration/test_sweeper_drain.py`
  (re-point the monkeypatch target only)

**Out of scope**:
- `handle_message` / `handle_envelope` / the sweeper drain loop — plan 009's
  territory. This plan must not move a single line of message handling.
- `failures.make_producer` — already shared; leave it where it is.
- The sweeper's consumer configuration (deliberately different).
- Integration conftest — the thin delegates keep it working untouched.

## Git workflow

- Commit directly on `main` (repo convention). Conventional style, e.g.
  `refactor(service): share consumer/client factories across worker, retrier, sweeper`.

## Steps

### Step 1: Create `service/app/infra.py`

```python
"""Shared process-startup factories for worker, retrier, and sweeper."""

import logging
import time

from anthropic import Anthropic          # module-local name: tests patch infra.Anthropic
from kafka import KafkaConsumer
from kafka.errors import KafkaError

from app.config import settings

logger = logging.getLogger(__name__)

# One definition (was duplicated in worker.py/retrier.py); config.py's
# retry_backoff_max_seconds documents an invariant against this value.
MAX_POLL_INTERVAL_MS = 600_000


def make_consumer(topic: str, group_id: str, retries: int = 30, delay: float = 2.0) -> KafkaConsumer:
    ...  # the existing retry loop, verbatim, parameterized by topic/group


def make_classifier_client(timeout: float = 60.0) -> Anthropic:
    # SDK retries are disabled: this service owns retry/backoff (classifier.py).
    # The explicit request timeout (SDK default is 600s) keeps one hung call
    # from blowing past max_poll_interval_ms and evicting us from the group.
    return Anthropic(
        api_key=settings.anthropic_api_key.get_secret_value(),
        base_url=settings.anthropic_base_url,
        max_retries=0,
        timeout=timeout,
    )
```

Move the guardrail comment here (single source); the three `main()`s keep at
most a one-line pointer.

### Step 2: Delegate from worker and retrier

- `worker.py`: `make_consumer()` becomes
  `return infra.make_consumer(settings.kafka_topic, settings.consumer_group)`;
  delete the local retry loop, `MAX_POLL_INTERVAL_MS`, and now-unused imports
  (`time`, `KafkaError`, possibly `anthropic`); `main()` uses
  `infra.make_classifier_client()`.
- `retrier.py`: same, with `settings.kafka_retry_topic` /
  `settings.retrier_consumer_group`. NOTE: `retrier.py:47` also uses
  `MAX_POLL_INTERVAL_MS` only for the consumer kwargs and a docstring
  reference — update the module docstring's reference if it names the
  constant's location.

Keep both zero-arg `make_consumer()` wrappers so
`service/tests/integration/conftest.py:343,360` work unchanged.

**Verify**: `uv run pytest` → all pass (unit tests don't build real consumers).

### Step 3: Delegate from the sweeper + re-point test seams

- `sweeper.py`: `main()` uses `infra.make_classifier_client()` (the
  `--model` flag logic is unrelated and unchanged). Its own `make_consumer`
  stays.
- `service/tests/test_sweeper.py:91`:
  `monkeypatch.setattr(sweeper.anthropic, "Anthropic", fake_anthropic)` →
  `monkeypatch.setattr(infra, "Anthropic", fake_anthropic)` (import `infra`
  in the test module). All assertions stay identical — including
  `test_client_owns_no_retries_and_bounds_each_request`.
- `service/tests/integration/test_sweeper_drain.py:120`: same re-point; also
  update its lines-20-25 docstring, which currently apologizes for the
  global patch — the apology is now stale.

**Verify**: `uv run pytest` → all pass.

### Step 4: Add `service/tests/test_infra.py`

Pin the guardrail kwargs ONCE for all three binaries (this closes the
"worker/retrier client kwargs unasserted" gap):

1. `make_classifier_client()` with `infra.Anthropic` monkeypatched to a
   recorder: assert `api_key == settings.anthropic_api_key.get_secret_value()`,
   `base_url == settings.anthropic_base_url`, `max_retries == 0`,
   `timeout == 60.0` (mirror the assertion messages in
   `test_sweeper.py:129-131` — "classifier.py owns retry/backoff", "a hung
   call must not stall …").
2. `make_consumer` retry loop: monkeypatch `infra.KafkaConsumer` with a fake
   that raises `KafkaError` N times then succeeds; assert attempt count,
   sleep schedule (conftest already neutralizes real sleep;
   re-patch with a recorder as `test_classifier.py:72-74` does), and the
   final kwargs (`enable_auto_commit=False`, `auto_offset_reset="earliest"`,
   `max_poll_interval_ms=600_000`, topic + group threaded through).
3. Exhaustion: raises after `retries` failures (pin `retries=2` for speed).

**Verify**: `uv run pytest service/tests/test_infra.py -v` → all pass.

### Step 5: Sweep for leftovers + mutation gate

**Verify**:
- `grep -rn "anthropic.Anthropic(" service/app/` → no matches (only
  `infra.py`'s `Anthropic(`).
- `grep -rn "MAX_POLL_INTERVAL_MS" service/app/` → definition in `infra.py`
  only (plus imports/references).
- `uv run pytest` all pass; integration suite if Docker available.
- mutmut on `app.infra`: no non-acceptable survivors (the kwarg values and
  retry arithmetic must be killed by Step 4's tests).

## Test plan

Step 4 (new) + Step 3 (re-pointed seams). Patterns:
`test_sweeper.py:118-131` (kwargs pinning), `test_classifier.py:72-74`
(sleep recorder), `tests/fakes.py` for any new double (extend there if
needed, per AGENTS.md).

## Done criteria

- [ ] `service/app/infra.py` exists; worker/retrier/sweeper contain no direct `anthropic.Anthropic(` construction and no duplicated retry loop
- [ ] `worker.make_consumer()` / `retrier.make_consumer()` still exist zero-arg (integration conftest diff is empty)
- [ ] `test_infra.py` pins client kwargs and consumer kwargs/retry/exhaustion
- [ ] No test patches `*.anthropic.Anthropic` on the shared module anymore (`grep -rn 'anthropic, "Anthropic"' service/tests/` → no matches)
- [ ] `uv run pytest` exits 0; integration non-llm suite passes if Docker available (say which in the status row)
- [ ] mutmut: no non-acceptable survivors in `app.infra`
- [ ] `git diff --stat` touches only in-scope files
- [ ] `plans/README.md` status row updated

## STOP conditions

- Any existing test assertion (not patch target) needs changing — this is a
  pure factoring; assertion changes mean behavior moved.
- The integration conftest needs edits (the delegates were supposed to
  prevent that).
- You are tempted to move `handle_message`/`handle_envelope` logic — that is
  plan 009, out of scope here.

## Maintenance notes

- Plan 009 assumes this plan's file state; land 008 first.
- Future binaries (e.g. a new consumer) should build from `infra.py`;
  reviewers should reject fresh `KafkaConsumer(`/`Anthropic(` call sites in
  `app/` modules.
- If the client timeout ever needs to differ per binary, thread it through
  `make_classifier_client(timeout=…)` — do not fork the factory.
