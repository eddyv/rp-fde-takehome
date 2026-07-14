# Plan 009: Extract the shared classifier-failure routing used by worker and retrier

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 2ab6013..HEAD -- service/app service/tests`
> Plans 006-008 intentionally land first (008 touches these same files).
> Re-read `worker.py` and `retrier.py` in full before starting; the excerpts
> below are from `2ab6013` and the *structure* must still match even if
> factory calls changed. On structural mismatch, STOP.

## Status

- **Priority**: P3 — the advisor recommended deferring this; the maintainer
  chose to proceed. Execute with the full verification battery below.
- **Effort**: M
- **Risk**: MED — this code carries the at-least-once commit-ordering
  invariant (DB write → broker-acked publish → commit → breaker). A careless
  extraction that reorders side effects loses or duplicates data.
- **Depends on**: plans/008-consolidate-infra-factories.md
- **Category**: tech-debt
- **Planned at**: commit `2ab6013`, 2026-07-14

## Why this matters

The three-branch failure taxonomy (`ModelConfigError` → crash;
`ModelUnavailableError` → failed row + retry/DLQ envelope + commit +
breaker++; `ClassificationParseError` → failed row + DLQ envelope + commit +
breaker reset) is implemented twice, near-identically: `worker.py:114-170`
and `retrier.py:151-224`. Any failure-contract change (new reason, breaker
policy, ordering fix) needs lockstep edits in both, re-pinned in two test
files. The **sweeper is explicitly out of scope**: its variant
(`sweeper.py:144-172`) deliberately differs — no failed-row rewrite, no
breaker, requeue-to-DLQ-tail, per-message explicit-offset commit — and
forcing it into a shared shape would add parameters that obscure more than
they share.

## Current state

The two blocks differ ONLY in these ways (verify this claim by diffing the
excerpts yourself before extracting — it is the plan's core premise):

| Aspect | worker (`worker.py:114-170`) | retrier (`retrier.py:151-224`) |
|---|---|---|
| attempts | fixed `1` | `attempts += 1` from the envelope |
| first_failed_at | omitted (envelope stamps now) | carried from the incoming envelope |
| transient destination | always `settings.kafka_retry_topic`, `not_before=next_not_before(1)` | retry topic with `next_not_before(attempts)`, OR DLQ with reason `retries_exhausted` when `attempts >= 1 + settings.max_retry_passes` |
| parse-failure envelope | no `attempts`/`first_failed_at` kwargs (defaults) | passes both through |
| source | `"worker"` | `"retrier"` |
| log wording | minor differences | minor differences |

Both share, byte-for-byte in structure:

```python
    except ModelConfigError as error:
        logger.critical("deterministic model failure (bad key/config?), crashing: %s", error)
        raise SystemExit(1) from error
    except ModelUnavailableError as error:
        error_text = str(error)
        conn = db.write_with_reconnect(conn, lambda c: db.upsert_failed_edit(
            c, edit, failures.REASON_TRANSIENT_EXHAUSTED, error_text))
        # ... build envelope ... failures.publish(producer, <topic>, envelope)
        consumer.commit()
        if breaker.record_failure():
            logger.critical("circuit breaker tripped after %d ...", breaker.threshold)
            raise SystemExit(1)
        return conn
    except ClassificationParseError as error:
        error_text = str(error)
        conn = db.write_with_reconnect(conn, lambda c: db.upsert_failed_edit(
            c, edit, failures.REASON_PARSE_FAILED, error_text))
        # ... build envelope ... failures.publish(producer, settings.kafka_dlq_topic, envelope)
        consumer.commit()
        breaker.record_success()  # the API is reachable; only the output was bad
        return conn
```

**The characterization net already exists and is strict.** These tests pin
every envelope field, SQL param, ordering log entry, exit code, and breaker
count — they are the safety harness and MUST NOT be weakened:

- `service/tests/test_worker.py` — e.g.
  `test_transient_exhaustion_failed_row_then_retry_publish_then_commit`
  asserts `log == [("db",), ("publish", settings.kafka_retry_topic), ("commit",)]`,
  every envelope field, the `not_before` delay window, and
  `breaker.consecutive_failures == 1`;
  `test_config_error_crashes_without_commit_or_publish` asserts exit code 1
  with zero commits/publishes/writes.
- `service/tests/test_retrier.py` — the retrier-side equivalents including
  the `attempts >= 1 + max_retry_passes` DLQ-promotion boundary.
- The shared ordering log lives in `service/tests/fakes.py` (`FakeConn`,
  `FakeConsumer`, `FakeProducer` append `("db",)`, `("commit",)`,
  `("publish", topic)` to one list).

AGENTS.md binds: mutmut on touched modules; tests pin data; the recurring
mutation-caught gaps are exactly this plan's risk surface ("envelope fields…
`SystemExit` codes… retry/backoff arithmetic and boundary conditions").

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Tests (run after every step) | `uv run pytest` | all pass, zero assertion edits |
| Focused | `uv run pytest service/tests/test_worker.py service/tests/test_retrier.py -v` | all pass |
| Integration (needs Docker) | `uv run pytest -m "integration and not llm"` | all pass |
| Mutation gate (required, not optional) | `uv run --directory service mutmut run && uv run --directory service mutmut results` | no new non-acceptable survivors in `app.worker`, `app.retrier`, or the new module |
| Lint | `uv run ruff check .` | no new errors |

## Scope

**In scope**:
- `service/app/routing.py` (create) — or extend `failures.py` if the helper
  is small enough to live beside `make_envelope`/`publish`; executor's call,
  but ONE location.
- `service/app/worker.py` (`_handle_edit`'s except blocks only)
- `service/app/retrier.py` (`_handle_edit`'s except blocks only)
- `service/tests/test_routing.py` (create, only if the helper has logic not
  already covered through the worker/retrier tests — e.g. the destination
  decision as a pure function)

**Out of scope** (do NOT touch):
- `service/app/sweeper.py` — deliberately divergent; see Why this matters.
- `service/tests/test_worker.py` / `test_retrier.py` **assertions** — import
  lines may change if symbols move; every `assert` stays byte-identical.
- The happy path, redelivery pre-check, malformed-message handling,
  `wait_until`, envelope schema, `failures.py`'s existing functions.
- Any reordering of side effects, however equivalent it looks.

## Git workflow

- Commit directly on `main` (repo convention). Conventional style, e.g.
  `refactor(service): share worker/retrier classifier-failure routing`.
- Commit only after the FULL battery (unit + mutmut + integration if
  available) is green.

## Steps

### Step 1: Extract the destination decision as a pure function

In the chosen module, a pure, trivially-testable core:

```python
def transient_destination(attempts: int) -> tuple[str, str, str | None]:
    """(topic, reason, not_before_iso|None) for a transient-exhausted edit."""
    if attempts >= 1 + settings.max_retry_passes:
        return settings.kafka_dlq_topic, REASON_RETRIES_EXHAUSTED, None
    return settings.kafka_retry_topic, REASON_TRANSIENT_EXHAUSTED, next_not_before(attempts)
```

The worker's fixed behavior is the `attempts=1` case of the same rule
(1 < 1 + max_retry_passes for any max_retry_passes ≥ 1 → retry topic,
`next_not_before(1)`) — confirm the equivalence against the excerpts, then
rely on the existing worker tests to prove it.

**Verify**: `uv run pytest` — nothing uses it yet, all green.

### Step 2: Extract the shared failure handler

One function, parameterized ONLY by what the table in Current state shows
differs:

```python
def handle_classifier_failure(error, *, conn, consumer, producer, breaker,
                              message, edit, source, attempts,
                              first_failed_at=None):
    """Route ModelUnavailableError/ClassificationParseError; returns conn.
    Ordering invariant: DB write -> broker-acked publish -> commit -> breaker."""
```

- Keep `ModelConfigError` handling INLINE in each caller (it's three lines,
  and `raise SystemExit(1) from error` inside a helper changes the traceback
  shape reviewers rely on — cheap to keep local, zero drift risk).
- Body: the shared structure from Current state, with the transient branch
  calling `transient_destination(attempts)` and the parse branch passing
  `attempts`/`first_failed_at` through (the worker's call passes
  `attempts=1, first_failed_at=None`, which makes today's default-omitting
  envelope calls explicit — envelope output must be byte-identical; the
  `make_envelope` signature defaults are `attempts=1, first_failed_at=None`,
  so passing them explicitly is a no-op. Confirm against
  `failures.py:45-56`.)
- Log lines move with the code; keep per-source wording via the `source`
  param or an f-string — log text is an acceptable-survivor class for
  mutmut, don't over-engineer it.

**Verify**: `uv run pytest` — still green (still unused).

### Step 3: Switch the worker, then the retrier, one at a time

Replace the two except blocks in `worker.py::_handle_edit` with calls to the
shared handler; run the full unit suite; only then do the same in
`retrier.py::_handle_edit`.

**Verify after EACH file**: `uv run pytest` → all pass with zero assertion
edits. If any worker/retrier test fails, the extraction changed behavior —
fix the extraction, never the test.

### Step 4: Full battery

1. `uv run pytest` → all pass.
2. `rm -rf service/mutants` then
   `uv run --directory service mutmut run` →
   `uv run --directory service mutmut results`: no new non-acceptable
   survivors in `app.worker`, `app.retrier`, or the new module. The
   boundary `attempts >= 1 + settings.max_retry_passes` and the breaker
   calls are the mutants to watch.
3. If Docker is available: `uv run pytest -m "integration and not llm"` →
   all pass (`test_failure_paths.py` exercises the real broker-acked
   ordering).

## Test plan

The existing worker/retrier suites ARE the test plan (characterization by
construction). Add `test_routing.py` only for `transient_destination`'s
boundary (attempts at/below/above `1 + max_retry_passes`) if mutmut shows
the boundary insufficiently pinned through the callers — prefer killing
mutants through the existing caller-level tests first.

## Done criteria

- [ ] `uv run pytest` exits 0; `git diff service/tests/test_worker.py service/tests/test_retrier.py` shows no assertion changes (imports only, ideally empty)
- [ ] The shared handler exists in exactly one module; worker/retrier except-blocks for the two shared branches are single calls
- [ ] `grep -c 'REASON_TRANSIENT_EXHAUSTED' service/app/worker.py service/app/retrier.py` → 0 per file (moved to the shared module)
- [ ] mutmut: no new non-acceptable survivors (attach the `results` summary to the status row)
- [ ] Integration non-llm suite passes, or Docker-unavailability is noted in the status row
- [ ] `service/app/sweeper.py` untouched (`git diff --stat` confirms)
- [ ] `plans/README.md` status row updated

## STOP conditions

- Any existing test assertion would need to change to go green — report the
  exact assertion and why; do not weaken it.
- The differences table in Current state turns out to be incomplete (you
  find a third behavioral difference between the two blocks while diffing) —
  report it; the parameterization was designed around that table.
- mutmut surfaces a survivor you can only kill by testing the helper's
  internals in a way that duplicates caller-level pins — report rather than
  padding tests.
- You are tempted to fold the sweeper in "while you're here."

## Maintenance notes

- This centralizes the commit-ordering invariant: future reviewers should
  treat any edit to the shared handler as a data-loss-risk change and demand
  the ordering-log assertions in review.
- The advisor's original recommendation was to defer this until after the
  take-home walkthrough (explicit inline code is easier to defend line by
  line than a parameterized helper). If the walkthrough is imminent,
  consider executing this plan afterwards — the plan stays valid.
- If a future failure class is added, it goes in the shared handler once;
  the sweeper decides separately whether it applies.
