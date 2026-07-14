# Plan 014: Prove the retrier survives a real multi-second `wait_until` sleep

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 585b9f6..HEAD -- service/app/retrier.py service/tests/integration/test_failure_paths.py service/tests/integration/conftest.py`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: tests
- **Planned at**: commit `585b9f6`, 2026-07-14

## Why this matters

`retrier.py`'s module docstring makes a specific, load-bearing claim:

> "consumer lag eats most of the delay for free ... kafka-python heartbeats
> from a background thread, so sleeping in the poll loop is safe."

That claim — that a real Kafka consumer can sit through several real seconds
of `time.sleep` inside `wait_until` without losing its group membership — is
currently **never tested with a real sleep, anywhere**:

- Unit tests (`service/tests/test_retrier.py:300-345`) stub `retrier._now`
  and advance a fake clock instantly inside a fake `time.sleep` — correct for
  pinning the chunking *arithmetic*, but no thread ever actually blocks.
- Integration tests set `settings.retry_backoff_base_seconds = 0` for every
  single test via the autouse `wire_settings` fixture
  (`service/tests/integration/conftest.py:172`), so
  `failures.next_not_before()` always computes a delay of `0` and
  `wait_until` returns immediately even against a live Redpanda container.

So a regression in the heartbeat-thread assumption — e.g. a kafka-python
version bump that changes when heartbeats are sent, or a future refactor that
accidentally moves `wait_until`'s sleep onto the same thread doing
`consumer.poll()` — would not be caught by anything in this suite until it
broke in production as a mysterious consumer-group eviction. This plan adds
one integration test that seeds a real multi-second, multi-chunk delay and
confirms the retrier both actually waits and comes back to process the
message correctly.

## Current state

- `service/app/retrier.py:1-23` (module docstring, the claim under test):
  ```
  Delay model: each envelope carries `not_before`; the retrier sleeps until it
  (consumer lag eats most of the delay for free — a message already past its
  `not_before` is processed immediately). Per-message time is bounded:
  `not_before` delays cap at settings.retry_backoff_max_seconds (120s) and every
  model call is bounded by the client's 60s request timeout, both well inside
  infra.MAX_POLL_INTERVAL_MS (600s); kafka-python heartbeats from a background
  thread, so sleeping in the poll loop is safe. If delays ever needed to
  approach the poll interval, the right tool would be consumer.pause() +
  periodic poll() instead of sleeping.
  ```
- `service/app/retrier.py:45,72-88` — `SLEEP_CHUNK_SECONDS = 5` and
  `wait_until`:
  ```python
  def wait_until(not_before) -> None:
      try:
          target = _parse_not_before(not_before)
      except (TypeError, ValueError):
          return
      if target is None:
          return
      while True:
          remaining = (target - _now()).total_seconds()
          if remaining <= 0:
              return
          time.sleep(min(remaining, SLEEP_CHUNK_SECONDS))
  ```
- `service/tests/integration/conftest.py:154-172` (`wire_settings`, autouse
  for every integration test):
  ```python
  monkeypatch.setattr(settings, "retry_backoff_base_seconds", 0)
  ```
  This only affects delays *computed* by `failures.next_not_before()` when
  the retrier itself publishes a new envelope — it does NOT prevent a test
  from seeding an envelope with an explicit `not_before` timestamp via
  `seed_envelope()`, which is the seam this plan uses.
- `service/tests/integration/conftest.py:240-248` — `seed_envelope`:
  ```python
  def seed_envelope(topic: str, edit: dict, **envelope_kwargs) -> dict:
      fake_message = make_message(b"", topic=topic, partition=0, offset=0)
      envelope = failures.make_envelope(
          source="test", message=fake_message, edit=edit, **envelope_kwargs
      )
      produce(topic, json.dumps(envelope).encode(), key=str(edit["id"]).encode())
      return envelope
  ```
- `service/tests/integration/conftest.py:377-389` — `run_retrier_once`:
  ```python
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
  ```
  This builds a **real** `KafkaConsumer` via `retrier.make_consumer()` and
  calls the real `handle_envelope`, which calls the real `wait_until` — the
  exact seam needed. No new fixtures required.
- Existing exemplar test using these helpers together —
  `service/tests/integration/test_failure_paths.py:171-198`
  (`test_retrier_promotes_exhausted_retries_to_dlq`), which already seeds an
  envelope with an explicit `not_before` in the *past* (so `wait_until`
  returns immediately). This plan's new test is the mirror case: `not_before`
  in the future, far enough to force real multi-chunk sleeping.

## Commands you will need

| Purpose   | Command                                                       | Expected on success |
|-----------|----------------------------------------------------------------|---------------------|
| Integration tests (Docker required) | `cd service && uv run pytest -m "integration and not llm"` | all pass |
| Just the new test | `cd service && uv run pytest -m integration -k real_multi_chunk_sleep -v` | 1 passed |
| Lint | `cd service && uv run ruff check .` | exit 0 |

Docker must be running (`docker ps` succeeds) — these tests spin up real
Redpanda + Postgres via testcontainers. If Docker is unreachable, integration
tests are auto-skipped (see `pytest_collection_modifyitems` in
`service/tests/integration/conftest.py:64-70`) — confirm Docker is up before
treating a skip as a pass.

## Scope

**In scope**:
- `service/tests/integration/test_failure_paths.py` — add one new test only.

**Out of scope** (do NOT touch, even though it looks related):
- `service/app/retrier.py` — this plan verifies existing behavior; it does
  not change `wait_until`, `SLEEP_CHUNK_SECONDS`, or the docstring claim.
  If the new test reveals the claim is false, that is a STOP condition (see
  below), not something to fix by editing production code in this plan.
- `service/tests/integration/conftest.py` — no new fixtures are needed; reuse
  `seed_envelope` and `run_retrier_once` as-is.
- Any other integration test file.

## Git workflow

- No feature branch — this repo commits directly on `main`.
- Commit message style: conventional commits, e.g.
  `test(retrier): prove wait_until survives a real multi-chunk sleep`.
- Do NOT push unless explicitly instructed.

## Steps

### Step 1: Add `import time` to the test file

Check `service/tests/integration/test_failure_paths.py`'s existing imports
(currently: `base64`, `json`, `datetime`/`UTC`/`timedelta`, `pytest`, `app.db`,
`app.classifier.Classification`, `app.config.settings`, `sqlalchemy.text`,
`tests.fakes`, `tests.integration.conftest`). Add a top-level `import time`
alongside the stdlib imports.

**Verify**: `cd service && uv run ruff check tests/integration/test_failure_paths.py` →
exit 0 (import ordering/lint clean).

### Step 2: Add the new test

Add to `service/tests/integration/test_failure_paths.py`, after
`test_retrier_promotes_exhausted_retries_to_dlq` (around line 198):

```python
def test_retrier_wait_until_survives_a_real_multi_chunk_sleep(pg_conn):
    """retrier.py's docstring claims kafka-python's background heartbeat
    thread makes sleeping in the poll loop safe. Every other test either
    fakes the clock (unit tests) or zeroes retry_backoff_base_seconds
    (wire_settings, this file's autouse fixture) — nothing has ever made a
    real consumer actually sleep across multiple SLEEP_CHUNK_SECONDS=5s
    chunks and come back to prove group membership survived. This does.
    """
    future = (datetime.now(UTC) + timedelta(seconds=8)).isoformat()
    seed_envelope(
        settings.kafka_retry_topic,
        EDIT,
        reason="transient_exhausted",
        error="previous failure",
        attempts=1,
        not_before=future,
    )
    client = FakeClient([GOOD_JSON])

    start = time.monotonic()
    message, committed = run_retrier_once(client)
    elapsed = time.monotonic() - start

    assert elapsed >= 7, (
        "must have actually slept through wait_until (spanning at least one "
        "5s chunk), not skipped or short-circuited it"
    )
    assert committed is not None and committed == message.offset + 1
    row = fetch_edit_row(pg_conn, EDIT["id"])
    assert row["status"] == "classified", (
        "the consumer must still be a live group member after the real "
        "sleep — an eviction would show up as this assertion failing "
        "(handle_envelope would never have run, or run against a revoked "
        "partition)"
    )
```

This reuses `EDIT`, `GOOD_JSON`, `FakeClient`, `seed_envelope`,
`run_retrier_once`, and `fetch_edit_row`, all already imported/defined at the
top of this file — no new imports beyond `time` (Step 1).

**Verify**: `cd service && uv run pytest -m "integration and not llm" -k real_multi_chunk_sleep -v` →
1 passed. Confirm the test actually took several real seconds (check the
reported duration in pytest's output, e.g. `-v --durations=0`) — a suspiciously
fast pass (well under 7s) means the sleep didn't really happen and the test
is not exercising what it claims to.

### Step 3: Full integration verification

**Verify**: `cd service && uv run pytest -m "integration and not llm"` → all
pass (this also re-runs the full existing integration suite to confirm
nothing else regressed).

## Test plan

- One new test in `service/tests/integration/test_failure_paths.py`:
  `test_retrier_wait_until_survives_a_real_multi_chunk_sleep` — seeds a
  retry envelope with `not_before` 8 real seconds in the future (crossing at
  least one `SLEEP_CHUNK_SECONDS=5` boundary), measures wall-clock elapsed
  time around `run_retrier_once`, and confirms both (a) real time actually
  passed and (b) the message was still correctly processed afterward
  (proving the consumer survived without a rebalance/eviction).
- Structural pattern: `test_retrier_promotes_exhausted_retries_to_dlq`
  (`service/tests/integration/test_failure_paths.py:171-198`) for the
  seed-envelope-then-run-retrier-once shape; this test is its mirror (future
  `not_before` instead of past).
- Verification: `cd service && uv run pytest -m "integration and not llm" -k real_multi_chunk_sleep -v` →
  1 passed, with visibly real elapsed time.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `cd service && uv run pytest -m "integration and not llm" -k real_multi_chunk_sleep -v` passes, 1 test
- [ ] `cd service && uv run pytest -m "integration and not llm"` exits 0 (full integration suite, still green)
- [ ] `cd service && uv run ruff check .` exits 0
- [ ] No files outside `service/tests/integration/test_failure_paths.py` are modified (`git status`)
- [ ] `plans/README.md` status row for 014 updated

## STOP conditions

Stop and report back (do not improvise) if:

- Docker is unavailable and the test is only ever observed as "skipped," not
  "passed" — do not mark this plan DONE on a skip.
- The new test is flaky (fails intermittently on elapsed-time timing) —
  report the actual elapsed times observed rather than loosening the `>= 7`
  assertion speculatively; a real problem here (e.g. CI running under heavy
  load skewing `time.monotonic()` deltas) needs a human judgment call on the
  right margin, not a silent widen-until-it-passes.
- The test reveals the consumer genuinely does get evicted / the retrier
  fails to process the message after the real sleep — this means the
  `retrier.py` docstring's safety claim is **false**. Do not "fix" this by
  editing `retrier.py`; stop and report it as a real, higher-severity finding
  (production impact: a retry with a long-enough backoff could silently lose
  its consumer group membership).
- `seed_envelope`/`run_retrier_once` have changed shape since this plan was
  written (drift) — re-derive the call from the live helper signatures.

## Maintenance notes

- If `SLEEP_CHUNK_SECONDS` (`retrier.py:45`) is ever changed from `5`, bump
  this test's `future = ... timedelta(seconds=8)` and the `elapsed >= 7`
  assertion so the delay still spans at least one full chunk plus a partial
  one — the point of the test is to prove chunking survives a real sleep,
  not just any sleep.
- This test adds ~8 real seconds to the integration suite's runtime — that's
  the intended cost of testing a real timing assumption; do not "optimize"
  it back down to a near-zero delay in a later cleanup pass without
  re-reading why it exists.
- A reviewer should scrutinize: that the assertion on `elapsed` is generous
  enough not to flake on a loaded CI box, but still tight enough to catch a
  regression where the sleep is accidentally skipped entirely.
