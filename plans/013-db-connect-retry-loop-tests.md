# Plan 013: Add direct unit tests for `db.connect()`'s Postgres warm-up retry loop

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 585b9f6..HEAD -- service/app/db.py service/tests/test_db.py`
> If either file changed since this plan was written, compare the
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

`db.connect()` (`service/app/db.py:120-130`) is the retry loop that lets the
worker/retrier/sweeper start up before Postgres has finished booting: it
calls `get_engine().connect()`, and on `sqlalchemy.exc.OperationalError`
sleeps and retries up to `retries` times before giving up. This is the exact
same shape as `infra.make_consumer`'s Kafka-broker warm-up retry loop
(`service/app/infra.py:19-37`) — but unlike that loop, which has three direct
unit tests pinning its retry count, delay, and exhaustion behavior
(`service/tests/test_infra.py:51-121`), `db.connect()` has **zero** direct
unit-test coverage. Every existing unit test that touches reconnection
monkeypatches `db.connect` away wholesale (e.g.
`service/tests/test_db.py:125`: `monkeypatch.setattr(db, "connect", lambda:
fresh)`) rather than calling the real function.

That means a regression in this loop — an off-by-one on `attempt == retries -
1`, the wrong exception class being caught, or the `time.sleep(delay)` call
being silently dropped (turning a Postgres warm-up race into a tight
crash-loop against a real container) — would ship with no unit test failing.
Only a live-Postgres integration run would ever exercise the real loop, and
none of the current integration tests intentionally delay Postgres's
availability to hit this path either.

## Current state

- `service/app/db.py:33-46`:
  ```python
  _engines: dict[str, Engine] = {}


  def get_engine() -> Engine:
      # Keyed by DSN because integration fixtures repoint settings.postgres_dsn
      # at a per-session container; a module-level singleton would freeze the
      # first DSN seen.
      dsn = normalize_dsn(settings.postgres_dsn)
      if dsn not in _engines:
          _engines[dsn] = create_engine(
              dsn, isolation_level="AUTOCOMMIT", pool_pre_ping=True
          )
      return _engines[dsn]
  ```
- `service/app/db.py:120-130`:
  ```python
  def connect(retries: int = 30, delay: float = 2.0) -> Connection:
      """Postgres may still be warming up when the worker starts."""
      for attempt in range(retries):
          try:
              return get_engine().connect()
          except sqlalchemy.exc.OperationalError as error:
              if attempt == retries - 1:
                  raise
              logger.info("postgres not ready (%s), retrying...", error)
              time.sleep(delay)
      raise RuntimeError("unreachable")
  ```
- The pattern to mirror — `service/tests/test_infra.py:51-121` (all three
  tests for `infra.make_consumer`'s equivalent loop):
  ```python
  def test_make_consumer_retries_until_broker_up_then_returns(monkeypatch):
      monkeypatch.setattr(settings, "kafka_brokers", "h1:9092,h2:9092")
      sleeps: list = []
      monkeypatch.setattr(infra.time, "sleep", lambda s: sleeps.append(s))

      calls: list = []
      sentinel = SimpleNamespace()

      def fake_consumer(*args, **kwargs):
          calls.append((args, kwargs))
          if len(calls) < 3:
              raise KafkaError("not up yet")
          return sentinel

      monkeypatch.setattr(infra, "KafkaConsumer", fake_consumer)

      result = infra.make_consumer("a-topic", "a-group", retries=5, delay=2.0)

      assert result is sentinel
      assert len(calls) == 3, "two failures then a success"
      assert sleeps == [2.0, 2.0], "one sleep per failed attempt, none after success"
  ```
  `db.connect()`'s analogous seam is `get_engine()`, called fresh on every
  loop iteration (line 124: `get_engine().connect()`) — monkeypatch
  `db.get_engine` to a fake factory whose returned object's `.connect()`
  fails N times then succeeds, exactly as `infra.KafkaConsumer` is
  monkeypatched today.
- `service/tests/test_db.py` top imports (already present, no changes
  needed): `import psycopg`, `import sqlalchemy.exc`, `from app import db`.

## Commands you will need

| Purpose   | Command                                    | Expected on success |
|-----------|---------------------------------------------|---------------------|
| Unit tests | `cd service && uv run pytest tests/test_db.py -v` | all pass, includes 3 new tests |
| Full unit suite | `cd service && uv run pytest` | all pass |
| Type check | `cd service && uv run ty check app` | exit 0 |
| Lint | `cd service && uv run ruff check .` | exit 0 |

## Scope

**In scope**:
- `service/tests/test_db.py` — add three new tests only. No production code
  changes in this plan.

**Out of scope** (do NOT touch, even though it looks related):
- `service/app/db.py` — this plan adds test coverage for the existing loop
  as-is; it does not change `connect()`'s behavior. (If a bug is discovered
  by the new tests, that's a STOP condition below, not something to silently
  fix here — a behavior change belongs in its own plan.)
- `service/app/infra.py` / `service/tests/test_infra.py` — the exemplar
  pattern, referenced but not modified.
- Plan 012's `CONNECTION_ERRORS` broadening (a different finding) — if plan
  012 has already landed, `db.connect()`'s own loop at line 125 is
  deliberately left on the narrower `sqlalchemy.exc.OperationalError` (see
  plan 012's Scope section for why); do not "helpfully" widen it here either.

## Git workflow

- No feature branch — this repo commits directly on `main`.
- Commit message style: conventional commits, e.g.
  `test(db): cover connect()'s Postgres warm-up retry loop directly`.
- Do NOT push unless explicitly instructed.

## Steps

### Step 1: Add `test_connect_retries_until_engine_up_then_returns`

Add to `service/tests/test_db.py`, near the other reconnect tests:

```python
from types import SimpleNamespace


def test_connect_retries_until_engine_up_then_returns(monkeypatch):
    sleeps: list = []
    monkeypatch.setattr(db.time, "sleep", lambda s: sleeps.append(s))
    calls: list = []
    sentinel = SimpleNamespace()

    class FakeEngine:
        def connect(self):
            calls.append(())
            if len(calls) < 3:
                raise sqlalchemy.exc.OperationalError(
                    "connect", {}, psycopg.OperationalError("could not connect")
                )
            return sentinel

    monkeypatch.setattr(db, "get_engine", lambda: FakeEngine())

    result = db.connect(retries=5, delay=2.0)

    assert result is sentinel
    assert len(calls) == 3, "two failures then a success"
    assert sleeps == [2.0, 2.0], "one sleep per failed attempt, none after success"
```

(Add the `from types import SimpleNamespace` import at the top of the file
alongside the existing imports, if not already there.)

**Verify**: `cd service && uv run pytest tests/test_db.py -k retries_until_engine_up -v` →
1 passed.

### Step 2: Add `test_connect_default_retries_and_delay`

```python
def test_connect_default_retries_and_delay(monkeypatch):
    # Explicit retries/delay are pinned by the test above; this test is the
    # only one exercising the *defaults* (retries=30, delay=2.0), mirroring
    # test_infra.py's equivalent split between explicit-args and defaults.
    sleeps: list = []
    monkeypatch.setattr(db.time, "sleep", lambda s: sleeps.append(s))
    calls: list = []

    class AlwaysFailsEngine:
        def connect(self):
            calls.append(())
            raise sqlalchemy.exc.OperationalError(
                "connect", {}, psycopg.OperationalError("could not connect")
            )

    monkeypatch.setattr(db, "get_engine", lambda: AlwaysFailsEngine())

    with pytest.raises(sqlalchemy.exc.OperationalError):
        db.connect()

    assert len(calls) == 30, "default retries must stay 30"
    assert sleeps == [2.0] * 29, "default delay must stay 2.0s, none after the last"
```

Add `import pytest` to `test_db.py`'s imports if not already present (check
first).

**Verify**: `cd service && uv run pytest tests/test_db.py -k default_retries_and_delay -v` →
1 passed.

### Step 3: Add `test_connect_raises_after_exhausting_retries`

```python
def test_connect_raises_after_exhausting_retries(monkeypatch):
    sleeps: list = []
    monkeypatch.setattr(db.time, "sleep", lambda s: sleeps.append(s))

    class AlwaysFailsEngine:
        def connect(self):
            raise sqlalchemy.exc.OperationalError(
                "connect", {}, psycopg.OperationalError("could not connect")
            )

    monkeypatch.setattr(db, "get_engine", lambda: AlwaysFailsEngine())

    with pytest.raises(sqlalchemy.exc.OperationalError):
        db.connect(retries=2, delay=1.0)

    assert sleeps == [1.0], "sleep between attempts, none after the last failure"
```

**Verify**: `cd service && uv run pytest tests/test_db.py -k raises_after_exhausting_retries -v` →
1 passed.

### Step 4: Full verification

**Verify**: `cd service && uv run pytest` → all pass. Then
`cd service && uv run ty check app` → exit 0. Then
`cd service && uv run ruff check .` → exit 0.

## Test plan

- New tests, all in `service/tests/test_db.py`:
  - `test_connect_retries_until_engine_up_then_returns` — two failures then
    success, explicit `retries=5, delay=2.0`.
  - `test_connect_default_retries_and_delay` — the *default* `retries=30,
    delay=2.0` are pinned (a mutation changing either default is caught).
  - `test_connect_raises_after_exhausting_retries` — small `retries=2`,
    always fails, confirms the loop gives up and re-raises rather than
    hitting the `raise RuntimeError("unreachable")` fallback.
- Structural pattern: `service/tests/test_infra.py:51-121` (`make_consumer`'s
  three equivalent tests) — same three-test shape, same assertions on call
  count and captured sleep values, applied to `db.connect()`'s seam
  (`db.get_engine`) instead of `infra.KafkaConsumer`.
- Verification: `cd service && uv run pytest` → all pass, 3 new tests
  present and green.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `cd service && uv run pytest tests/test_db.py -v` exits 0, shows the 3
      new tests passing
- [ ] `cd service && uv run pytest` exits 0 (full suite)
- [ ] `cd service && uv run ty check app` exits 0
- [ ] `cd service && uv run ruff check .` exits 0
- [ ] No files outside `service/tests/test_db.py` are modified (`git status`)
- [ ] `plans/README.md` status row for 013 updated

## STOP conditions

Stop and report back (do not improvise) if:

- The code at `service/app/db.py:120-130` doesn't match the excerpt above
  (drift since this plan was written).
- Any new test reveals `db.connect()` does NOT actually behave as documented
  (e.g. the default retry count isn't 30, or a sleep is missing) — that is a
  real bug, not something to quietly "fix" as part of a test-only plan. Stop
  and report the discrepancy instead of changing `db.py`.
- A step's verification fails twice after a reasonable fix attempt.

## Maintenance notes

- If `db.connect()`'s signature or defaults ever change, `test_connect_default_retries_and_delay`
  is the test that pins them — update its literal `30`/`2.0`/`29` values
  deliberately, not incidentally.
- A reviewer should scrutinize that the new `FakeEngine`/`AlwaysFailsEngine`
  test doubles are scoped to their own test functions (not hoisted into
  `tests/fakes.py`) unless a third consumer of the same shape appears later —
  premature sharing here would couple otherwise-independent tests (a pattern
  this repo's `plans/README.md` "Findings considered and rejected" section
  already flags as low-value for `make_fixtures`-style helpers).
