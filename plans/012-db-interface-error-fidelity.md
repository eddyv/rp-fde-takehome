# Plan 012: Treat `InterfaceError` as a connection-level failure, not malformed data

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 585b9f6..HEAD -- service/app/db.py service/app/worker.py service/app/retrier.py service/app/sweeper.py service/tests/test_db.py service/tests/test_worker.py service/tests/test_retrier.py service/tests/test_sweeper.py`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: LOW
- **Depends on**: none
- **Category**: security
- **Planned at**: commit `585b9f6`, 2026-07-14

## Why this matters

`db.write_with_reconnect`/`read_with_reconnect` (`service/app/db.py`) and every
caller's crash-vs-DLQ routing (`worker.py`, `retrier.py`, `sweeper.py`) treat
`sqlalchemy.exc.OperationalError` as "the connection died — reconnect once,
and if it's still dead, crash so Kafka redelivers." Everything else that
subclasses `sqlalchemy.exc.SQLAlchemyError` is treated as "this data will
never fit the schema — park it to the DLQ and move on."

That split misses a real case: `sqlalchemy.exc.InterfaceError` is a **sibling**
of `OperationalError`, not a subclass (confirmed: neither is an ancestor of
the other in either SQLAlchemy's or psycopg's exception hierarchy — see
"Current state" below for the verification). psycopg genuinely raises
`InterfaceError` for connection-adjacent failures — e.g. `"the cursor is
closed"` (`psycopg/_cursor_base.py:381,608`) after a connection has already
been torn down. Today, if that surfaces from a dropped Postgres connection:
`write_with_reconnect`/`read_with_reconnect` never attempt a reconnect (they
only catch `OperationalError`), and the caller's `except OperationalError:
raise` never matches either, so the error falls into the broader
`SQLAlchemyError` branch and a **perfectly good edit gets permanently parked
to the DLQ as "malformed data"** instead of crashing and letting Kafka
redeliver it once the connection recovers. This is silent, incorrect data
loss dressed up as a routine DLQ entry.

The test suite's fakes (`tests/fakes.py`'s `FakeConn.fail_with`) only ever
construct `sqlalchemy.exc.OperationalError`, so every existing reconnect/crash
test passes regardless of whether this gap exists — the fake's failure mode
was shaped to match what the code already catches, not what the real driver
can actually raise.

## Current state

- `service/app/db.py:178-190` — `write_with_reconnect`:
  ```python
  def write_with_reconnect(
      conn: Connection, write: Callable[[Connection], None]
  ) -> Connection:
      """Run a write, reconnecting once if Postgres dropped the connection."""
      try:
          write(conn)
      except sqlalchemy.exc.OperationalError:
          logger.warning("postgres connection lost, reconnecting")
          with suppress(Exception):
              conn.close()
          conn = connect()
          write(conn)
      return conn
  ```
- `service/app/db.py:193-204` — `read_with_reconnect`, identical shape for reads.
- `service/app/worker.py:72-83`:
  ```python
  try:
      return _handle_edit(client, conn, consumer, producer, breaker, message, edit)
  except sqlalchemy.exc.OperationalError:
      raise  # connection-level failure even after reconnect: crash, redeliver
  except sqlalchemy.exc.SQLAlchemyError as error:
      # Data-shaped failure (e.g. byte_delta that isn't an int): retrying
      # the same message can never succeed, so park it and move on.
      logger.warning(
          "edit %s does not fit the schema -> DLQ: %s", edit.get("id"), error
      )
      failures.park_malformed(producer, consumer, message, error, source="worker")
      return conn
  ```
- `service/app/retrier.py:116-129` — same shape, in `handle_envelope`.
- `service/app/sweeper.py:166-179` — same shape, inline in the sweep loop:
  ```python
  try:
      conn = db.write_with_reconnect(
          conn, lambda c, e=edit, r=result: db.upsert_edit(c, e, r)
      )
  except sqlalchemy.exc.OperationalError:
      raise  # connection-level failure even after reconnect: abort
  except sqlalchemy.exc.SQLAlchemyError as error:
      logger.error(
          "edit %s does not fit the schema, skipping: %s", edit.get("id"), error
      )
      _commit(consumer, message)
      counts["skipped"] += 1
      continue
  ```
- `service/app/db.py:120-130` — `connect()`'s own startup warm-up retry loop
  (catches `OperationalError` while Postgres is still booting). **Do not
  change this one** — see Scope below.

Verified class hierarchy (ran in this repo's venv):
```
$ uv run python -c "
import sqlalchemy.exc as exc
print(issubclass(exc.InterfaceError, exc.OperationalError))
print(issubclass(exc.OperationalError, exc.InterfaceError))
"
False
False
```
Both are direct siblings under `sqlalchemy.exc.DBAPIError`. Same for the
underlying psycopg exceptions (`psycopg.InterfaceError` and
`psycopg.OperationalError` are unrelated siblings under `psycopg.Error`).

Existing test pattern to match — `service/tests/test_db.py:118-131`:
```python
def test_write_with_reconnect_retries_once_on_fresh_connection(monkeypatch):
    dead = FakeConn(
        fail_with=sqlalchemy.exc.OperationalError(
            "stmt", {}, psycopg.OperationalError("server closed")
        )
    )
    fresh = FakeConn()
    monkeypatch.setattr(db, "connect", lambda: fresh)

    returned = db.write_with_reconnect(dead, lambda c: c.execute("SELECT 1"))

    assert returned is fresh, "caller must keep using the reconnected conn"
    assert [sql for sql, _ in fresh.executed] == ["SELECT 1"]
```
This already wraps a **real** psycopg exception inside a **real** SQLAlchemy
wrapper exception — that convention is correct and this plan reuses it
verbatim, just for `InterfaceError` instead of `OperationalError`.

Existing crash-path convention to match — `service/tests/test_worker.py:163-174`
(`test_config_error_crashes_without_commit_or_publish`) shows the pattern for
asserting "must crash, must not commit, must not publish"; adapt its
assertion style (not its trigger — that test is about `ModelConfigError`, a
different branch) for the new connection-error crash test.

## Commands you will need

| Purpose   | Command                                              | Expected on success |
|-----------|-------------------------------------------------------|---------------------|
| Unit tests (this repo's tests dir) | `cd service && uv run pytest` | all pass, includes new tests |
| Type check | `cd service && uv run ty check app` | exit 0, no errors |
| Lint | `cd service && uv run ruff check .` | exit 0 |
| Mutation test (scoped) | `cd service && uv run mutmut run` | new mutants on the touched `except` lines are killed (run `uv run mutmut results` to inspect) |

Run all commands from `service/` (the workspace member with its own
`pyproject.toml`).

## Scope

**In scope**:
- `service/app/db.py` — broaden `write_with_reconnect` (line 184) and
  `read_with_reconnect` (line 199) to also treat `sqlalchemy.exc.InterfaceError`
  as reconnect-worthy.
- `service/app/worker.py` — broaden the crash-vs-DLQ split at line 74.
- `service/app/retrier.py` — broaden the crash-vs-DLQ split at line 120.
- `service/app/sweeper.py` — broaden the crash-vs-DLQ split at line 171.
- `service/tests/test_db.py` — new tests for the broadened reconnect behavior.
- `service/tests/test_worker.py`, `service/tests/test_retrier.py`,
  `service/tests/test_sweeper.py` — new tests proving the crash path now
  fires correctly for `InterfaceError` too.

**Out of scope** (do NOT touch, even though it looks related):
- `service/app/db.py:120-130` (`connect()`'s own startup retry loop) — that
  loop only retries the *initial* `engine.connect()` while Postgres is
  booting, a different failure shape (connection refused) than a live
  connection going bad mid-query. Real production Postgres drivers raise
  `OperationalError` for "can't connect yet," not `InterfaceError`; broadening
  this loop is unrelated to the bug this plan fixes. (Plan 013, if selected,
  covers this loop's missing unit-test coverage — a separate finding.)
- `service/app/db.py:112` (`_execute`'s `except sqlalchemy.exc.DBAPIError:
  conn.rollback(); raise`) — this always re-raises the *original* exception
  unchanged after rolling back; it doesn't make a crash-vs-DLQ decision, so
  there is nothing to broaden here.
- Any change to `sql/schema.sql` or the Postgres container/version.

## Git workflow

- Branch: none required — this repo commits directly on `main` per its
  existing history (see `git log --oneline`); do not create a feature branch
  unless the operator directs otherwise.
- Commit message style: conventional commits, matching recent history, e.g.
  `fix(db): treat InterfaceError as reconnect-worthy, not malformed data`.
- Do NOT push unless explicitly instructed.
- Do NOT touch the README `## Tradeoffs` section — repo convention reserves
  that for the human author only.

## Steps

### Step 1: Add a shared connection-error tuple in `db.py`

Add, near the top of `service/app/db.py` (after the imports, before
`normalize_dsn`):

```python
# Connection-level failures worth reconnecting for (and, if reconnect also
# fails, crashing so Kafka redelivers) rather than parking as malformed data.
# InterfaceError is a *sibling* of OperationalError, not a subclass (verified
# via issubclass() against both sqlalchemy.exc and psycopg) — psycopg raises
# it for e.g. "the cursor is closed" after a connection has already died.
CONNECTION_ERRORS = (sqlalchemy.exc.OperationalError, sqlalchemy.exc.InterfaceError)
```

**Verify**: `cd service && uv run python -c "from app.db import CONNECTION_ERRORS; print(CONNECTION_ERRORS)"` →
prints a 2-tuple of the two exception classes, exit 0.

### Step 2: Broaden `write_with_reconnect` and `read_with_reconnect`

In `service/app/db.py`, change both `except sqlalchemy.exc.OperationalError:`
lines (184 and 199) to `except CONNECTION_ERRORS:`.

**Verify**: `cd service && grep -n "except CONNECTION_ERRORS" app/db.py` →
2 matches.

### Step 3: Broaden the caller-side crash-vs-DLQ split

In each of `service/app/worker.py:74`, `service/app/retrier.py:120`,
`service/app/sweeper.py:171`, change
`except sqlalchemy.exc.OperationalError:` to `except db.CONNECTION_ERRORS:`
(all three files already `from app import db`, confirm this import is
present before relying on it — it is, per each file's existing `from app import
db, failures[, infra, routing]` line).

**Verify**: `cd service && grep -rn "except db.CONNECTION_ERRORS" app/worker.py app/retrier.py app/sweeper.py` →
3 matches, one per file.

### Step 4: Add reconnect-on-`InterfaceError` tests to `test_db.py`

Add two new tests immediately after
`test_write_with_reconnect_swallows_a_failing_close_on_the_broken_conn`
(around line 150) and after the read equivalent (around line 192), each
mirroring the existing `OperationalError` reconnect test but with
`InterfaceError`:

```python
def test_write_with_reconnect_also_retries_on_interface_error(monkeypatch):
    dead = FakeConn(
        fail_with=sqlalchemy.exc.InterfaceError(
            "stmt", {}, psycopg.InterfaceError("the cursor is closed")
        )
    )
    fresh = FakeConn()
    monkeypatch.setattr(db, "connect", lambda: fresh)

    returned = db.write_with_reconnect(dead, lambda c: c.execute("SELECT 1"))

    assert returned is fresh, "InterfaceError must trigger reconnect too"
    assert [sql for sql, _ in fresh.executed] == ["SELECT 1"]


def test_read_with_reconnect_also_retries_on_interface_error(monkeypatch):
    dead = FakeConn(
        fail_with=sqlalchemy.exc.InterfaceError(
            "stmt", {}, psycopg.InterfaceError("the cursor is closed")
        )
    )
    fresh = FakeConn(statuses={"42": "classified"})
    monkeypatch.setattr(db, "connect", lambda: fresh)

    returned, status = db.read_with_reconnect(
        dead, lambda c: db.fetch_edit_status(c, EDIT)
    )

    assert returned is fresh, "InterfaceError must trigger reconnect too"
    assert status == "classified"
```

**Verify**: `cd service && uv run pytest tests/test_db.py -k interface_error -v` →
2 passed.

### Step 5: Add crash-not-DLQ tests to `test_worker.py`, `test_retrier.py`, `test_sweeper.py`

For each file, add one test proving that when the write **and** the
reconnect attempt both fail with a connection-level error, the caller
re-raises (crashes) instead of parking to the DLQ. Use two failing
`FakeConn`s — the original, and the one `db.connect` reconnects to — so the
error survives past `write_with_reconnect`'s single retry, matching how
`write_with_reconnect` itself is implemented (see Current state above: it
only wraps the *first* `write(conn)` call in `try/except`; the retry's
`write(conn)` after reconnecting is unguarded and propagates directly).

`service/tests/test_worker.py` (add near
`test_schema_mismatch_row_goes_to_dlq_as_malformed`, line 82):

```python
def test_connection_error_surviving_reconnect_crashes_not_dlq():
    # Both the original and the reconnected conn are still dead: proves the
    # crash path fires for InterfaceError, not just OperationalError, and
    # that it is NOT swallowed into a DLQ "malformed" park.
    log: list = []
    dead = FakeConn(
        log,
        fail_with=sqlalchemy.exc.InterfaceError(
            "stmt", {}, psycopg.InterfaceError("the cursor is closed")
        ),
    )
    still_dead = FakeConn(
        fail_with=sqlalchemy.exc.InterfaceError(
            "stmt", {}, psycopg.InterfaceError("the cursor is closed")
        )
    )
    consumer, producer = FakeConsumer(log), FakeProducer(log)
    breaker = failures.CircuitBreaker(25)
    message = make_message(json.dumps(EDIT).encode())

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(db, "connect", lambda: still_dead)
        with pytest.raises(sqlalchemy.exc.InterfaceError):
            handle_message(FakeClient([GOOD_JSON]), dead, consumer, producer, breaker, message)

    assert consumer.commits == 0, "must not commit on a connection-level crash"
    assert producer.sent == [], "must not park to DLQ — this is not malformed data"
```

Add `import psycopg` to `test_worker.py`'s imports if not already present
(check first — `test_worker.py:7` already has it).

Mirror the same test shape in `service/tests/test_retrier.py` (calling
`handle_envelope` instead of `handle_message`; use its existing
`make_envelope_message`/fixture helpers — follow the file's established
pattern for constructing a retry-topic message) and
`service/tests/test_sweeper.py` (calling `main()` via the file's existing
`run_sweeper` helper, asserting `pytest.raises` around it and that no
`park_malformed`/DLQ envelope is produced).

**Verify**: `cd service && uv run pytest tests/test_worker.py tests/test_retrier.py tests/test_sweeper.py -k connection_error -v` →
3 passed.

### Step 6: Full verification

**Verify**: `cd service && uv run pytest` → all pass (unit suite; no Docker
needed). Then `cd service && uv run ty check app` → exit 0. Then
`cd service && uv run ruff check .` → exit 0.

## Test plan

- `test_db.py`: `test_write_with_reconnect_also_retries_on_interface_error`,
  `test_read_with_reconnect_also_retries_on_interface_error` — prove the
  broadened reconnect trigger.
- `test_worker.py`, `test_retrier.py`, `test_sweeper.py`: one
  `test_connection_error_surviving_reconnect_crashes_not_dlq`-style test
  each — prove the crash path (not the DLQ path) fires for `InterfaceError`
  once reconnect also fails.
- Structural pattern: `test_db.py:118-131` for the reconnect tests;
  `test_worker.py:82-105` (`test_schema_mismatch_row_goes_to_dlq_as_malformed`)
  for the FakeConn `fail_with` wiring, and
  `test_worker.py:163-174` (`test_config_error_crashes_without_commit_or_publish`)
  for the "must crash, must not commit/publish" assertion shape.
- Verification: `cd service && uv run pytest` → all pass, 5 new tests
  present and green.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `cd service && uv run pytest` exits 0
- [ ] `cd service && uv run ty check app` exits 0
- [ ] `cd service && uv run ruff check .` exits 0
- [ ] `grep -rn "except sqlalchemy.exc.OperationalError" service/app/db.py service/app/worker.py service/app/retrier.py service/app/sweeper.py` returns **zero** matches at the reconnect/crash-split sites (line 120 in `db.py`'s `connect()` is explicitly out of scope and should still say `OperationalError` — confirm with `grep -n "except sqlalchemy.exc.OperationalError" service/app/db.py` showing exactly one remaining hit, at `connect()`)
- [ ] `grep -rn "CONNECTION_ERRORS" service/app` shows the constant defined once in `db.py` and referenced in `worker.py`, `retrier.py`, `sweeper.py`
- [ ] No files outside the in-scope list are modified (`git status`)
- [ ] `plans/README.md` status row for 012 updated

## STOP conditions

Stop and report back (do not improvise) if:

- The code at any "Current state" location doesn't match the excerpts above
  (drift since this plan was written).
- `write_with_reconnect`/`read_with_reconnect` have been refactored to a
  shape where "the retry's `write(conn)` call is unguarded" (Step 5's
  premise) is no longer true — re-derive the crash-triggering test setup
  from the live code instead of assuming this plan's excerpt still applies.
- A step's verification fails twice after a reasonable fix attempt.
- You find that `sqlalchemy.exc.InterfaceError` is *not* actually reachable
  from psycopg 3 in the pinned version (`psycopg[binary]==3.3.4` per
  `service/pyproject.toml`) — re-verify with
  `uv run python -c "import psycopg; print(psycopg.InterfaceError)"` before
  assuming the premise is wrong.

## Maintenance notes

- If a future change introduces a *new* connection-level psycopg/SQLAlchemy
  exception class (e.g. a driver upgrade adds a new sibling under
  `DBAPIError`), it needs the same triage: is it "the connection died" or
  "this data will never fit"? Add it to `CONNECTION_ERRORS` only if it's the
  former.
- A reviewer should scrutinize: that `CONNECTION_ERRORS` is imported/used
  consistently (not reintroducing a bare `sqlalchemy.exc.OperationalError`
  check somewhere this plan missed), and that the new tests actually fail
  before Steps 1–3 land (a quick sanity check: temporarily revert Steps 1–3,
  confirm the new tests in Steps 4–5 fail, then re-apply).
- Deferred out of this plan: no integration test forces a real
  `psycopg.InterfaceError` against the live testcontainers Postgres (unlike
  the real `DataError` integration test at
  `tests/integration/test_failure_paths.py:100`) — reliably triggering
  `InterfaceError` specifically (vs. `OperationalError`) against a live
  connection is brittle (depends on exact timing of server-side connection
  teardown vs. client-side cursor state) and the unit-level fidelity fix
  here is the leveraged part of this finding. If it's ever worth doing,
  severing the connection via the Docker API mid-write and asserting on
  which exception class actually surfaces would be the way in.
