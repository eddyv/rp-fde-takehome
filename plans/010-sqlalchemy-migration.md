# Plan 010: Migrate all Postgres access from raw psycopg3 to SQLAlchemy 2.x

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat c5416e5..HEAD -- service/ sql/schema.sql`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition. NOTE: at planning time the tree had
> STAGED, UNCOMMITTED changes (see "Current state — working-tree note"); the
> excerpts below describe the staged state, which is expected to be committed
> before you start. If `git status` shows those files still staged/dirty,
> STOP and ask the operator to commit first.

## Status

- **Priority**: P2
- **Effort**: L
- **Risk**: MED
- **Depends on**: none (plan 011 depends on this)
- **Category**: migration
- **Planned at**: commit `c5416e5`, 2026-07-14

## Why this matters

The service talks to Postgres through raw psycopg3 in five places (write path,
API read path, three pipeline consumers) plus test fixtures. The maintainer
wants SQLAlchemy as the single data-access layer: typed statements instead of
string SQL, a pooled engine instead of hand-rolled connect/reconnect loops,
and a foundation for fastapi-pagination (plan 011), which requires SQLAlchemy
selectables. After this plan, psycopg remains ONLY as SQLAlchemy's driver
(`postgresql+psycopg://` dialect) and as the realistic `.orig` inside wrapped
exceptions in unit tests. **This plan must produce zero observable behavior
change** — same API response bytes, same failure routing, same idempotency
guarantees. The existing test suite, unchanged in its assertions, is the
acceptance gate.

## Current state

### Working-tree note (read before the drift check)

At planning time HEAD was `c5416e5` with three files STAGED but uncommitted:
`service/app/api.py` (imports `VALID_STATUSES` from `app.classifier` instead
of defining it locally), `service/app/classifier.py` (now defines
`VALID_STATUSES = {"classified", "failed"}` near the top, after `logger`),
and `sql/schema.sql` (the `edits_status_idx` index was REMOVED). The excerpts
below reflect that staged state.

### Files and roles

- `service/app/db.py` — the write-path DB module. Three raw-SQL string
  constants and five functions. Everything in the pipeline goes through it.
- `service/app/api.py` — `GET /edits`: cursor pagination (hand-rolled keyset
  on `(processed_at DESC, id DESC)`, base64-JSON cursor), label/status
  filters, connection-per-request psycopg with `dict_row`. Response contract:
  `{"items": [...], "next_cursor": "<opaque>" | null}`. **The contract must
  not change in this plan** (plan 011 changes it).
- `service/app/worker.py`, `service/app/retrier.py`, `service/app/sweeper.py`
  — Kafka consumers; each has a load-bearing psycopg exception split (below).
- `service/app/config.py:20` — `postgres_dsn: str = "postgresql://wiki:wiki@localhost:5433/wiki"`
  (pydantic-settings; same plain-scheme DSN in docker-compose worker/retrier/api
  env and the testcontainers fixture). **Do not change any DSN value.**
- `sql/schema.sql` — DDL source of truth (applied by compose initdb and the
  test fixture). Stays the source of truth; no Alembic, no `create_all`.
- `service/tests/fakes.py` — `FakeConn` (psycopg-connection stand-in).
- `service/tests/test_db.py` — 7 unit tests pinning SQL identity + param dicts.
- `service/tests/integration/conftest.py` — testcontainers Postgres fixtures.
- `service/tests/integration/{test_api,test_db_guards,test_failure_paths,test_sweeper_drain,test_pipeline_e2e}.py`
  — integration tests asserting DB state via raw SQL on the `pg_conn` fixture.

### Key excerpts (verify you're looking at the same code)

`service/app/db.py:12-24` — the classified upsert (identity-asserted by tests):

```python
UPSERT_SQL = """
INSERT INTO edits (id, title, editor, comment, byte_delta, label, confidence,
                   reasoning, model, status, event_time, processed_at)
VALUES (%(id)s, %(title)s, %(editor)s, %(comment)s, %(byte_delta)s, %(label)s,
        %(confidence)s, %(reasoning)s, %(model)s, %(status)s, %(event_time)s, now())
ON CONFLICT (id) DO UPDATE SET
    label = EXCLUDED.label,
    confidence = EXCLUDED.confidence,
    reasoning = EXCLUDED.reasoning,
    model = EXCLUDED.model,
    status = EXCLUDED.status,
    processed_at = now()
"""
```

`service/app/db.py:28-41` — the failed upsert with the never-downgrade guard
(the single most load-bearing line in the module):

```python
FAILED_UPSERT_SQL = """
INSERT INTO edits (id, title, editor, comment, byte_delta, label, confidence,
                   reasoning, model, status, event_time, processed_at)
VALUES (%(id)s, %(title)s, %(editor)s, %(comment)s, %(byte_delta)s, NULL,
        NULL, %(reasoning)s, NULL, 'failed', %(event_time)s, now())
ON CONFLICT (id) DO UPDATE SET
    label = NULL,
    confidence = NULL,
    reasoning = EXCLUDED.reasoning,
    model = NULL,
    status = 'failed',
    processed_at = now()
WHERE edits.status IS DISTINCT FROM 'classified'
"""
```

`service/app/db.py:43` — `STATUS_SQL = "SELECT status FROM edits WHERE id = %(id)s"`

`service/app/db.py:46-56` — warmup connect (autocommit, 30×2s retry on
`psycopg.OperationalError`). `db.py:104-126` — `write_with_reconnect` /
`read_with_reconnect`: run the callable, on `psycopg.OperationalError` log,
`connect()` once, retry, return the (possibly new) conn.

`service/app/worker.py:89-100` — the exception split (same shape in
`retrier.py:129-142` and `sweeper.py:174-188`; sweeper's poison branch
commits+skips instead of parking):

```python
    try:
        return _handle_edit(client, conn, consumer, producer, breaker, message, edit)
    except psycopg.OperationalError:
        raise  # connection-level failure even after reconnect: crash, redeliver
    except psycopg.Error as error:
        # Data-shaped failure (e.g. byte_delta that isn't an int): retrying
        # the same message can never succeed, so park it and move on.
        logger.warning(
            "edit %s does not fit the schema -> DLQ: %s", edit.get("id"), error
        )
        failures.park_malformed(producer, consumer, message, error, source="worker")
        return conn
```

`service/app/api.py` (staged state) — connection-per-request and keyset SQL:

```python
    if cursor is not None:
        cursor_processed_at, cursor_id = decode_cursor(cursor)
        conditions.append("(processed_at, id) < (%s, %s)")
        params.extend([cursor_processed_at, cursor_id])

    query = "SELECT * FROM edits"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    # id breaks processed_at ties so pages never skip or repeat rows.
    query += " ORDER BY processed_at DESC, id DESC LIMIT %s"
    params.append(limit + 1)  # the extra row tells us whether a next page exists

    with psycopg.connect(settings.postgres_dsn, row_factory=dict_row) as conn:
        rows = conn.execute(query, params).fetchall()
```

`service/tests/fakes.py:126-138` — `FakeConn.execute` (identity check against
`db.STATUS_SQL`, `fail_with` injection, records `(sql, params)` into
`self.executed` and `("db",)` into the shared ordering log).

`service/tests/test_db.py:26,49` — `assert sql is db.UPSERT_SQL` /
`assert sql is db.FAILED_UPSERT_SQL` (object identity) plus full param-dict
equality (id coerced to `"42"`, reasoning capped at 500 chars).

`service/tests/test_worker.py:108` — `assert "status IS DISTINCT FROM 'classified'" in sql`.
`test_worker.py:85`, `test_retrier.py:254`, `test_sweeper.py:323` —
`FakeConn(..., fail_with=psycopg.DataError("invalid input for type integer"))`.
`test_db.py:80,102` — `FakeConn(fail_with=psycopg.OperationalError("server closed"))`.

`service/tests/integration/conftest.py:84-90` and `:191-199`:

```python
@pytest.fixture(scope="session")
def postgres_dsn():
    with PostgresContainer("postgres:16", driver=None) as postgres:
        dsn = postgres.get_connection_url()
        with psycopg.connect(dsn, autocommit=True) as conn:
            conn.execute(SCHEMA_SQL.read_text())
        yield dsn

@pytest.fixture
def pg_conn(postgres_dsn):
    """Real psycopg connection, rows as dicts; table truncated per test."""
    conn = psycopg.connect(postgres_dsn, autocommit=True, row_factory=dict_row)
    conn.execute("TRUNCATE edits")
    try:
        yield conn
    finally:
        conn.close()
```

Integration assertion pattern (~30 sites across the 5 integration test files):
`pg_conn.execute("SELECT * FROM edits WHERE id = %s", (X,)).fetchone()` with
dict rows (via `row_factory=dict_row`), plus `SELECT count(*) AS n FROM edits`
sites, plus `seed_edit` in `test_api.py:33-38` (direct INSERT with `%s`).

### Conventions to match

- Descriptive variable names over abbreviations (AGENTS.md).
- Every change needs a related test; tests must pin data, not just routing —
  "DB parameter dicts and SQL identity" is called out explicitly (AGENTS.md).
- Use/extend the shared fakes in `tests/fakes.py`; don't invent new doubles.
- Comments state constraints the code can't (see `db.py:26-27` guard comment
  and `db.py:90-91` reasoning-cap comment — preserve both).
- Conventional commits (e.g. `refactor(db): ...`), commit messages end with
  `Co-Authored-By:` trailer per repo history.

### Library facts (verified against fastapi-pagination 0.15.15 / SQLAlchemy 2.0.51 — current PyPI releases)

- SQLAlchemy's plain `postgresql://` URL scheme selects the psycopg2 driver,
  which is NOT installed. The DSN must be normalized to `postgresql+psycopg://`
  in code (do not edit compose/env/config defaults).
- `sqlalchemy.exc.OperationalError ⊂ DBAPIError ⊂ StatementError ⊂ SQLAlchemyError`;
  wrapped driver exception available as `.orig`; `str()` of a wrapped error
  embeds the driver message (e.g. `invalid input syntax for type integer`)
  plus `[SQL: ...]` and `[parameters: ...]` text.
- Even with `isolation_level="AUTOCOMMIT"`, SQLAlchemy tracks an implicit
  transaction on a `Connection`; after a DBAPI error the connection must be
  `rollback()`-ed or every later statement raises `PendingRollbackError`.
  Raw psycopg autocommit had no such state — this is the migration's biggest
  behavioral trap and gets its own integration test (step 8).

## Commands you will need

| Purpose | Command (from `service/` unless noted) | Expected on success |
|---------|----------------------------------------|---------------------|
| Add dep | `uv add "sqlalchemy==2.0.51"` | exit 0, lock updated |
| Unit tests | `uv run pytest -q` | 115+ passed, 17 deselected |
| Integration | `uv run pytest -m "integration and not llm" -q` (Docker required) | 16+ passed |
| Lint | `uv run ruff check .` (repo root) | only the 2 pre-existing E501s (`tests/integration/conftest.py:275`, `tests/integration/test_sweeper_drain.py:66`) |
| Format | `uv run ruff format --check .` | no changes needed |
| Mutation | `uv run mutmut run` (from `service/`, per AGENTS.md, on touched modules) | review surviving mutants in `app.db` |

## Scope

**In scope** (the only files you should modify):
- `service/pyproject.toml` + `uv.lock` (dependency add only)
- `service/app/models.py` (create)
- `service/app/db.py`
- `service/app/api.py` (internals only — response contract unchanged)
- `service/app/worker.py`, `service/app/retrier.py`, `service/app/sweeper.py`
  (exception clauses + import only)
- `service/tests/fakes.py`, `service/tests/test_db.py`
- `service/tests/test_worker.py`, `service/tests/test_retrier.py`,
  `service/tests/test_sweeper.py` (exception-construction swaps only)
- `service/tests/integration/conftest.py` and the five integration test files
  (fixture + assertion-site mechanics only — no assertion semantics change)

**Out of scope** (do NOT touch):
- `sql/schema.sql`, `docker-compose.yml`, `service/app/config.py`,
  `service/Dockerfile` — no DSN or DDL changes anywhere.
- `service/app/classifier.py`, `service/app/failures.py` — no DB code.
- The API response shape, query params, or cursor format — byte-identical
  behavior; plan 011 owns the contract change.
- README "## Tradeoffs" section (standing rule: human-authored only).
- fastapi-pagination — that is plan 011.

## Git workflow

- This repo commits directly on `main` (maintainer's convention — no feature
  branches unless the operator says otherwise).
- Conventional commits; one commit for the whole plan is acceptable, e.g.
  `refactor(db)!: migrate Postgres access from raw psycopg to SQLAlchemy`
  (use `!` only if you conclude observable behavior changed — it should not).
- Do NOT push.

## Steps

### Step 1: Add the dependency

From `service/`: `uv add "sqlalchemy==2.0.51"`.

**Verify**: `uv run python -c "import sqlalchemy; print(sqlalchemy.__version__)"`
→ `2.0.51`; `uv run pytest -q` → all pass (nothing imports it yet).

### Step 2: Create `service/app/models.py`

`Base(DeclarativeBase)` + `Edit` mirroring `sql/schema.sql` exactly. Module
docstring must state: "Mirrors sql/schema.sql, which remains the DDL source of
truth — this model is never used for create_all; CHECK constraints and indexes
are initdb concerns and deliberately omitted."

```python
from datetime import datetime

from sqlalchemy import Integer, REAL, Text, func
from sqlalchemy.dialects.postgresql import TIMESTAMP
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Edit(Base):
    __tablename__ = "edits"

    id: Mapped[str] = mapped_column(Text, primary_key=True)
    title: Mapped[str | None] = mapped_column(Text)
    editor: Mapped[str | None] = mapped_column(Text)
    comment: Mapped[str | None] = mapped_column(Text)
    byte_delta: Mapped[int | None] = mapped_column(Integer)
    label: Mapped[str | None] = mapped_column(Text)
    confidence: Mapped[float | None] = mapped_column(REAL)
    reasoning: Mapped[str | None] = mapped_column(Text)
    model: Mapped[str | None] = mapped_column(Text)
    status: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="classified"
    )
    event_time: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True))
    processed_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
```

**Verify**: `uv run python -c "from app.models import Edit; print(list(Edit.__table__.columns.keys()))"`
→ the 12 columns in schema order.

### Step 3: Rewrite `service/app/db.py`

Public seam is UNCHANGED: `connect()`, `upsert_edit(conn, edit, result)`,
`upsert_failed_edit(conn, edit, reason, error)`, `fetch_edit_status(conn, edit)`,
`write_with_reconnect(conn, write)`, `read_with_reconnect(conn, read)`.
`conn` is now a `sqlalchemy.engine.Connection`. The param dicts built inside
`upsert_edit` / `upsert_failed_edit` / `fetch_edit_status` are byte-identical
to today (test_db.py pins them). Structure:

```python
def normalize_dsn(dsn: str) -> str:
    """SQLAlchemy's plain postgresql:// scheme means psycopg2; we ship psycopg 3."""
    if dsn.startswith("postgresql://"):
        return dsn.replace("postgresql://", "postgresql+psycopg://", 1)
    return dsn


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

Statement constants — module-level expression objects (identity-assertable,
like the old strings). Build the classified upsert from
`pg_insert(Edit).values(...)` with `bindparam(...)` for every column and
`processed_at=func.now()`, then `.on_conflict_do_update(index_elements=[Edit.id],
set_={label/confidence/reasoning/model/status: excluded.*, processed_at: func.now()})`
→ name it `UPSERT_STMT`. Build `FAILED_UPSERT_STMT` the same way with
`label=null(), confidence=null(), model=null(),
status=literal_column("'failed'")` in both `values` and `set_`, and:

```python
    where=Edit.status.is_distinct_from(literal_column("'classified'")),
```

`literal_column` (not a Python string) is deliberate: the compiled SQL must
contain `IS DISTINCT FROM 'classified'` verbatim so the guard stays
inspectable text (step 4 pins it). Keep the existing guard comment from
`db.py:26-27` above `FAILED_UPSERT_STMT`.

`STATUS_STMT = select(Edit.status).where(Edit.id == bindparam("id"))`.

Single private execute wrapper (all three data functions call it):

```python
def _execute(conn: Connection, statement, params: dict | None = None):
    try:
        return conn.execute(statement, params)
    except sqlalchemy.exc.DBAPIError:
        # AUTOCOMMIT still tracks an implicit transaction; without this
        # rollback the long-lived pipeline connection would raise
        # PendingRollbackError on every statement after a data error.
        conn.rollback()
        raise
```

`connect()` keeps its exact signature/defaults/log message, catching
`sqlalchemy.exc.OperationalError`, returning `get_engine().connect()`.
`write_with_reconnect` / `read_with_reconnect`: same bodies, catch
`sqlalchemy.exc.OperationalError`, and before reconnecting close the broken
connection best-effort (`with suppress(Exception): conn.close()`) so the
pooled connection isn't leaked. `fetch_edit_status` keeps returning
`row[0] if row is not None else None` (SQLAlchemy `Row` supports positional
indexing).

**Verify**: `uv run python -c "from app import db; from sqlalchemy.dialects import postgresql; print(db.FAILED_UPSERT_STMT.compile(dialect=postgresql.dialect()))"`
→ output contains `ON CONFLICT (id) DO UPDATE SET` and
`WHERE edits.status IS DISTINCT FROM 'classified'`.

### Step 4: Update `tests/fakes.py` and `tests/test_db.py`

`FakeConn` keeps its shape (AGENTS.md: extend the shared fakes). Changes:
- `execute()`: identity check becomes `if sql is db.STATUS_STMT:` (the
  STATUS read path returns positional rows exactly as today).
- Add `rollback()` (increment `self.rollbacks`, initialized 0) and no-op
  `close()` — required by `db._execute` and the reconnect close.

`test_db.py`:
- Identity asserts: `sql is db.UPSERT_STMT` / `db.FAILED_UPSERT_STMT`.
  Param-dict asserts unchanged.
- Exception swaps: `psycopg.OperationalError("server closed")` →
  `sqlalchemy.exc.OperationalError("stmt", {}, psycopg.OperationalError("server closed"))`
  (3-arg wrapped form; keep psycopg as the realistic `.orig`; keep the
  `import psycopg`).
- NEW data-pinning tests (mutmut coverage for the statement constants):
  compile each statement with `stmt.compile(dialect=postgresql.dialect())`
  and assert the full SQL text — must pin at minimum: every inserted column
  name, `ON CONFLICT (id) DO UPDATE SET`, each `SET` column, `now()` for
  `processed_at`, and (failed variant) the literal
  `WHERE edits.status IS DISTINCT FROM 'classified'` plus NULLed columns.

**Verify**: `uv run pytest tests/test_db.py -q` → all pass (7 existing +
new compiled-SQL tests).

### Step 5: Swap exception clauses in the pipeline + its unit tests

In `worker.py:91-93`, `retrier.py:133-135`, `sweeper.py:178-180`:
`except psycopg.OperationalError:` → `except sqlalchemy.exc.OperationalError:`
and `except psycopg.Error as error:` → `except sqlalchemy.exc.SQLAlchemyError as error:`.
Replace `import psycopg` with `import sqlalchemy.exc` in all three. Keep every
comment. (`SQLAlchemyError`, not `DBAPIError`: it additionally covers
`StatementError` — client-side bind-adaptation failures are also deterministic
poison — and the ordering is safe because the OperationalError clause
re-raises first.)

Unit tests — swap the injected errors for wrapped forms:
- `test_worker.py:85`, `test_retrier.py:254`, `test_sweeper.py:323`:
  `psycopg.DataError("invalid input for type integer")` →
  `sqlalchemy.exc.DataError("INSERT INTO edits ...", {}, psycopg.DataError("invalid input for type integer"))`
  — `str()` still contains `"invalid input"`, preserving the envelope
  assertions.
- `test_worker.py:108`: replace
  `assert "status IS DISTINCT FROM 'classified'" in sql` with
  `assert sql is db.FAILED_UPSERT_STMT` (stronger identity pin; the guard
  text itself is now pinned by test_db.py's compiled-SQL test).

**Verify**: `uv run pytest -q` → full unit suite passes.
`uv run ruff check .` → only the 2 pre-existing E501s.

### Step 6: Port `service/app/api.py` internals (contract byte-identical)

Replace the psycopg block only. Keep: `VALID_STATUSES` import from
`app.classifier`, label/status 400s, `limit` (1..500, default 50) and
`cursor` params, `encode_cursor`/`decode_cursor` exactly as-is, the
`limit + 1` / `next_cursor` logic, and the `{"items", "next_cursor"}` return.

```python
    stmt = (
        select(Edit)
        .order_by(Edit.processed_at.desc(), Edit.id.desc())
        .limit(limit + 1)
    )
    if label is not None:
        stmt = stmt.where(Edit.label == label)
    if status is not None:
        stmt = stmt.where(Edit.status == status)
    if cursor is not None:
        cursor_processed_at, cursor_id = decode_cursor(cursor)
        stmt = stmt.where(
            tuple_(Edit.processed_at, Edit.id)
            < tuple_(literal(cursor_processed_at), literal(cursor_id))
        )

    with db.get_engine().connect() as conn:
        rows = [dict(row) for row in conn.execute(stmt).mappings()]
```

(`tuple_` compiles to the PG row-value comparison `(processed_at, id) < (...)` —
the same SQL as today. `select(Edit)` selects all mapped columns; `.mappings()`
restores dict rows.)

**Verify**: `uv run python -c "from app import api"` → imports clean;
`uv run pytest -q` still green.

### Step 7: Migrate the integration fixtures and assertion sites

`service/tests/integration/conftest.py` — replace the two psycopg fixtures
(drop the `psycopg` / `dict_row` imports):

```python
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

from app import db as app_db

@pytest.fixture(scope="session")
def postgres_dsn():
    with PostgresContainer("postgres:16", driver=None) as postgres:
        dsn = postgres.get_connection_url()
        engine = create_engine(app_db.normalize_dsn(dsn), isolation_level="AUTOCOMMIT")
        with engine.connect() as conn:
            conn.exec_driver_sql(SCHEMA_SQL.read_text())  # multi-statement DDL
        engine.dispose()
        yield dsn

@pytest.fixture
def pg_conn(postgres_dsn):
    """Real SQLAlchemy connection (AUTOCOMMIT); table truncated per test."""
    engine = create_engine(
        app_db.normalize_dsn(postgres_dsn),
        isolation_level="AUTOCOMMIT",
        poolclass=NullPool,
    )
    with engine.connect() as conn:
        conn.execute(text("TRUNCATE edits"))
        yield conn
    engine.dispose()
```

(Building from the `postgres_dsn` argument — not `settings` — sidesteps
autouse ordering with `wire_settings` and mirrors today's dedicated
connection. `db.upsert_edit(pg_conn, ...)` seeding calls keep working because
`pg_conn` is now a real SQLAlchemy `Connection`.)

Add two helpers to conftest and use them at the assertion sites:

```python
def fetch_edit_row(pg_conn, edit_id: str) -> dict | None:
    row = pg_conn.execute(
        text("SELECT * FROM edits WHERE id = :id"), {"id": edit_id}
    ).mappings().fetchone()
    return dict(row) if row is not None else None

def count_edits(pg_conn) -> int:
    return pg_conn.execute(text("SELECT count(*) FROM edits")).scalar_one()
```

Mechanical rewrite pattern for every remaining raw-SQL site in
`test_api.py`, `test_db_guards.py`, `test_failure_paths.py`,
`test_sweeper_drain.py`, `test_pipeline_e2e.py`:

```
OLD: pg_conn.execute("SELECT ... %s", (X,)).fetchone()      # dict via row_factory
NEW: pg_conn.execute(text("SELECT ... :id"), {"id": X}).mappings().fetchone()
```

`seed_edit` in `test_api.py` becomes
`pg_conn.execute(text("INSERT INTO edits (id, label, status, processed_at) VALUES (:id, :label, :status, :processed_at)"), {...})`.
Assertion SEMANTICS must not change — same columns, same expected values.

**Verify**: `uv run pytest -m "integration and not llm" -q` → all pass,
same count as before the migration. Pay special attention to
`test_failure_paths.py::test_schema_mismatch_parks_to_dlq_with_real_psycopg_error`
— it proves the wrapped `DataError` still routes to the DLQ with
`"invalid input syntax"` in the envelope.

### Step 8: Add the DataError-survival integration test

In `test_db_guards.py`: on ONE `db.connect()` connection, attempt
`db.upsert_edit` with `byte_delta="lots"` (expect
`pytest.raises(sqlalchemy.exc.DBAPIError)`), then on the SAME connection run a
good `db.upsert_edit` and assert the row landed (via `fetch_edit_row`). This
pins `_execute`'s rollback — without it the second write raises
`PendingRollbackError`, a failure mode raw psycopg never had.

**Verify**: `uv run pytest -m "integration and not llm" tests/integration/test_db_guards.py -q` → passes.

### Step 9: Full verification battery

Run every command in "Commands you will need". From `service/`, run mutmut
per AGENTS.md and review survivors in `app.db` — the statement constants are
now expression objects; the compiled-SQL tests from step 4 must kill mutants
in the `values`/`set_`/`where` clauses.

**Verify**: `grep -rn "import psycopg" service/app service/tests` → hits ONLY
in unit-test files that construct wrapped `.orig` exceptions
(`tests/test_db.py`, `tests/test_worker.py`, `tests/test_retrier.py`,
`tests/test_sweeper.py`). Zero hits in `service/app/` and
`tests/integration/`.

## Test plan

- No existing assertion may weaken. New tests: compiled-SQL pins for
  `UPSERT_STMT` / `FAILED_UPSERT_STMT` / `STATUS_STMT` in `test_db.py`
  (model after the existing identity tests there), and the DataError-survival
  integration test in `test_db_guards.py` (model after
  `test_failed_upsert_never_downgrades_a_classified_row`).
- Acceptance gate: the full integration suite green with UNCHANGED assertions
  (only fixture/query mechanics differ).

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `uv run pytest -q` exits 0 (unit)
- [ ] `uv run pytest -m "integration and not llm" -q` exits 0 (Docker)
- [ ] `uv run ruff check .` → only the 2 pre-existing E501s; `ruff format --check .` clean
- [ ] `grep -rn "import psycopg" service/app` → zero hits
- [ ] `grep -rn "psycopg.connect" service` → zero hits
- [ ] `curl` contract unchanged: `GET /edits?limit=2` returns `{"items": [...], "next_cursor": ...}` (compose smoke or TestClient)
- [ ] No files outside the in-scope list modified (`git status`)
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back (do not improvise) if:

- The staged working-tree changes described in "Current state" are still
  uncommitted when you start, or the excerpts don't match the live code.
- `test_schema_mismatch_parks_to_dlq_with_real_psycopg_error` fails on the
  `"invalid input syntax"` assertion after step 7 — do NOT weaken it to
  inspect `.orig`; the envelope string is operator-facing provenance and the
  change needs a recorded decision.
- Binding the ISO-string `event_time` (str into `TIMESTAMP(timezone=True)`)
  fails under the SQLAlchemy psycopg dialect — the fix (fromisoformat in
  `upsert_*`) changes the pinned param dicts in `test_db.py` and must be a
  deliberate, recorded decision, not a patch.
- The `tuple_(...) < tuple_(...)` keyset predicate renders anything other
  than a row-value comparison (the pagination walk tests in `test_api.py`
  catch skips/duplicates).
- The step-8 test still hits `PendingRollbackError` despite `_execute`'s
  rollback — the AUTOCOMMIT/transaction interaction needs a design revisit
  (e.g. per-write `engine.begin()`), not a workaround.
- Any step's verification fails twice after a reasonable fix attempt.

## Maintenance notes

- Plan 011 (fastapi-pagination) builds directly on `models.Edit`,
  `db.get_engine()`, and the `select(Edit)` query built here.
- Reviewer focus: the compiled SQL of `FAILED_UPSERT_STMT` (guard semantics),
  `_execute`'s rollback, and that no integration assertion got weakened
  during the mechanical rewrite.
- Deferred: connection-pool tuning (pool size, timeouts) — defaults are fine
  at this scale; revisit if the API grows concurrent load.
- Plans 003/004 from the earlier audit assumed `psycopg_pool` + a
  `build_query` helper — superseded/reworked per `plans/README.md`.
