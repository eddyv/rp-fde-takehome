# Plan 003: Give api.py fast unit coverage (closing the mutation blindspot) and harden its DB access

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 2ab6013..HEAD -- service/app/api.py service/pyproject.toml sql/schema.sql service/tests/`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: M
- **Risk**: LOW
- **Depends on**: none
- **Category**: tests (+ perf)
- **Planned at**: commit `2ab6013`, 2026-07-14

## Why this matters

`service/app/api.py` — the service's entire read/serve surface — has zero
fast coverage. Its only test is Docker-gated
(`service/tests/integration/test_api.py`, marked `integration`), and both the
default test run (`pyproject.toml:31` → `addopts = "-m 'not integration'"`)
and mutmut's test selection (`service/pyproject.toml:35` →
`pytest_add_cli_args_test_selection = ["tests"]`, which inherits the same
deselection) never import it. Every `api.py` mutant survives, silently
undermining the repo's mutation-testing discipline (AGENTS.md). While adding
the tests, three small hardening items in the same file: a new Postgres
connection is opened per request, `SELECT *` couples the response to schema
order, and `ORDER BY processed_at DESC` has no supporting index.

## Current state

`service/app/api.py` in full (46 lines):

```python
@app.get("/edits")
def get_edits(
    label: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=500),
) -> list[dict]:
    if label is not None and label not in VALID_LABELS:
        raise HTTPException(status_code=400, detail=f"label must be one of {sorted(VALID_LABELS)}")
    if status is not None and status not in VALID_STATUSES:
        raise HTTPException(status_code=400, detail=f"status must be one of {sorted(VALID_STATUSES)}")

    conditions: list[str] = []
    params: list = []
    if label is not None:
        conditions.append("label = %s")
        params.append(label)
    if status is not None:
        conditions.append("status = %s")
        params.append(status)

    query = "SELECT * FROM edits"
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY processed_at DESC LIMIT %s"
    params.append(limit)

    with psycopg.connect(settings.postgres_dsn, row_factory=dict_row) as conn:
        return conn.execute(query, params).fetchall()
```

- `sql/schema.sql:17-18` indexes only `label` and `status`; nothing supports
  the `ORDER BY processed_at DESC` top-N read. Columns (all 12, in order):
  `id, title, editor, comment, byte_delta, label, confidence, reasoning,
  model, status, event_time, processed_at`.
- `service/pyproject.toml:6-13` pins `psycopg[binary]==3.3.4`. The official
  pool lives in the `pool` extra (`psycopg[binary,pool]`).
- The worker/retrier reuse one long-lived autocommit connection
  (`service/app/db.py:46-56`) — connection-per-request in the API is the
  inconsistency.
- Existing integration test (`service/tests/integration/test_api.py:28-48`)
  covers filter-by-label/status, 400 paths, against real Postgres via
  `TestClient(api.app)`. Keep it passing unchanged.
- House test style: data-pinning with shared fakes — see
  `service/tests/test_db.py:20-39` (`test_upsert_edit_maps_every_column`
  asserts the full params dict and `sql is db.UPSERT_SQL`) and
  `service/tests/fakes.py` (`FakeConn` records `(sql, params)` tuples).
- AGENTS.md conventions that bind this plan: descriptive variable names;
  every change needs tests; run mutmut on touched modules; "tests must pin
  down data, not just routing".

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Sync after dep change | `uv sync` | exit 0, lockfile updated |
| Tests | `uv run pytest` | all pass (115 existing + your new ones) |
| Integration (needs Docker) | `uv run pytest -m "integration and not llm"` | all pass |
| Lint | `uv run ruff check .` | only the 2 pre-existing E501s in `service/tests/integration/` |
| Mutation gate | `uv run --directory service mutmut run && uv run --directory service mutmut results` | no surviving mutants in `app.api` (see AGENTS.md for acceptable-survivor classes) |

## Scope

**In scope** (the only files you should modify):
- `service/app/api.py`
- `service/pyproject.toml` (dependency line only) + `uv.lock` (via `uv sync`)
- `sql/schema.sql` (one index)
- `service/tests/test_api.py` (create)
- `README.md` (one line in the runbook "Schema changes" bullet — see Step 3)

**Out of scope** (do NOT touch):
- `service/tests/integration/test_api.py` — it must keep passing unchanged;
  it is the proof the refactor preserved behavior.
- `service/app/db.py` — the worker-side connection helpers are separate.
- Response shape: the endpoint must keep returning the same 12 keys per row.
  Clients (and the take-home evaluators' curl commands) depend on it.
- Authentication — deliberately rejected for this local stack (see
  `plans/README.md`).

## Git workflow

- Commit directly on `main` (repo convention). Conventional style, e.g.
  `test(api): unit-cover query building; pool connections; index processed_at`.
- Never commit `.env`.

## Steps

### Step 1: Extract a pure query builder

In `service/app/api.py`, extract the SQL construction into a module-level
function so it is unit-testable and mutmut-visible without a DB:

```python
COLUMNS = ("id, title, editor, comment, byte_delta, label, confidence, "
           "reasoning, model, status, event_time, processed_at")

def build_query(label: str | None, status: str | None, limit: int) -> tuple[str, list]:
    ...  # same logic as today, but SELECT <COLUMNS> instead of SELECT *
```

`get_edits` keeps the validation/HTTPException logic and calls
`build_query`. Behavior must be identical (same WHERE combinations, same
`ORDER BY processed_at DESC LIMIT %s`, params in the same order).

**Verify**: `uv run pytest` → existing 115 still pass.

### Step 2: Replace connection-per-request with a lifespan-managed pool

- Change `service/pyproject.toml` dependency `psycopg[binary]==3.3.4` →
  `psycopg[binary,pool]==3.3.4`, then `uv sync`.
- In `api.py`, create a `psycopg_pool.ConnectionPool` in a FastAPI lifespan
  handler (opened on startup, closed on shutdown), configured with
  `kwargs={"row_factory": dict_row}` and the DSN from settings. The request
  handler acquires with `with pool.connection() as conn:`.
- Store the pool on `app.state` so tests can substitute a fake.

**Verify**: `uv run pytest` passes; if Docker is available,
`uv run pytest -m "integration and not llm" -k api` passes unchanged
(TestClient triggers lifespan, so the pool opens against the container).

### Step 3: Add the sort index

Append to `sql/schema.sql`:

```sql
CREATE INDEX IF NOT EXISTS edits_processed_at_idx ON edits (processed_at DESC);
```

The schema only runs on a fresh Postgres volume; extend the existing README
runbook bullet ("Schema changes…", `README.md:180-182`) so the
`docker compose down -v` guidance explicitly covers picking up new indexes.

**Verify**: `grep -c 'processed_at_idx' sql/schema.sql` → 1.

### Step 4: Write the unit tests

Create `service/tests/test_api.py` (no `integration` marker — this is the
point). Model the data-pinning style on `service/tests/test_db.py`. Cover:

1. `build_query` output for all four filter combinations (none / label /
   status / both): assert the exact SQL string and the exact params list —
   including explicit columns (no `SELECT *`) and `LIMIT` param last.
2. 400 paths via `TestClient(api.app)` with the pool substituted by a fake on
   `app.state` (a small stub whose `.connection()` context manager yields a
   `FakeConn`-like object; extend `tests/fakes.py` only if a seam is missing,
   per AGENTS.md).
3. Limit bounds: `limit=0` and `limit=501` → 422 (FastAPI `ge`/`le`),
   default limit 50 lands in params when omitted.
4. Happy path through the route with the fake pool: response JSON equals the
   fake's rows, and the executed `(sql, params)` matches `build_query`'s
   output for the same inputs.

**Verify**: `uv run pytest service/tests/test_api.py -v` → all new tests pass.

### Step 5: Mutation gate

Run `uv run --directory service mutmut run`, then
`uv run --directory service mutmut results`. Delete `service/mutants/` first
if results look stale (AGENTS.md). No surviving mutants in `app.api` other
than AGENTS.md's acceptable classes (log strings, equivalent mutants).

**Verify**: `uv run --directory service mutmut results` → `app.api` mutants all killed/acceptable.

## Test plan

Covered by Step 4. Pattern files: `service/tests/test_db.py` (data pinning),
`service/tests/fakes.py` (FakeConn `(sql, params)` recording),
`service/tests/integration/test_api.py` (unchanged behavioral reference).

## Done criteria

- [ ] `uv run pytest` exits 0; `service/tests/test_api.py` exists with ≥ 8 tests
- [ ] `grep -n 'SELECT \*' service/app/api.py` → no matches
- [ ] `grep -n 'psycopg.connect' service/app/api.py` → no matches (pool only)
- [ ] `sql/schema.sql` contains `edits_processed_at_idx`
- [ ] Integration api test unchanged (`git diff service/tests/integration/test_api.py` empty) and passing if Docker available
- [ ] mutmut: no non-acceptable survivors in `app.api`
- [ ] `git diff --stat` touches only the in-scope files
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back if:

- `psycopg[binary,pool]==3.3.4`'s pool extra fails to resolve with `uv sync`
  (version mismatch between psycopg and psycopg-pool) — report the resolver
  error rather than loosening version pins.
- The integration api test fails after the pool change (lifespan not running
  under TestClient, or pool blocking on a missing DB at import time — the
  pool must NOT connect at import, only via lifespan).
- Preserving the exact 12-key response shape proves impossible.
- mutmut surfaces survivors that require weakening an existing test to kill.

## Maintenance notes

- Plan 004 (`/stats`) builds on the pool and this file's structure — land
  this first.
- Plan 005 wires `ty` and expects `api.py`'s final shape; the query built
  from string literals can be typed `LiteralString` to satisfy psycopg's
  typed `execute` (see that plan).
- Reviewers: check the pool is opened lazily (lifespan), sized modestly
  (defaults fine), and that no request path can hold a connection across a
  slow operation.
