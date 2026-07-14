# Plan 004: Add GET /stats — label/status counts for output usability (REWORKED for the 010/011 SQLAlchemy shape)

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 063ee46..HEAD -- service/app/api.py service/tests/integration/test_api.py README.md`
> This plan was reworked against commit `063ee46` (plans 010+011 landed:
> SQLAlchemy engine/models + fastapi-pagination CursorPage). If any in-scope
> file changed since, compare the "Current state" excerpts against the live
> code before proceeding; on a mismatch, STOP.

## Status

- **Priority**: P3
- **Effort**: S
- **Risk**: LOW
- **Depends on**: plans/010 and plans/011 (both DONE — this rework assumes their landed shape)
- **Category**: direction
- **Planned at**: original `2ab6013`; reworked at `063ee46`, 2026-07-14

## Why this matters

The serve layer is a single row-dump endpoint. The take-home's evaluation
rubric scores "Output usability — result is something a person could actually
look at and act on". A tiny aggregate view — how many edits per label, how
many failed — turns the table into an at-a-glance answer ("is vandalism
spiking? is the pipeline healthy?") for one small query. The maintainer
selected this knowing it is mild gold-plating; keep it minimal. It also
pairs with plan 011's deliberate `UseIncludeTotal(False)` decision: totals
live here, not as a COUNT(*) on every page request.

## Current state (verify before editing)

`service/app/api.py` (post-011) is a fastapi-pagination CursorPage endpoint:

- Imports include `from sqlalchemy import select`, `from sqlalchemy.orm import Session`,
  `from app import db`, `from app.models import Edit`,
  `from app.classifier import VALID_LABELS, VALID_STATUSES`.
- A `get_session()` dependency yields `Session(db.get_engine())`.
- `get_edits` validates label/status with 400s, builds
  `select(Edit).order_by(Edit.processed_at.desc(), Edit.id.desc())`, and
  returns `paginate(session, query)` (CursorPage; `total` is null by design).
- `add_pagination(app)` at module bottom.

There is NO unit-level `service/tests/test_api.py` — API coverage lives in
`service/tests/integration/test_api.py` (TestClient + real Postgres via the
`pg_conn` fixture; `seed_edit(pg_conn, edit_id, processed_at, label, status)`
helper inserts rows with `text()` SQL).

`label` is NULL on failed rows (`db.FAILED_UPSERT_STMT` sets label NULL +
status 'failed'), so a label GROUP BY must expect NULL.

KNOWN TOOLING FACT (from plan 011's execution): mutmut generates ZERO mutants
for `@app.get(...)`-decorated route functions. Therefore the aggregation
logic must live in a PLAIN module-level function (`summarize_stats`) that
mutmut can mutate and unit tests can pin without a DB.

## Commands you will need

| Purpose | Command (repo root) | Expected on success |
|---------|---------------------|---------------------|
| Unit tests | `uv run pytest -q` | all pass (baseline 131 passed, 20 deselected) |
| Integration | `uv run pytest -m "integration and not llm" -q` | all pass (baseline 19 passed) |
| Lint | `uv run ruff check .` | zero errors |
| Format | `uv run ruff format --check .` | clean |
| Mutation gate | `uv run --directory service mutmut run && uv run --directory service mutmut results` | no new non-acceptable survivors in `app.api` (i.e. `summarize_stats` mutants all killed) |

## Scope

**In scope**:
- `service/app/api.py` (one stats statement constant, one pure
  `summarize_stats` function, one new route)
- `service/tests/test_api.py` (CREATE — unit tests for `summarize_stats`
  only; no DB, no TestClient needed)
- `service/tests/integration/test_api.py` (new test functions only; do not
  modify existing ones)
- `README.md` ("Query results" block: one curl line + one-line response
  description — NOT "## Tradeoffs")

**Out of scope**:
- New schema objects, materialized views, caching.
- Any change to `GET /edits`, `EditsPage`, `get_session`, or pagination.
- HTML/UI of any kind.
- Parameters, time-bucketing, percentiles.

## Git workflow

- Commit directly on the worktree's current branch. Conventional style:
  `feat(api): add GET /stats label/status counts`.

## Steps

### Step 1: Add the statement, the pure aggregator, and the route

In `service/app/api.py` (near the other module-level definitions):

```python
STATS_STMT = select(Edit.label, Edit.status, func.count()).group_by(
    Edit.label, Edit.status
)


def summarize_stats(rows) -> dict:
    """Fold (label|None, status, count) rows into the /stats response.

    label is NULL on failed rows, so NULL labels count toward total and
    by_status but never appear in by_label. Zero-count keys are absent,
    not zero-filled.
    """
    total = 0
    by_label: dict[str, int] = {}
    by_status: dict[str, int] = {}
    for label, status, count in rows:
        total += count
        if label is not None:
            by_label[label] = by_label.get(label, 0) + count
        by_status[status] = by_status.get(status, 0) + count
    return {"total": total, "by_label": by_label, "by_status": by_status}


@app.get("/stats")
def get_stats(session: Session = Depends(get_session)) -> dict:
    return summarize_stats(session.execute(STATS_STMT).all())
```

Add `func` to the existing `from sqlalchemy import select` import line.
Mention `/stats` in the module docstring's first line alongside `/edits`.

**Verify**: `uv run python -c "from app import api; print(api.summarize_stats([('trivia','classified',2),(None,'failed',1)]))"`
→ `{'total': 3, 'by_label': {'trivia': 2}, 'by_status': {'classified': 2, 'failed': 1}}`

### Step 2: Unit tests (CREATE `service/tests/test_api.py`)

Pure-function tests on `summarize_stats` (house style: pin data, model on
`service/tests/test_db.py`'s directness; descriptive test names):

1. Empty input → `{"total": 0, "by_label": {}, "by_status": {}}`.
2. Mixed labels/statuses → exact dict equality on the full response
   (multiple labels, both statuses, counts summed into total).
3. NULL-label rows count in `total` and `by_status` but not `by_label`.
4. Same label under two statuses aggregates (`by_label` sums across rows).

Also pin the statement's compiled SQL (kills GROUP BY / column mutants;
model on test_db.py's compiled-SQL tests):

```python
def test_stats_statement_groups_label_and_status():
    from sqlalchemy.dialects import postgresql

    sql = str(api.STATS_STMT.compile(dialect=postgresql.dialect()))
    assert "count(*)" in sql
    assert "GROUP BY edits.label, edits.status" in sql
```

**Verify**: `uv run pytest service/tests/test_api.py -q` → all pass.

### Step 3: Integration test + README

In `service/tests/integration/test_api.py`, add (do not modify existing
tests; reuse `seed_edit` and `BASE_TIME`):

```python
def test_stats_counts_labels_and_statuses(pg_conn):
    seed_edit(pg_conn, "s1", BASE_TIME, label="trivia", status="classified")
    seed_edit(pg_conn, "s2", BASE_TIME, label="vandalism", status="classified")
    seed_edit(pg_conn, "s3", BASE_TIME, label=None, status="failed")

    client = TestClient(api.app)
    body = client.get("/stats").json()

    assert body == {
        "total": 3,
        "by_label": {"trivia": 1, "vandalism": 1},
        "by_status": {"classified": 2, "failed": 1},
    }


def test_stats_empty_table(pg_conn):
    client = TestClient(api.app)
    assert client.get("/stats").json() == {
        "total": 0,
        "by_label": {},
        "by_status": {},
    }
```

NOTE: check `seed_edit`'s signature first — if it doesn't accept
`label=None`, extend the INSERT params call site as needed WITHOUT changing
existing callers' behavior.

README "Query results" block (after the existing /edits curls, before the
closing fence or as an adjacent line — match the surrounding style):

```sh
curl "http://localhost:8000/stats"
# {"total": ..., "by_label": {...}, "by_status": {...}} — label/status counts
```

Do NOT touch "## Tradeoffs".

**Verify**: `uv run pytest -m "integration and not llm" -q` → all pass
(baseline 19 + 2 new = 21).

### Step 4: Full battery

All commands in "Commands you will need". Mutation gate: every mutant in
`summarize_stats` and `STATS_STMT` must be killed (the += arithmetic, the
`is not None` guard, the dict-get defaults, the GROUP BY columns).

## Test plan

Steps 2-3. Cases: empty table, mixed labels, NULL-label handling,
cross-status label aggregation, compiled-SQL pin.

## Done criteria

- [ ] `uv run pytest -q` exits 0 with the new unit tests
- [ ] `uv run pytest -m "integration and not llm" -q` exits 0 (21 passed)
- [ ] `uv run ruff check .` zero errors; `ruff format --check .` clean
- [ ] README has the /stats curl line; "## Tradeoffs" untouched
- [ ] mutmut: no new non-acceptable survivors in `app.api`
- [ ] `git diff --stat` touches only in-scope files
- [ ] `plans/README.md` status row updated

## STOP conditions

- The drift check shows in-scope files changed since `063ee46` and the
  Current-state excerpts no longer match.
- You find yourself adding parameters, time-bucketing, or percentiles —
  beyond the selected scope; report instead.
- Existing integration tests need edits to stay green.

## Maintenance notes

- If the table grows large enough that GROUP BY over all rows is slow,
  revisit with a summary table then, not now.
- Response shape is additive only; nothing existing changed.
- The aggregation deliberately lives in `summarize_stats` (not the route
  body) because mutmut cannot mutate decorator-wrapped route handlers.
