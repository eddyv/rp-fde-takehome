# Plan 011: Switch GET /edits to fastapi-pagination cursor pagination

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat c5416e5..HEAD -- service/app/api.py service/tests/integration/test_api.py README.md`
> Plan 010 MUST be DONE first (check its row in `plans/README.md`); this
> plan's "Current state" describes api.py AS PLAN 010 LEAVES IT. If plan 010
> is not DONE, or api.py still imports psycopg, STOP.

## Status

- **Priority**: P2
- **Effort**: M
- **Risk**: MED
- **Depends on**: plans/010-sqlalchemy-migration.md
- **Category**: migration
- **Planned at**: commit `c5416e5`, 2026-07-14

## Why this matters

The API's cursor pagination is hand-rolled (~30 lines: base64-JSON cursor
encode/decode, limit+1 fetch, keyset predicate). It works, but it's bespoke
code the maintainer must own as filters and sort orders grow. fastapi-pagination
(backed by sqlakeyset) is the maintained implementation of exactly this
technique: it owns cursor encoding, keyset predicates, bidirectional paging,
and OpenAPI documentation of the page schema. The maintainer decided to adopt
the library's DEFAULT response shape — a deliberate breaking change to the
`/edits` contract (the second in the repo's history; the first introduced the
envelope).

## Current state (after plan 010)

- `service/app/api.py` — `GET /edits` on SQLAlchemy: `select(Edit)` ordered by
  `(processed_at DESC, id DESC)`, hand-rolled `encode_cursor`/`decode_cursor`
  (base64 of `{"p": <iso timestamp>, "i": <id>}`), params `label`, `status`,
  `limit` (1..500 default 50), `cursor`; returns
  `{"items": [...], "next_cursor": "<opaque>" | null}`; malformed cursor →
  400 `"invalid cursor"`. `VALID_STATUSES` is imported from `app.classifier`
  (staged change at planning time); label/status get manual 400s listing the
  valid values.
- `service/app/models.py` — `Edit` ORM model (from plan 010).
- `service/app/db.py` — `get_engine()` (AUTOCOMMIT, pool_pre_ping),
  `normalize_dsn()` (from plan 010).
- `service/tests/integration/test_api.py` — 6 tests: filters/validation,
  full pagination walk (`walk_pages` helper follows `next_cursor` with
  `limit`), tie-break on id, filter+pagination, 3 invalid-cursor 400s,
  exact-limit last page. Seeds via a `seed_edit` INSERT helper (plan 010
  ported it to `text()`), uses the `pg_conn` fixture (TRUNCATE per test) and
  `TestClient(api.app)`.
- `README.md` — "Query results" section (~lines 49-62) documents the
  `{"items", "next_cursor"}` envelope, `?cursor=` follow-up curl, and a
  design note about the composite keyset vs UUIDv7; Layout section
  (~line 124) says `GET /edits?label=&status=&limit=&cursor=`.
- The composite index `edits_processed_at_id_idx (processed_at DESC, id DESC)`
  exists in `sql/schema.sql` — sqlakeyset's keyset scan uses it unchanged.

### Library facts (verified against fastapi-pagination 0.15.15 source; re-verify on install)

- Install extra: `fastapi-pagination[sqlalchemy]` pulls `sqlakeyset`.
  Requires `fastapi>=0.93` (repo has 0.139) and pydantic v2. Python 3.13 OK.
- `fastapi_pagination.cursor.CursorPage` fields: `items`, `total`,
  `current_page`, `current_page_backwards`, `previous_page`, `next_page`.
  `CursorRawParams.include_total` defaults to **True** → a `SELECT count(*)`
  per request. Disable with `UseIncludeTotal(False)` — `total` then
  serializes as `null`.
- `CursorParams.size` defaults to `Query(50, ge=0, le=100)` — must be
  overridden to `ge=1, le=500` via `UseParamsFields`.
- Invalid-cursor behavior is split: a cursor that fails base64 decoding
  raises `HTTPException(400, "Invalid cursor value")` inside
  `CursorParams.to_raw_params()`; a cursor that decodes but is garbage to
  sqlakeyset (including every old-format `{"p":..., "i":...}` cursor) raises
  `sqlakeyset.InvalidPage` → **500 unless the endpoint catches it**.
- `paginate(session, query)` must receive an ORM `Session` (not a
  `Connection`) so items come back as `Edit` instances for
  `from_attributes` validation.

## Commands you will need

| Purpose | Command (from `service/`) | Expected on success |
|---------|---------------------------|---------------------|
| Add dep | `uv add "fastapi-pagination[sqlalchemy]==0.15.15"` | exit 0 |
| Import check | `uv run python -c "from fastapi_pagination.ext.sqlalchemy import paginate; from sqlakeyset import InvalidPage"` | no output, exit 0 |
| Unit tests | `uv run pytest -q` | all pass |
| Integration | `uv run pytest -m "integration and not llm" tests/integration/test_api.py -q` (Docker) | all pass |
| Full integration | `uv run pytest -m "integration and not llm" -q` | all pass |
| Lint | `uv run ruff check .` (repo root) | only the 2 pre-existing E501s |

## Scope

**In scope**:
- `service/pyproject.toml` + `uv.lock` (dependency add)
- `service/app/api.py`
- `service/tests/integration/test_api.py`
- `README.md` (Query-results section, Layout line — NOT "## Tradeoffs")
- `plans/README.md` (status row)

**Out of scope** (do NOT touch):
- `service/app/db.py`, `service/app/models.py`, worker/retrier/sweeper —
  plan 010 owns them and they are done.
- `sql/schema.sql`, `docker-compose.yml`, `service/app/config.py`.
- All other integration tests (`test_db_guards.py`, `test_failure_paths.py`,
  `test_sweeper_drain.py`, `test_pipeline_e2e.py`) — they don't touch the API.
- README "## Tradeoffs" (standing rule: human-authored only).

## Git workflow

- Commit directly on `main` (repo convention). Conventional commit, breaking:
  `feat(api)!: adopt fastapi-pagination cursor pages on GET /edits` with a
  `BREAKING CHANGE:` footer describing the response-shape and param changes.
- Do NOT push.

## Steps

### Step 1: Add the dependency

From `service/`: `uv add "fastapi-pagination[sqlalchemy]==0.15.15"`.

**Verify**: the import-check command above → exit 0.

### Step 2: Rewrite the endpoint in `service/app/api.py`

Delete: `encode_cursor`, `decode_cursor`, the `limit`/`cursor` params, the
`limit + 1` logic, and the `base64`/`binascii`/`json` imports. Keep the
label/status validation 400s exactly. New shape:

```python
from datetime import datetime

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi_pagination import add_pagination
from fastapi_pagination.cursor import CursorPage
from fastapi_pagination.customization import (
    CustomizedPage,
    UseIncludeTotal,
    UseParamsFields,
)
from fastapi_pagination.ext.sqlalchemy import paginate
from pydantic import BaseModel, ConfigDict
from sqlakeyset import InvalidPage
from sqlalchemy import select
from sqlalchemy.orm import Session

from app import db
from app.classifier import VALID_LABELS, VALID_STATUSES
from app.models import Edit

app = FastAPI(title="wiki-edits")


class EditOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    title: str | None
    editor: str | None
    comment: str | None
    byte_delta: int | None
    label: str | None
    confidence: float | None
    reasoning: str | None
    model: str | None
    status: str
    event_time: datetime | None
    processed_at: datetime


EditsPage = CustomizedPage[
    CursorPage[EditOut],
    UseParamsFields(size=Query(50, ge=1, le=500)),
    UseIncludeTotal(False),  # no COUNT(*) per page
]


def get_session():
    with Session(db.get_engine()) as session:
        yield session


@app.get("/edits")
def get_edits(
    label: str | None = Query(default=None),
    status: str | None = Query(default=None),
    session: Session = Depends(get_session),
) -> EditsPage:
    # (keep the two existing HTTPException(400, ...) validations here)
    query = select(Edit).order_by(Edit.processed_at.desc(), Edit.id.desc())
    if label is not None:
        query = query.where(Edit.label == label)
    if status is not None:
        query = query.where(Edit.status == status)
    try:
        return paginate(session, query)
    except InvalidPage:
        raise HTTPException(status_code=400, detail="invalid cursor") from None


add_pagination(app)
```

Update the module docstring: response is now a fastapi-pagination CursorPage
(`items` + `next_page`/`previous_page` cursors), params `size`/`cursor`.
NOTE: the exact `CustomizedPage[CursorPage[EditOut], ...]` generic spelling
may need to be `CustomizedPage[CursorPage, ...]` then `EditsPage[EditOut]` as
the return annotation — use whichever the installed version accepts (the
import-check + OpenAPI verification below proves it).

**Verify**:
`uv run python -c "from fastapi.testclient import TestClient; from app import api; print(sorted(p['name'] for p in TestClient(api.app).get('/openapi.json').json()['paths']['/edits']['get']['parameters']))"`
→ `['cursor', 'label', 'size', 'status']`, and the `size` param shows
default 50 with 1..500 bounds in the schema.

### Step 3: Rewrite `service/tests/integration/test_api.py`

Keep `seed_edit` and the fixtures. Changes:

- `walk_pages(client, params)` follows `body["next_page"]`, passing
  `{"size": N, "cursor": ...}`; stops when `next_page` is `None` AND `items`
  is empty or when `next_page` is `None` (verify at runtime which the library
  produces on the exact-boundary page — see STOP conditions).
- Existing shape assertions: `.json()["items"]` stays; `next_cursor`
  assertions become `next_page`.
- NEW `test_response_shape`: seed one row, assert
  `set(body.keys()) == {"items", "total", "current_page", "current_page_backwards", "previous_page", "next_page"}`
  and `body["total"] is None`.
- Invalid-cursor test covers three cases, all asserting `status_code == 400`:
  1. `cursor="not-base64!!!"` → detail `"Invalid cursor value"` (library),
  2. a base64-of-garbage cursor → detail `"invalid cursor"` (our catch),
  3. an OLD-format cursor
     (`base64.urlsafe_b64encode(json.dumps({"p": "2026-07-01T00:00:00+00:00", "i": "1"}).encode())`)
     → 400 (pins that pre-migration cursors fail closed, not 500).
- `limit` param is gone: anywhere tests used `{"limit": N}` becomes
  `{"size": N}`. Add one assertion that `size=501` → 422 (bounds enforced).
- Keep: pagination walk across 3 pages, tie-break on id, filter+pagination,
  exact-size last page (`next_page` must be falsy — None — with no extra
  empty page fetch needed), label/status 400s.

**Verify**: `uv run pytest -m "integration and not llm" tests/integration/test_api.py -q`
→ all pass.

### Step 4: Update README.md

- "Query results" section: replace the envelope example with the CursorPage
  shape (`items`, `next_page`, `previous_page`, `total: null`), curl examples
  using `?size=` and `?cursor=<next_page>`; keep the composite-keyset /
  UUIDv7 design note but reword its first sentence to say pagination is
  provided by fastapi-pagination (sqlakeyset) over the same
  `(processed_at, id)` keyset and index.
- Layout section: `GET /edits?label=&status=&limit=&cursor=` →
  `GET /edits?label=&status=&size=&cursor=` (fastapi-pagination CursorPage).
- Do NOT touch "## Tradeoffs".

**Verify**: `grep -n "next_cursor" README.md` → zero hits.

### Step 5: Full battery + index

All commands in "Commands you will need". From `service/`: mutmut on
`app.api` per AGENTS.md. Update this plan's row in `plans/README.md`.

## Test plan

- Rewritten `test_api.py` (model each test on its current counterpart): shape
  test, 3-page walk, id tie-break, filter+pagination, 3 invalid-cursor 400s
  (incl. old-format), exact-size last page, size bounds 422, label/status 400s.
- Everything else in the integration suite must pass UNTOUCHED — if any
  non-API integration test needs editing, that's a scope violation (STOP).

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `uv run pytest -q` exits 0
- [ ] `uv run pytest -m "integration and not llm" -q` exits 0
- [ ] `uv run ruff check .` → only the 2 pre-existing E501s
- [ ] OpenAPI params for `/edits` are exactly `cursor, label, size, status`
- [ ] `grep -rn "encode_cursor\|decode_cursor\|next_cursor" service/app` → zero hits
- [ ] Old-format cursor returns 400, not 500 (test exists and passes)
- [ ] README has no `next_cursor` references; "## Tradeoffs" untouched (`git diff README.md`)
- [ ] No files outside the in-scope list modified (`git status`)
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back (do not improvise) if:

- Plan 010's row is not DONE, or `grep -n "psycopg" service/app/api.py`
  returns hits.
- `fastapi-pagination==0.15.15` fails to resolve against `fastapi==0.139.0`
  / pydantic in the lock — report the resolver output; do not loosen other
  pins to force it.
- The installed version's `CursorPage` fields differ from the list in
  "Library facts" (run the shape test first) — the response contract decision
  was made against these exact fields.
- `paginate(session, query)` returns items that fail `EditOut` validation —
  check the library's `unwrap_mode` before changing the query or model.
- On the exact-size last page, the library returns a non-None `next_page`
  that yields an empty page — that's a real behavioral difference from the
  hand-rolled limit+1 approach; report it (with the observed JSON) so the
  maintainer can accept or reject the trailing-empty-page behavior. Do not
  silently adjust the test to accept it.
- Any step's verification fails twice after a reasonable fix attempt.

## Maintenance notes

- The response now includes `previous_page` (bidirectional paging) — free
  functionality, but clients keying on exact response keys must be told.
- `UseIncludeTotal(False)` is deliberate: enabling totals adds a `count(*)`
  per request. If a total is ever needed, prefer a separate `/stats` endpoint
  (see plan 004's rework note in `plans/README.md`).
- Reviewer focus: the `InvalidPage` catch (the only thing standing between a
  garbage cursor and a 500), the size bounds, and that the keyset ordering
  `(processed_at DESC, id DESC)` survived the rewrite.
- Deferred: old-cursor grace period (accept both formats temporarily) —
  rejected as not worth it; cursors are opaque, short-lived tokens here.
