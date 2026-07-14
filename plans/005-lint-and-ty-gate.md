# Plan 005: Clear the two lint errors and wire `ty` into the documented dev loop

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 2ab6013..HEAD -- service/app service/tests README.md pyproject.toml`
> Plans 003/004 intentionally change `service/app/api.py` first; re-run
> `uv run ty check service/app` yourself and work from the live diagnostic
> list, not this plan's snapshot, if they have landed.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: plans/003 and 004 (soft — see drift note; only because they reshape `api.py`)
- **Category**: dx
- **Planned at**: commit `2ab6013`, 2026-07-14

## Why this matters

`ty` (Astral's type checker) sits in the dev dependency group
(`pyproject.toml:23`) but is referenced nowhere — no README command, no
config. A checker nobody runs provides zero signal and reads as an abandoned
tool in a code walkthrough. Separately, `uv run ruff check .` currently fails
with 2 errors, so the documented lint command doesn't pass on a clean
checkout. The maintainer chose to wire `ty` scoped to the `app` package (the
test tier's ~44 diagnostics all stem from fakes passed where SDK types are
expected — checking that tier is not worth the annotation churn) and fix the
lint errors. No CI workflow — that was explicitly declined; these stay local
gates.

## Current state

- `uv run ruff check .` → exactly 2 errors, both `E501 Line too long (89 > 88)`:
  - `service/tests/integration/conftest.py:275` —
    `def read_records(topic: str, expected_count: int, timeout: float = POLL_TIMEOUT_SECONDS):`
  - `service/tests/integration/test_sweeper_drain.py:66` —
    `def test_sweeper_reclassifies_requeues_skips_and_stops_at_boundary(pg_conn, monkeypatch):`
- `uv run ty check service/app` → 4 diagnostics at commit `2ab6013`:
  1. `service/app/api.py:45` — `row_factory=dict_row` vs
     `RowFactory[tuple[Any, ...]]` (psycopg generic-connection typing).
     Likely GONE after plan 003 (pool with `kwargs={"row_factory": dict_row}`).
  2. `service/app/api.py:46` — `conn.execute(query, ...)` expects
     `LiteralString | bytes | SQL | Composed`, got `str` (psycopg's
     injection-safety typing).
  3. `service/app/classifier.py:116-118` — `output_config={...}` literal dict
     vs the SDK's `OutputConfigParam` TypedDict.
  4. Same call — the nested `schema` value vs `Dict[str, object]` on
     `JSONOutputFormatParam`.
- `uv run pytest` → 115 passed (more after plans 003/004).
- Repo lint config: `pyproject.toml:26-27` → ruff `select = ["I", "F", "E"]`,
  default line length 88.
- README dev commands live at `README.md:62-90` (tests, integration, lint,
  mutation testing).

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Lint | `uv run ruff check .` | exit 0, no errors |
| Format check | `uv run ruff format --check .` | exit 0 (run before committing) |
| Types | `uv run ty check service/app` | exit 0, 0 diagnostics |
| Tests | `uv run pytest` | all pass |

## Scope

**In scope**:
- `service/tests/integration/conftest.py` (line 275 wrap only)
- `service/tests/integration/test_sweeper_drain.py` (line 66 wrap only)
- `service/app/api.py` (type annotations only — no behavior)
- `service/app/classifier.py` (type annotations only — no behavior)
- `README.md` (add the `ty` command to the Lint/format block)
- `pyproject.toml` (optional `[tool.ty]` block ONLY if needed to scope paths)

**Out of scope**:
- Annotating the test tier / fakes to satisfy `ty` — explicitly declined.
- Any CI workflow — explicitly declined by the maintainer.
- Any behavioral change; this plan must be a no-op at runtime.
- Adding `ty` to pre-commit hooks (none exist; don't introduce them).

## Git workflow

- Commit directly on `main` (repo convention). Conventional style, e.g.
  `chore: fix E501s and wire ty check for the app package`.

## Steps

### Step 1: Fix the two E501s

Wrap the parameter lists (do not rename anything):

```python
def read_records(
    topic: str, expected_count: int, timeout: float = POLL_TIMEOUT_SECONDS
):
```

```python
def test_sweeper_reclassifies_requeues_skips_and_stops_at_boundary(
    pg_conn, monkeypatch
):
```

**Verify**: `uv run ruff check .` → exit 0. `uv run ruff format --check .` → exit 0.

### Step 2: Clear the `app/` type diagnostics

Run `uv run ty check service/app` and address what remains (list above may
have shifted after plans 003/004). Preferred fixes, in order:

1. **`api.py` query typing**: the query is concatenated purely from string
   literals, and Python type checkers propagate `LiteralString` through
   literal concatenation/joins. Annotate:

   ```python
   from typing import LiteralString
   conditions: list[LiteralString] = []
   query: LiteralString = "SELECT ..."
   ```

   This satisfies psycopg's typed `execute` *and* is a real guard: if anyone
   later interpolates request data into `query`, the type checker will flag
   it. Do NOT cast — the point is the constraint.
2. **`classifier.py` output_config**: annotate `OUTPUT_SCHEMA` as
   `dict[str, object]` (matches the SDK's `JSONOutputFormatParam.schema`
   declared type). If the `OutputConfigParam` mismatch persists, construct
   the param with the SDK's own TypedDicts
   (`from anthropic.types import ...` — check what `anthropic==0.116.0`
   exports) rather than a bare dict.
3. **Last resort only**: a targeted `# ty: ignore[<rule>]` with a one-line
   reason comment, only where the diagnostic is a genuine false positive and
   1-2 are not achievable without behavior risk. Zero blanket ignores.

The runtime request payload must remain byte-identical —
`service/tests/test_classifier.py:85-96`
(`test_request_shape_is_single_user_message_with_bounded_tokens`) pins the
exact kwargs and must pass untouched.

**Verify**: `uv run ty check service/app` → 0 diagnostics; `uv run pytest` → all pass.

### Step 3: Document the gate

In README's lint/format block (`README.md:77-82`), add:

```sh
uv run ty check service/app
```

with a half-line note that the check is scoped to the app package (test
doubles are deliberately untyped).

**Verify**: `grep -n 'ty check' README.md` → 1 match.

## Test plan

No new tests: annotations and line wraps only. The full suite plus the
pinned-request-shape test are the regression net. If any step changes a
runtime value (not just an annotation), that step is out of bounds — STOP.

## Done criteria

- [ ] `uv run ruff check .` exits 0
- [ ] `uv run ruff format --check .` exits 0
- [ ] `uv run ty check service/app` exits 0 with 0 diagnostics
- [ ] `uv run pytest` all pass, zero test-file assertion changes
- [ ] `grep -rn 'ty: ignore' service/app/` → at most 2 hits, each with a reason comment
- [ ] README documents the ty command
- [ ] `plans/README.md` status row updated

## STOP conditions

- Clearing a diagnostic requires changing runtime behavior (different SDK
  call shape, different SQL) — report which one.
- `ty`'s version has drifted and now reports a materially different
  diagnostic set (>2 new ones) — report the list instead of chasing it.
- `LiteralString` propagation fails on some line (e.g. `" AND ".join`)
  in the installed type-checker version — fall back to Step 2.3 for that one
  line, note it.

## Maintenance notes

- The maintainer declined CI; if that changes later, the three gates in
  "Commands you will need" are the workflow's job list.
- `ty` is pre-1.0 (`>=0.0.40`) — expect diagnostic churn on upgrades; keep
  the check scoped to `service/app` so churn stays small.
