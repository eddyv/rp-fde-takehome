# Plan 015: Assert real backoff values in the 5xx classifier-exhaustion test

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md`.
>
> **Drift check (run first)**: `git diff --stat 585b9f6..HEAD -- service/tests/test_classifier.py`
> If this file changed since this plan was written, compare the "Current
> state" excerpt against the live code before proceeding; on a mismatch,
> treat it as a STOP condition.

## Status

- **Priority**: P3
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: tests
- **Planned at**: commit `585b9f6`, 2026-07-14

## Why this matters

`service/tests/conftest.py`'s autouse `no_real_sleep` fixture neutralizes
`time.sleep` for every non-integration test, which is fine — but it means a
test that wants to prove the classifier's exponential backoff
(`app/classifier.py:49-50`: `MAX_CALL_ATTEMPTS = 3`, `BACKOFF_SECONDS = [1, 2,
4]`) actually runs must re-patch `time.sleep` itself and assert on the
captured values, the way its sibling tests already do. Two of the three
`call_model` retry-exhaustion tests in `service/tests/test_classifier.py` do
exactly that:

- `test_rate_limit_exhaustion_raises_unavailable_after_three_calls` (line
  88): captures sleeps, asserts `sleeps == [1, 2]`.
- `test_response_validation_error_exhausts_to_unavailable` (line 231):
  same.

The third, `test_5xx_exhaustion_raises_unavailable_after_three_calls` (line
245), does not — it takes no `monkeypatch` parameter and relies solely on the
global fixture, so it only checks `len(client.calls) == 3` and the raised
exception type. All three tests exercise the exact same `call_model` retry
loop and the exact same `BACKOFF_SECONDS` list (the 429/APIError/5xx branches
in `classifier.py:138-161` all fall through to the same
`time.sleep(BACKOFF_SECONDS[attempt])` at line 161), so the practical risk of
this specific gap is small — but it's an avoidable inconsistency, and
"assert the real captured values" is cheap insurance against a future
refactor that splits the branches and only breaks backoff for one of them.

## Current state

`service/tests/test_classifier.py:245-253` (current):
```python
@pytest.mark.parametrize("status", [500, 502, 503])
def test_5xx_exhaustion_raises_unavailable_after_three_calls(status):
    client = FakeClient([make_status_error(status)] * 3)

    with pytest.raises(ModelUnavailableError, match=f"http {status}"):
        classify(client, EDIT)

    assert len(client.calls) == 3
```

Sibling test to mirror — `service/tests/test_classifier.py:88-98`:
```python
def test_rate_limit_exhaustion_raises_unavailable_after_three_calls(monkeypatch):
    sleeps: list = []
    monkeypatch.setattr(app.classifier.time, "sleep", lambda s: sleeps.append(s))
    client = FakeClient([make_status_error(429)] * 3)

    with pytest.raises(ModelUnavailableError, match="http 429"):
        classify(client, EDIT)

    assert len(client.calls) == 3
    assert sleeps == [1, 2], "backoff between attempts, none after the last"
```
`test_classifier.py:1-25` already imports `app.classifier` (as `import
app.classifier`) — the same module reference the sibling test uses for
`app.classifier.time` — so no new import is needed.

## Commands you will need

| Purpose   | Command                                                    | Expected on success |
|-----------|--------------------------------------------------------------|---------------------|
| Just this test | `cd service && uv run pytest tests/test_classifier.py -k 5xx -v` | 3 passed (parametrized over 500/502/503) |
| Full unit suite | `cd service && uv run pytest` | all pass |
| Lint | `cd service && uv run ruff check .` | exit 0 |

## Scope

**In scope**:
- `service/tests/test_classifier.py` — modify exactly one test function
  (`test_5xx_exhaustion_raises_unavailable_after_three_calls`).

**Out of scope**:
- `service/app/classifier.py` — no production code changes; this is a
  test-only tightening.
- Any other test in the file.

## Git workflow

- No feature branch — this repo commits directly on `main`.
- Commit message style: conventional commits, e.g.
  `test(classifier): assert real backoff values in the 5xx exhaustion test`.
- Do NOT push unless explicitly instructed.

## Steps

### Step 1: Add the `monkeypatch` parameter and sleep capture

In `service/tests/test_classifier.py`, change:

```python
@pytest.mark.parametrize("status", [500, 502, 503])
def test_5xx_exhaustion_raises_unavailable_after_three_calls(status):
    client = FakeClient([make_status_error(status)] * 3)

    with pytest.raises(ModelUnavailableError, match=f"http {status}"):
        classify(client, EDIT)

    assert len(client.calls) == 3
```

to:

```python
@pytest.mark.parametrize("status", [500, 502, 503])
def test_5xx_exhaustion_raises_unavailable_after_three_calls(status, monkeypatch):
    sleeps: list = []
    monkeypatch.setattr(app.classifier.time, "sleep", lambda s: sleeps.append(s))
    client = FakeClient([make_status_error(status)] * 3)

    with pytest.raises(ModelUnavailableError, match=f"http {status}"):
        classify(client, EDIT)

    assert len(client.calls) == 3
    assert sleeps == [1, 2], "backoff between attempts, none after the last"
```

Note the parametrize decorator's parameter order: pytest matches
`monkeypatch` by fixture name regardless of position, so appending it after
`status` in the function signature is correct and matches how
`monkeypatch`-using parametrized tests are conventionally written.

**Verify**: `cd service && uv run pytest tests/test_classifier.py -k 5xx -v` →
3 passed (one per parametrized status).

### Step 2: Full verification

**Verify**: `cd service && uv run pytest` → all pass. Then
`cd service && uv run ruff check .` → exit 0.

## Test plan

- No new test — this plan strengthens an existing one.
- Structural pattern:
  `test_rate_limit_exhaustion_raises_unavailable_after_three_calls`
  (`service/tests/test_classifier.py:88-98`) and
  `test_response_validation_error_exhausts_to_unavailable`
  (`service/tests/test_classifier.py:231-242`) — both already assert
  `sleeps == [1, 2]` the same way this plan adds to the 5xx test.
- Verification: `cd service && uv run pytest tests/test_classifier.py -k 5xx -v` →
  3 passed, with the new `sleeps == [1, 2]` assertion present and green.

## Done criteria

Machine-checkable. ALL must hold:

- [ ] `cd service && uv run pytest tests/test_classifier.py -k 5xx -v` passes, 3 tests
- [ ] `cd service && uv run pytest` exits 0 (full suite)
- [ ] `cd service && uv run ruff check .` exits 0
- [ ] `grep -n "def test_5xx_exhaustion_raises_unavailable_after_three_calls" service/tests/test_classifier.py` shows the signature now includes `monkeypatch`
- [ ] No files outside `service/tests/test_classifier.py` are modified (`git status`)
- [ ] `plans/README.md` status row for 015 updated

## STOP conditions

Stop and report back (do not improvise) if:

- The code at `service/tests/test_classifier.py:245-253` doesn't match the
  "Current state" excerpt (drift since this plan was written).
- The new assertion `sleeps == [1, 2]` fails for any of the three
  parametrized statuses — that would mean the 5xx branch's backoff genuinely
  differs from the 429/validation-error branches' backoff, which is a real
  finding, not a test bug. Stop and report it rather than adjusting the
  assertion to match whatever the code actually does.
- A step's verification fails twice after a reasonable fix attempt.

## Maintenance notes

- This is the last of the three `call_model`-exhaustion tests to assert real
  backoff values; if a fourth such test is ever added for a new error
  branch, it should follow the same `sleeps == [...]` pattern from the
  start rather than needing a follow-up tightening plan like this one.
- Nothing else interacts with this change — it's a self-contained test
  strengthening with no production code touched.
