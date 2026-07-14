# Plan 007: Stop laundering unrelated TypeErrors into ModelConfigError

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 2ab6013..HEAD -- service/app/classifier.py service/tests/test_classifier.py`
> Plan 006 intentionally edits the prompt builders in this file first; the
> excerpt below is from `call_model`, which 006 must not touch. On any
> mismatch in `call_model` itself, STOP.

## Status

- **Priority**: P3
- **Effort**: S
- **Risk**: LOW
- **Depends on**: plans/006 (ordering only — same file; avoids conflicting edits)
- **Category**: bug
- **Planned at**: commit `2ab6013`, 2026-07-14

## Why this matters

`call_model` catches **every** `TypeError` from the SDK call and rebrands it
`ModelConfigError`, which the worker/retrier escalate to
`SystemExit(1)` with a CRITICAL log blaming "bad key/config?"
(`service/app/worker.py:116-120`). The catch was written for one specific
case — the SDK raises `TypeError` at request time when no API key is
configured — but as written, any other `TypeError` (an SDK version that
doesn't accept the `output_config` kwarg, a future signature change, a plain
bug) crash-loops the service with a misleading diagnosis. The intended
behavior (crash loudly on genuine misconfiguration, never swallow) is right;
the diagnosis just needs to be honest. This was a LOW-confidence audit
finding: **Step 1 is an investigate step — do it before changing code.**

## Current state

`service/app/classifier.py:120-125` (inside `call_model`'s try/except chain):

```python
        except (anthropic.RateLimitError, anthropic.APIConnectionError) as error:
            last_error = error
        except TypeError as error:
            # The SDK raises TypeError at request time when no API key is
            # configured — deterministic, so crash loudly instead of retrying.
            raise ModelConfigError(str(error)) from error
```

The existing test pins the intended case with the SDK's real message text
(`service/tests/test_classifier.py:54-60`):

```python
def test_missing_api_key_typeerror_is_config_error():
    client = FakeClient([TypeError("Could not resolve authentication method")])
    with pytest.raises(ModelConfigError, match="authentication"):
        classify(client, EDIT)
```

Routing context: `ModelConfigError` → `SystemExit(1)`, no commit, crash-loop
(worker.py:116-120, retrier.py:153-157, sweeper.py:146-150). An *uncaught*
`TypeError` propagates out of `handle_message` → `main()` → process dies with
a real traceback, offset uncommitted → also a crash-loop, but with an honest
stack trace instead of a false "fix your .env" instruction. Same operational
safety (nothing swallowed, nothing committed), better diagnosis.

Anthropic SDK pin: `anthropic==0.116.0` (`service/pyproject.toml:8`).

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Investigate | `grep -rn "Could not resolve authentication method" .venv/lib/python3.13/site-packages/anthropic/` | 1+ hits showing the raise site and full message |
| Tests | `uv run pytest service/tests/test_classifier.py -v` | all pass |
| Full suite | `uv run pytest` | all pass |
| Mutation gate | `uv run --directory service mutmut run && uv run --directory service mutmut results` | no non-acceptable survivors in `app.classifier` |

## Scope

**In scope**:
- `service/app/classifier.py` — the `except TypeError` clause in `call_model` only.
- `service/tests/test_classifier.py` — one updated + one new test.

**Out of scope**:
- Prompt builders (plan 006), `classify`, `normalize`, `OUTPUT_SCHEMA`.
- worker/retrier/sweeper routing — their handling of `ModelConfigError` is
  correct and untouched.
- Catching the misconfiguration at client construction time — the SDK
  defers auth resolution to request time; don't fight it.

## Git workflow

- Commit directly on `main` (repo convention). Conventional style, e.g.
  `fix(classifier): only treat auth-resolution TypeErrors as config errors`.

## Steps

### Step 1: INVESTIGATE — pin the SDK's actual message

Run the grep in the commands table against the installed `anthropic==0.116.0`
package. Confirm: (a) the missing-key failure is a `TypeError`, (b) the exact
message text (expected to contain "Could not resolve authentication
method"), (c) whether any *other* `TypeError` raise sites exist on the
`messages.create` request path. Record what you find in the plan's status row
note. If (a) is false — the SDK version in the lockfile no longer raises
`TypeError` for missing auth — STOP and report; the whole catch needs a
different shape and the fix below would be wrong.

### Step 2: Narrow the catch

Replace the clause with a message-matched version:

```python
        except TypeError as error:
            # The SDK raises TypeError at request time when no API key is
            # configured — deterministic, so crash loudly instead of retrying.
            # Any other TypeError is a genuine bug (e.g. an SDK signature
            # mismatch) and must surface as itself, not as "fix your .env".
            if "could not resolve authentication method" in str(error).lower():
                raise ModelConfigError(str(error)) from error
            raise
```

(Adjust the matched substring to exactly what Step 1 found, lowercased.)

**Verify**: `uv run pytest service/tests/test_classifier.py -v` → all pass
(the existing missing-key test uses matching message text, so it still
passes).

### Step 3: Add the counterfactual test

In `test_classifier.py`, next to `test_missing_api_key_typeerror_is_config_error`:

```python
def test_unrelated_typeerror_propagates_as_itself():
    # e.g. an SDK version that rejects the output_config kwarg: crashing is
    # right (deterministic, must not be swallowed), but it must crash as a
    # TypeError with a real traceback, not as "bad key/config".
    client = FakeClient([TypeError("create() got an unexpected keyword argument 'output_config'")])

    with pytest.raises(TypeError, match="unexpected keyword"):
        classify(client, EDIT)
```

Also assert `len(client.calls) == 1` (no retry — mutation testing has
caught attempt-count drift here before, per AGENTS.md).

**Verify**: `uv run pytest` → all pass.

### Step 4: Mutation gate

Run mutmut on the touched module; the message-match condition (the `in`,
the `.lower()`) must be killed by the pair of tests.

**Verify**: no non-acceptable survivors in `app.classifier`.

## Test plan

Steps 2-3. Pattern: `test_missing_api_key_typeerror_is_config_error`
(`service/tests/test_classifier.py:54`). Cases: auth TypeError →
`ModelConfigError` (existing); unrelated TypeError → propagates unwrapped,
exactly one call (new).

## Done criteria

- [ ] Step 1 findings recorded (SDK raise site + message confirmed)
- [ ] `uv run pytest` exits 0 with the new test
- [ ] The catch matches the SDK's message; unrelated TypeErrors propagate (proved by the new test)
- [ ] mutmut: no non-acceptable survivors in `app.classifier`
- [ ] `git diff --stat` touches only the two in-scope files
- [ ] `plans/README.md` status row updated

## STOP conditions

- Step 1 disproves the premise (missing key isn't a `TypeError` in
  `anthropic==0.116.0`, or the message differs materially) — report findings.
- Step 1 finds other legitimate config-shaped `TypeError` sites on the
  request path that the narrow match would misroute — report them; the match
  set may need to grow deliberately, not accidentally.
- Any existing worker/retrier/sweeper test fails (their fakes raise
  status-code errors, not TypeErrors, so nothing should change there).

## Maintenance notes

- The string match couples to the SDK's message. On any `anthropic` version
  bump, re-run Step 1's grep; the existing missing-key test will catch a
  changed message (it matches on "authentication").
- Reviewer: check the `raise` (bare re-raise) path keeps the original
  traceback — no `from` clause, no wrapping.
