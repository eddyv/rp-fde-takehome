# Plan 001: Make README, AGENTS.md, and worker.py comments match the code that actually exists

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 2ab6013..HEAD -- README.md AGENTS.md service/app/worker.py`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P1
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: docs
- **Planned at**: commit `2ab6013`, 2026-07-14

## Why this matters

This repo is a take-home whose README is literally the evaluated artifact, and
the follow-up walkthrough explicitly probes "what happens on a malformed model
response". Commit `d41f233` ("refactor(classifier): Migrate JSON parsing to
native structured outputs") deleted the brace-counting `{...}` extractor and
the format-reminder retry — but the README still describes both, and a
`worker.py` comment still counts model calls using the old three-stage math.
Separately, `AGENTS.md` instructs contributors to use tooling (deepeval,
coverage) that is not installed and references "golden tests" that commit
`193686c` replaced with invariant assertions. Docs that contradict the code
are worse than missing docs; here they are an active anti-signal.

## Current state

Files and the exact stale text (verified against `git show d41f233` and the
current code):

1. `README.md:102-107` — the Layout bullet for `classifier.py` reads:

   ```
   - `service/app/classifier.py` — the LLM loop as named stages: prompt build →
     API call (bounded retry + backoff) → parse first `{...}` block → normalize
     label to the enum → retry once on parse failure → second-pass prompt when
     confidence is below threshold. Failures raise a typed taxonomy
     (`ModelConfigError` / `ModelUnavailableError` / `ClassificationParseError`)
     instead of fabricating an `unclear` row.
   ```

   Reality (`service/app/classifier.py:1-18,102-119,175-188`): the model call
   uses Anthropic structured outputs (`output_config` with a JSON schema that
   pins the label enum); the response is parsed as whole-text JSON
   (`parse_response` → `json.loads`, no `{...}` extraction); there is NO
   format-reminder retry — unusable output raises `ClassificationParseError`
   after one call (pinned by `test_classifier.py:29-37`,
   "structured outputs removed the format retry").

2. `README.md:130` — failure-handling table row:

   ```
   | Parse failure | unusable output after the format-reminder retry | `status='failed'` row + envelope, commit, breaker reset | `wiki.edits.dlq` |
   ```

   The "after the format-reminder retry" clause is stale (same reason).

3. `service/app/worker.py:44-46` — comment above `MAX_POLL_INTERVAL_MS`:

   ```python
   # Model calls are bounded (see main: request timeout 60s; classify makes at
   # most 9 calls plus seconds of backoff), so per-message time stays under this.
   # The kafka-python default (300s) is too tight for a slow multi-pass classify.
   ```

   Reality: `classify()` makes at most **6** calls — `_attempt` calls
   `call_model` (up to `MAX_CALL_ATTEMPTS = 3` calls, `classifier.py:47,110`),
   once for the first pass and once for the optional second pass
   (`classifier.py:201,212`). The 9 came from the deleted format-retry stage.

4. `AGENTS.md:10` — "This project is configured with pytest, mutmut,
   coverage." — no coverage tool exists (root `pyproject.toml:17-24` dev group
   is `mutmut`, `pytest`, `ruff`, `testcontainers`, `ty`; no
   `pytest-cov`/`coverage` anywhere, no coverage config).

5. `AGENTS.md:12` — "For evaluations, use deepeval." — deepeval was removed in
   commit `258c12b` ("chore: Remove deepeval dependency…"); it appears nowhere
   in the repo.

6. `AGENTS.md:44-45` — "Prompt text in `classifier.py` is product behavior,
   not a log message — the golden tests in `test_classifier.py` must be
   updated deliberately when prompts change." — commit `193686c` replaced
   golden prompt tests with invariant assertions; the current tests are e.g.
   `test_prompt_label_menu_matches_the_validation_enum`
   (`service/tests/test_classifier.py:239`) and
   `test_prompt_includes_the_edit_fields_the_model_judges` (`:249`).

Repo conventions: prose style in README is terse and technical; keep edits
minimal and factual — do not rewrite surrounding text.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Tests | `uv run pytest` | 115 passed (12 deselected) |
| Lint | `uv run ruff check .` | only 2 pre-existing E501 errors in `service/tests/integration/` (fixed by plan 005) |
| Stale-string sweep | see Done criteria greps | no matches |

## Scope

**In scope** (the only files you should modify):
- `README.md` (specific lines only, listed above)
- `AGENTS.md` (specific lines only)
- `service/app/worker.py` (the comment at lines 44-46 ONLY — no code)

**Out of scope** (do NOT touch, even though they look related):
- `README.md` "## Tradeoffs" section (line ~227) — it is a deliberate TODO the
  human author must write personally; an exercise rule forbids AI-written
  text there. Do not add, draft, or stub anything in it.
- Any other README section (Surprises, Production-failure notes, runbook) —
  they were verified accurate.
- `HINTS.md` and `TAKEHOME.md` — upstream exercise material, not repo docs.
- Any executable code in `service/app/` — this plan changes prose only.
- Do not add coverage/deepeval tooling; this plan corrects the docs to match
  reality (adding coverage is a separate decision the maintainer didn't take).

## Git workflow

- Commit directly on `main` (this repo's convention — no feature branches).
- One commit, conventional style matching `git log`, e.g.
  `docs: align README/AGENTS.md with the structured-outputs classifier`.
- Never commit `.env`.

## Steps

### Step 1: Fix the README classifier bullet

In `README.md:102-107`, replace the stage list so it describes the current
loop. Target shape (adjust wording to flow, keep the taxonomy sentence):

```
- `service/app/classifier.py` — the LLM loop as named stages: prompt build →
  API call (bounded retry + backoff, structured outputs pin the JSON schema
  and label enum) → parse + normalize (confidence clamped; refusal/truncation
  rejected) → second-pass prompt when confidence is below threshold. Failures
  raise a typed taxonomy (`ModelConfigError` / `ModelUnavailableError` /
  `ClassificationParseError`) instead of fabricating an `unclear` row.
```

**Verify**: `grep -n 'format-reminder\|first .{...}. block\|retry once on parse' README.md` → only the line-130 table row remains (fixed next step).

### Step 2: Fix the README failure table row

In `README.md:130`, change the Detection cell from
`unusable output after the format-reminder retry` to
`refusal, truncation, or non-conforming output (single call; structured outputs, no format retry)`.
Keep the Action and Destination cells untouched; keep the table aligned.

**Verify**: `grep -n 'format-reminder' README.md` → no matches.

### Step 3: Fix the worker.py call-count comment

In `service/app/worker.py:44-46`, change "at most 9 calls" to "at most 6
calls (3 bounded attempts × first + optional second pass)". Touch nothing
else in the file.

**Verify**: `uv run pytest` → 115 passed. `grep -n '9 calls' service/app/worker.py` → no matches.

### Step 4: Fix AGENTS.md

- Line 10: "configured with pytest, mutmut, coverage" → "configured with
  pytest and mutmut".
- Line 12: delete the sentence "For evaluations, use deepeval."
- Lines 44-45: "the golden tests in `test_classifier.py`" → "the prompt
  invariant tests in `test_classifier.py`" (keep the rest of the sentence).

**Verify**: `grep -n 'deepeval\|coverage\|golden' AGENTS.md` → no matches (word "coverage" may legitimately appear elsewhere — confirm any remaining hit is not a tooling claim).

## Test plan

No new tests — this plan changes prose and one comment. The full suite must
still pass to prove no code was accidentally touched.

## Done criteria

- [ ] `grep -rn 'format-reminder' README.md service/` → no matches
- [ ] `grep -n 'first .{...}. block' README.md` → no matches
- [ ] `grep -n '9 calls' service/app/worker.py` → no matches
- [ ] `grep -n 'deepeval' AGENTS.md pyproject.toml service/pyproject.toml` → no matches
- [ ] `uv run pytest` → 115 passed
- [ ] `git diff --stat` shows only README.md, AGENTS.md, service/app/worker.py
- [ ] The "## Tradeoffs" section body is byte-identical to before (`git diff README.md` shows no hunk there)
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back (do not improvise) if:

- Any excerpt in "Current state" does not match the live file (drift since `2ab6013`).
- You find yourself wanting to edit any README section other than the two
  listed spots — that means the plan's premise is off.
- A test fails after your change (should be impossible for prose edits).

## Maintenance notes

- Future prompt/classifier changes must update the README Layout bullet in the
  same commit — this drift happened because `d41f233` didn't.
- Reviewer should check: no new claims were introduced, only stale ones
  removed; the Tradeoffs section is untouched.
