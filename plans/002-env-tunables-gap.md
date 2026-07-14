# Plan 002: Make the retry-backoff and sweeper-model tunables reachable and documented

> **Executor instructions**: Follow this plan step by step. Run every
> verification command and confirm the expected result before moving to the
> next step. If anything in the "STOP conditions" section occurs, stop and
> report — do not improvise. When done, update the status row for this plan
> in `plans/README.md` — unless a reviewer dispatched you and told you they
> maintain the index.
>
> **Drift check (run first)**: `git diff --stat 2ab6013..HEAD -- .env.example docker-compose.yml service/app/config.py`
> If any in-scope file changed since this plan was written, compare the
> "Current state" excerpts against the live code before proceeding; on a
> mismatch, treat it as a STOP condition.

## Status

- **Priority**: P2
- **Effort**: S
- **Risk**: LOW
- **Depends on**: none
- **Category**: dx
- **Planned at**: commit `2ab6013`, 2026-07-14

## Why this matters

The README's operator runbook advertises the retry schedule ("30s → 60s →
120s cap", `README.md:135-138`) as a property of the system, and
`service/app/config.py` defines it as tunable settings — but docker-compose
forwards a fixed env allowlist to the worker and retrier that omits them, so
on the primary (Docker) run path the schedule is effectively hardcoded.
`SWEEPER_MODEL` is similarly real-but-undocumented. Closing this gap makes
`.env.example` the honest, complete catalogue of operator knobs.

## Current state

- `service/app/config.py:22-38` defines (pydantic-settings, env-var names are
  the upper-cased field names):

  ```python
  anthropic_model: str = "claude-haiku-4-5"
  # Stronger model for manual DLQ drains; None falls back to anthropic_model.
  sweeper_model: str | None = None
  ...
  max_retry_passes: int = 3
  retry_backoff_base_seconds: int = 30
  retry_backoff_max_seconds: int = 120
  ```

- `.env.example` documents only `ANTHROPIC_API_KEY`, `ANTHROPIC_MODEL`,
  `ANTHROPIC_BASE_URL`, `CONFIDENCE_THRESHOLD`, `BREAKER_THRESHOLD`,
  `MAX_RETRY_PASSES` — no `SWEEPER_MODEL`, no `RETRY_BACKOFF_*`.

- `docker-compose.yml:74-86` (worker) and `:103-111` (retrier) forward env
  with the `${VAR:-default}` pattern, e.g.:

  ```yaml
  CONFIDENCE_THRESHOLD: ${CONFIDENCE_THRESHOLD:-0.6}
  BREAKER_THRESHOLD: ${BREAKER_THRESHOLD:-25}
  MAX_RETRY_PASSES: ${MAX_RETRY_PASSES:-3}
  ```

  Neither service forwards `RETRY_BACKOFF_BASE_SECONDS` /
  `RETRY_BACKOFF_MAX_SECONDS`. Both the worker and the retrier consume them
  (`failures.retry_delay_seconds`, `service/app/failures.py:86-95`, called
  from `worker.py:139` and `retrier.py:191`).

- **Load-bearing convention** — the compose file's own comment
  (`docker-compose.yml:77-80`): empty-string env values break the service
  ("pydantic rejects `""` for numbers"), so every forwarded numeric var MUST
  use `${VAR:-<real default>}`, never `${VAR:-}`.

- The sweeper runs via `docker compose run --rm worker python -m app.sweeper`
  (`README.md:172-175`), so it inherits the **worker's** environment map.
  `SWEEPER_MODEL` is a `str | None`; the sweeper resolves
  `args.model or settings.sweeper_model or settings.anthropic_model`
  (`service/app/sweeper.py:65`), so an empty string would harmlessly fall
  through — but to honor the compose comment's convention, do NOT forward
  `SWEEPER_MODEL` with an empty default; document it for the native path and
  point Docker users at the existing `--model` flag instead.

## Commands you will need

| Purpose | Command | Expected on success |
|---------|---------|---------------------|
| Compose syntax check | `docker compose config --quiet` | exit 0, no output |
| Rendered-env check | `docker compose config \| grep -A2 RETRY_BACKOFF` | both vars with defaults, under worker and retrier |
| Tests | `uv run pytest` | 115 passed |

(`docker compose config` only renders the file — it does not start anything.)

## Scope

**In scope** (the only files you should modify):
- `.env.example`
- `docker-compose.yml` (worker and retrier `environment` maps only)

**Out of scope**:
- `service/app/config.py` — the settings already exist; no code change.
- The `api` service in compose — it takes no LLM/retry settings.
- `README.md` — the runbook text stays accurate as-is (it names the defaults,
  which remain the defaults).
- Kafka topics / consumer groups / DSN settings — infra values compose sets
  directly; deliberately not operator-facing.

## Git workflow

- Commit directly on `main` (repo convention). Conventional style, e.g.
  `fix(compose): expose retry-backoff tunables and document SWEEPER_MODEL`.
- Never commit `.env`.

## Steps

### Step 1: Extend `.env.example`

Append to the "Optional overrides (defaults shown)" block, matching its
comment style:

```
# SWEEPER_MODEL=claude-sonnet-4-5    # DLQ sweeper default model (native runs);
#                                    # on Docker use: docker compose run --rm worker \
#                                    #   python -m app.sweeper --model <model>
# RETRY_BACKOFF_BASE_SECONDS=30     # retry-topic schedule: base * 2**(n-1) ...
# RETRY_BACKOFF_MAX_SECONDS=120     # ... capped here (keep well under 600s poll interval)
```

**Verify**: `grep -c 'RETRY_BACKOFF' .env.example` → 2.

### Step 2: Forward the backoff vars in compose

Add to BOTH the `worker` and `retrier` `environment` maps, next to
`MAX_RETRY_PASSES`, following the existing pattern exactly:

```yaml
      RETRY_BACKOFF_BASE_SECONDS: ${RETRY_BACKOFF_BASE_SECONDS:-30}
      RETRY_BACKOFF_MAX_SECONDS: ${RETRY_BACKOFF_MAX_SECONDS:-120}
```

Defaults must equal the `config.py` defaults (30 / 120).

**Verify**: `docker compose config --quiet` → exit 0; `docker compose config | grep -c 'RETRY_BACKOFF_BASE_SECONDS'` → 2 (worker + retrier).

## Test plan

No new tests: this plan adds env plumbing with defaults identical to code
defaults, so behavior is unchanged. `uv run pytest` must still pass. If
Docker is available, `docker compose config` is the verification gate; if it
is not available, note that in the status row and verify by eye against the
existing `MAX_RETRY_PASSES` lines.

## Done criteria

- [ ] `.env.example` documents `SWEEPER_MODEL`, `RETRY_BACKOFF_BASE_SECONDS`, `RETRY_BACKOFF_MAX_SECONDS`
- [ ] `docker compose config --quiet` exits 0
- [ ] Both worker and retrier render `RETRY_BACKOFF_*` with defaults 30/120
- [ ] No `${VAR:-}` (empty default) introduced anywhere (`grep -n ':-}' docker-compose.yml` → no matches)
- [ ] `uv run pytest` → 115 passed
- [ ] `git diff --stat` shows only `.env.example` and `docker-compose.yml`
- [ ] `plans/README.md` status row updated

## STOP conditions

Stop and report back if:

- `config.py` defaults are no longer 30/120 (compose defaults must mirror
  them — mismatched defaults are worse than the current gap).
- You feel the need to forward `SWEEPER_MODEL` through compose — the
  empty-string hazard says don't; report instead if you disagree.

## Maintenance notes

- Any future `config.py` setting intended for operators needs the same two
  touches: `.env.example` entry + compose forwarding with a matching default.
  A drifted default between compose and config.py would be silent — reviewers
  should diff the two whenever either changes.
