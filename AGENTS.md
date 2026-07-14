## Code Style

Prefer using proper variable names instead of abbreviations. We always favor readability for the humans.

ex: 
w = 5 # bad
width = 5 # good

## Testing

This project is configured with pytest and mutmut. All changes you make should have a related test for it.
For mutation testing, use mutmut.

### Mutation testing (mutmut)

Run from `service/` (config lives in `service/pyproject.toml`):

```bash
uv run mutmut run            # full run (regenerates service/mutants/, gitignored)
uv run mutmut results        # per-mutant outcomes
uv run mutmut show <mutant>  # diff for one mutant, e.g. app.worker.x__handle_edit__mutmut_60
```

Expectations when you add or change code in `service/app/`:

- Run mutmut on the touched module and make sure your change does not add
  surviving mutants that alter behavior. Delete `service/mutants/` first if
  results look stale.
- Tests must pin down data, not just routing. The recurring gaps mutation
  testing has caught here: envelope fields (`reason`, `source`, `error`,
  `attempts`, `not_before` schedule), DB parameter dicts and SQL identity,
  error-message content (it becomes DLQ/row provenance — assert on it),
  `SystemExit` codes (`assert excinfo.value.code == 1`; exit 0 would read as
  success to the restart policy), retry/backoff arithmetic and boundary
  conditions (`<` vs `<=` at thresholds, attempt counts).
- Use the shared fakes in `tests/fakes.py` (FakeClient/FakeConn/FakeConsumer/
  FakeProducer + the shared ordering log) instead of inventing new doubles;
  extend them if a seam is missing.
- Acceptable survivors: mutations confined to log-message strings, and
  equivalent mutants (e.g. `decode("ascii")` -> `decode("ASCII")`, or removing
  a kwarg that restates its default). Everything else needs a test.
- Prompt text in `classifier.py` is product behavior, not a log message — the
  prompt invariant tests in `test_classifier.py` must be updated deliberately
  when prompts change.

## Documentation

README.md — must contain copy-pasteable run instructions, the Tradeoffs section (Leave empty, for the user to complete), surprises, production-failure notes

## Hints

When stuck, see @HINTS.md for common gotchas and solutions.