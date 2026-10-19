# AGENTS.md

Rules for anyone changing this repo. Each rule names what enforces it; a
rule with no enforcement does not belong here.

## Verify

```bash
just check    # lint + tests + bandit + pip-audit, same as CI
just e2e      # the running service in Docker, end to end
```

`just install` enables `just lint test` as a git pre-commit hook.

## Rules

| Rule | Enforced by |
|---|---|
| PEP 8, line length 79, imports sorted | `ruff` (pre-commit, CI) |
| `domain.py` imports no FastAPI, aiosqlite, Prometheus or asyncio | `tests/test_static.py` |
| `repository.py` never blocks the event loop (no `sqlite3`) | `tests/test_static.py` |
| SQL values are bound parameters; only module constants are interpolated | `tests/test_static.py`, `bandit` |
| A database failure answers 503, never 500 | `tests/test_api.py` |
| The payload never reaches the logs | `tests/test_security.py` (S9) |
| No known CVEs in runtime dependencies | `pip-audit` in CI |

## How to change things

- A bug fix starts with a test that reproduces it and fails, then the fix.
- One task, one branch, one PR. Do not commit plans, task lists or
  scratch specs; `README.md` is the only design document.
- All SQL lives in `app/repository.py`; retry and recovery rules live in
  `app/domain.py` and are passed to the repository as a `Transition`.
