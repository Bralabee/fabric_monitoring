# USF Fabric Monitoring — Gap Report (2025-12-14)

Scope: `usf_fabric_monitoring` repo (library, scripts, docs, packaging, tests). This report focuses on operational readiness, documentation accuracy, and maintainability.

## Current status (what’s solid)

- Core architecture is coherent (`core/` library, `scripts/` entry points, `Makefile` workflows).
- Packaging uses `pyproject.toml` with `src/` layout; dependencies are declared and version is consistent (`0.3.35`).
- Pytest defaults are now safe by default (runs `tests/` only; integration tests are opt-in via marker).
- Auth strategy in `core/auth.py` is robust (explicit Service Principal → Fabric notebook identity → `DefaultAzureCredential`).

## Status

All issues identified in this gap report have been addressed with non-breaking improvements (additive changes, doc/template alignment, and optional validation tooling).

## Previously identified gaps (resolved)

1) Documentation references files that do not exist
- Added `CHANGELOG.md`.
- Updated `README.md` to reflect `pyproject.toml` dependencies (no `requirements.txt` claim).
- Added `LICENSE` (MIT).

Impact: breaks onboarding trust, makes compliance reviews harder, and causes immediate confusion for new users.

2) Environment setup documentation contradicts the actual auth implementation
- Updated `README.md` to document `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` and `DefaultAzureCredential`.
- Added backward-compatible env var aliases in `core/auth.py` (`TENANT_ID`, `CLIENT_ID`, `CLIENT_SECRET`).

Impact: new users will configure the wrong variables and fail authentication.

3) `.env.template` appears copied from a different repository and is misleading
- Replaced `.env.template` with a monitoring-focused template containing only variables used by this repo.

Impact: onboarding friction, higher chance of misconfiguration, and increased support burden.

## High-priority gaps (resolved)

1) No CI automation present in-repo
- Added `.github/workflows/ci.yml` to run `python -m build` and safe `pytest -q`.

Impact: no automated lint/test/build guardrails; regressions will ship unnoticed.

2) Script UX messaging has contradictions
- Updated `scripts/monitor_hub_pipeline.py` examples and fixed the `--days` plumbing to pass the computed effective days.

Impact: users will run the tool expecting one behavior and get another.

## Medium-priority gaps (resolved)

1) Mixed logging vs `print()` in core runtime paths
- Added `USF_FABRIC_MONITORING_SHOW_PROGRESS` gating for console progress output (default preserves current behavior).

Impact: harder to run headless/structured logs; inconsistent operational telemetry.

2) Code hygiene nits in imports
- Removed the duplicate `infer_location` import in `core/extractor.py`.

Impact: low, but indicates review debt and increases noise.

3) Config schema validation is not enforced
- Added `core/config_validation.py`, `tests/test_config_validation.py`, and `make validate-config`.
- Added warn-only validation for `config/inference_rules.json` on load.

Impact: silent misconfiguration risk; issues surface late at runtime.

## Low-priority gaps / polish (resolved)

- Added console entry points via `[project.scripts]` in `pyproject.toml` (`usf-monitor-hub`, `usf-enforce-access`, `usf-validate-config`).
- Centralized env var guidance across `README.md`, `.env.template`, and code (with backward-compatible aliases).

## Optional next steps (nice-to-have)

1) Extend validation coverage
- Add schemas for any additional config files you introduce later.

2) Optional logging cleanup
- Convert remaining CLI-facing `print()` calls to structured logging where appropriate, keeping human-friendly CLI output in entrypoints.

## Notes / constraints observed

- Given prior “broad tests crashed the system”, keep CI and local defaults aligned with the current “safe-by-default” pytest config, and make any API-calling tests opt-in via the `integration` marker.
