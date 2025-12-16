# GitHub Copilot Instructions for USF Fabric Monitoring

This repo is a **library-first** Microsoft Fabric monitoring/governance toolkit. Keep core logic in `src/usf_fabric_monitoring/`; scripts/notebooks should be thin wrappers.

## Architecture (big picture)
- **Monitor Hub (end-to-end)**: `core/pipeline.py:MonitorHubPipeline` runs extraction → loads/enriches → writes reports.
  - Activity extraction: `scripts/extract_historical_data.py` (per-day exports; respects `MAX_HISTORICAL_DAYS`)
  - Job details enrichment: `scripts/extract_fabric_item_details.py` (8-hour cache; merged in `MonitorHubPipeline._merge_activities()`)
  - Outputs: defaults to `EXPORT_DIRECTORY=exports/monitor_hub_analysis`; pipeline persists a Parquet “source of truth” under the output directory.
- **Workspace governance**: access enforcement lives in `core/workspace_access_enforcer.py` and script entrypoints under `src/usf_fabric_monitoring/scripts/`.
- **Fabric vs local paths**: always route output/data paths through `core/utils.py:resolve_path()` so relative paths land under OneLake (`/lakehouse/default/Files`) in Fabric.

## Workflows (preferred commands)
- Use the `Makefile` (environment-aware wrappers): `make create`, `make install`, `make monitor-hub`, `make enforce-access`, `make extract-lineage`, `make validate-config`, `make test-smoke`, `make build`.
- CLI entrypoints (see `pyproject.toml`): `usf-monitor-hub`, `usf-enforce-access`, `usf-validate-config`.
- Most scripts support env-driven config (see `.env.template` + `README.md`).

## Repo conventions
- **Paths**: don’t hardcode `/lakehouse/...` in library code; use `resolve_path("exports/... ")` and accept `OUTPUT_DIR`/`EXPORT_DIRECTORY` overrides.
- **Logging**: use `core/logger.py:setup_logging` (rotating files under `logs/`; optional stdout; progress printing gated by `USF_FABRIC_MONITORING_SHOW_PROGRESS`).
- **Config JSONs**: business rules live in `config/` (e.g., `inference_rules.json`, `workspace_access_targets.json`, `workspace_access_suppressions.json`). Update schemas + run `make validate-config`.
- **Tests**: unit tests live in `tests/` and are safe/offline; heavier validation scripts live in `tools/dev_tests/` and are run via Make targets.
- **Notebooks**: `notebooks/Monitor_Hub_Analysis.ipynb` is designed to be safe-by-default (verification cells should avoid API/auth imports).

## Auth/Secrets
- Prefer `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` from `.env` (see `.env.template`).
- Fabric identity fallbacks are supported in `core/auth.py` (Fabric `notebookutils` → Azure Identity). If Azure SDK deps are missing, token acquisition should fail with a clear error (don’t silently succeed).

## Quick commands (for agents)
- Smoke check: `make test-smoke` (or `python -m pytest -q tests/test_basic_imports.py`).
- Run pipeline (tenant-wide): `make monitor-hub DAYS=7` (member-only via `MEMBER_ONLY=1`; see `src/usf_fabric_monitoring/scripts/monitor_hub_pipeline.py`).
