---
description: Standard development workflow for this project
---

# Development Workflow

This workflow describes the standard development procedures for the usf_fabric_monitoring project.

## Project Structure

```
usf_fabric_monitoring/
├── src/usf_fabric_monitoring/
│   ├── core/       # Business logic modules
│   └── scripts/    # CLI entry points
├── tests/          # Test suite
├── scripts/        # Development utilities (not installed)
└── pyproject.toml  # Package configuration
```

## Entry Points

All CLI commands are defined in `pyproject.toml`:

| Command | Module |
|---------|--------|
| `usf-monitor-hub` | `scripts.monitor_hub_pipeline` |
| `usf-extract-lineage` | `scripts.extract_lineage` |
| `usf-enforce-access` | `scripts.enforce_workspace_access` |
| `usf-star-schema` | `scripts.build_star_schema` |
| `usf-validate-config` | `scripts.validate_config` |

## Common Tasks

### Setup Environment

```bash
make create    # Create conda environment
make install   # Install package in editable mode
```

### Run Tests

// turbo
```bash
make test
```

### Run Specific Command

// turbo
```bash
# Via Makefile
make monitor-hub DAYS=7

# Via entry point
usf-monitor-hub --days 7

# Via module
python -m usf_fabric_monitoring.scripts.monitor_hub_pipeline --days 7
```

### Lint and Format

// turbo
```bash
# Run pre-commit on all files
pre-commit run --all-files

# Or individual commands
make lint
make format
```

### Validate Config

// turbo
```bash
make validate-config
```

### After Modifying pyproject.toml

// turbo
```bash
pip install -e .
```

## Key Files

- `pyproject.toml` - Package metadata and entry points
- `Makefile` - Development commands
- `environment.yml` - Conda environment
- `.env` / `.env.template` - Environment variables

## GitHub Workflow

```bash
# Switch to correct account
gh auth switch -u BralaBee-LEIT

# Push changes
git push origin main
```
