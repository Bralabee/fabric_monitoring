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

## Lineage Explorer

> **CRITICAL**: The Lineage Explorer requires **iterative mode** extraction to show relationships. 
> Scanner mode only captures warehouse metadata without shortcuts/connections.

### Quick Start (Automated - Recommended)

// turbo
```bash
# Full end-to-end workflow: extract data, start Neo4j, start server
make lineage-full
```

### Manual Steps

1. **Extract lineage data** (iterative mode for rich data):
   
   // turbo
   ```bash
   # IMPORTANT: Use --mode iterative for shortcuts and connections
   make extract-lineage-full
   
   # Or via module directly:
   python -m usf_fabric_monitoring.scripts.extract_lineage --mode iterative
   ```

2. **Start Neo4j** (optional, for graph queries):
   ```bash
   cd lineage_explorer && docker compose up -d
   ```

3. **Start Lineage Explorer**:
   
   // turbo
   ```bash
   make lineage-explorer
   ```

### Table Lineage Panel (v0.3.25)

The explorer includes a Table Lineage Panel for filtering items:
- **Open**: Click the table icon in toolbar (next to labels toggle)
- **Filter by Node**: Click a graph node → panel filters to related items
- **Search**: Type to filter by name/database/path
- **Clear Filter**: Click X in the filter indicator bar

### Extraction Modes

| Mode | Command | Data Captured | Use Case |
|------|---------|---------------|----------|
| `scanner` | `make extract-lineage` | Warehouses only, no relationships | Quick inventory |
| `iterative` | `make extract-lineage-full` | Lakehouses, Shortcuts, Mirrored DBs, connections | **Rich visualization** |

### File Naming Patterns

The server selects files in this priority order:
1. `lineage_*.json` - Iterative extraction (preferred, has shortcuts)
2. `lineage_scanner_*.json` - Scanner mode (warehouse metadata only)
3. `mirrored_lineage_*.csv` - Legacy CSV format

> **Note**: If both patterns exist, the server may load the wrong file. 
> Archive scanner files if you need rich visualization.

### Neo4j Integration

After starting Lineage Explorer, load data into Neo4j via API:
```bash
curl -X POST "http://127.0.0.1:8000/api/neo4j/load?clear_existing=true"
```

**Neo4j Credentials** (from docker-compose.yml):
- Username: `neo4j`
- Password: `changeme_in_production` (or set `NEO4J_PASSWORD` env var)

## GitHub Workflow

```bash
# Switch to correct account
gh auth switch -u BralaBee-LEIT

# Push changes
git push origin main
```
