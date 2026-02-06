# GitHub Copilot Instructions for USF Fabric Monitoring

**Version**: 0.3.33 (February 2026) | **Library-first** Microsoft Fabric monitoring/governance toolkit.
Core logic in `src/usf_fabric_monitoring/`; scripts/notebooks are thin wrappers.

## Project Structure (src-layout)

This project uses **Python src-layout** (`src/usf_fabric_monitoring/`). The `pyproject.toml` confirms:
```toml
[tool.setuptools.packages.find]
where = ["src"]
```
The nested `src/usf_fabric_monitoring/` structure is **correct by design**, not a mistake.

## Architecture

### Data Pipeline Flow
```
Monitor Hub API → extraction → Smart Merge (jobs+activities) → Parquet → Star Schema → Analytics
```

1. **Monitor Hub Pipeline** (`core/pipeline.py:MonitorHubPipeline`)
   - Extracts activity events + job details → merges via Smart Merge → writes parquet
   - Output: `exports/monitor_hub_analysis/parquet/activities_*.parquet` (28 columns, source of truth)
   - Legacy CSV: `activities_master_*.csv` (19 columns, incomplete - avoid)
   - **Validated**: 1.28M activities, 6,218 failures (99.52% success rate)

2. **Star Schema Builder** (`core/star_schema_builder.py`)
   - Transforms raw activities → Kimball dimensional model
   - **Critical function**: `build_star_schema_from_pipeline_output()` - use this, NOT `StarSchemaBuilder` directly
   - Output: `exports/star_schema/*.parquet` (dim_date, dim_time, dim_workspace, dim_item, dim_user, dim_activity_type, dim_status, fact_activity, fact_daily_metrics)
   - **Validated**: 46 activity types (12 with failures, 34 audit-log only)

3. **Workspace Governance** (`core/workspace_access_enforcer.py`)
   - Enforces security group assignments to workspaces

### Smart Merge Algorithm
The Smart Merge correlates two data sources to enrich activity data:
- **Activity Events API**: High-volume audit log (ReadArtifact, CreateFile, ViewReport) - ALL return status="Succeeded" because audit entries record that an event happened, not whether it "succeeded"
- **Job History API**: Lower-volume job executions (Pipeline, Notebook, Dataflow) - HAS actual failure data with `end_time`, `failure_reason`, `duration`

**Key Insight**: Activity Events API entries CANNOT fail by design - they are audit log entries. Only Job History API entries can have failures.

Merge logic in `MonitorHubPipeline._merge_activities()`:
1. Extract activities from Activity Events API (per-day, paginated)
2. Extract job details from Job History API (8-hour cache)
3. Match by `item_id` + `merge_asof` with 5-minute timestamp tolerance
4. Enrich with duration, status, failure_reason from job history
5. Write merged result to `parquet/activities_*.parquet`

**Smart Merge Stats (Dec 2025 validation run)**:
- Activities enriched: 1,416,701
- Missing end times fixed: 95,352
- Duration data restored: 90,409 records
- Total detailed jobs loaded: 15,952

### Workspace Name Enrichment
The `StarSchemaBuilder` auto-enriches activities with workspace names:
1. `_load_workspace_lookup()` searches for workspace parquet in:
   - Explicit path (if provided)
   - `exports/monitor_hub_analysis/parquet/workspaces_*.parquet`
   - `notebooks/monitor_hub_analysis/parquet/`
2. `_enrich_activities_with_workspace_names()` joins workspace names before dimension build
3. Result: 99%+ of activities get proper workspace names (vs "Unknown" from ID-only data)

### Critical Data Patterns

**Activity Types**: Two sources with different failure behavior:
- **Audit Log activities** (ReadArtifact, CreateFile, ViewReport, RenameFileOrDirectory, etc.): 
  - High volume (1M+ per month)
  - ALL return status="Succeeded" - this is CORRECT behavior, not a bug
  - These entries record "user X did action Y" - the action always "succeeds" in being recorded
  - 34 activity types fall in this category
  
- **Job History activities** (Pipeline, RunArtifact, StartRunNotebook, etc.): 
  - Lower volume (tens of thousands per month)
  - CAN have failures with actual failure_reason
  - Have `end_time` but NULL `start_time` - star schema uses `end_time` fallback
  - Have `workspace_name` but NULL `workspace_id` - star schema uses name-based lookup
  - 12 activity types can have failures

**Validated Failure Distribution (Dec 2025)**:
| Activity Type | Failed | Failure % |
|--------------|--------|-----------|
| ReadArtifact | 3,019 | 0.79% |
| RunArtifact | 2,336 | 2.47% |
| UpdateArtifact | 251 | 0.94% |
| MountStorageByMssparkutils | 191 | 1.07% |
| ViewSparkAppLog | 159 | 0.23% |
| StartRunNotebook | 115 | 1.17% |
| StopNotebookSession | 110 | 1.07% |

**Counting Pattern** - ALWAYS use `record_count` sum, NOT `activity_id` count:
```python
# ✅ CORRECT - record_count exists for every activity
stats = fact.groupby('workspace_sk').agg(activity_count=('record_count', 'sum'))

# ❌ WRONG - activity_id is NULL for 99% of granular audit log operations
stats = fact.groupby('workspace_sk').agg(activity_count=('activity_id', 'count'))
```

**Dimension Columns** (check schema, not assumptions):
- `dim_time`: `hour_24`, `time_period` (NOT `hour`, `period_of_day`)
- `dim_date`: `month_number` (NOT `month`)
- `dim_status`: `status_code` (NOT `status_name`)

## Environment Setup

**CRITICAL**: Always use the project's conda environment for all Python operations.

```bash
# Create environment (first time only)
make create
# OR: conda env create -f environment.yml

# Activate environment (REQUIRED before any Python command)
conda activate fabric-monitoring

# Verify correct environment
conda env list  # Should show * next to fabric-monitoring

# Install package in editable mode
make install
# OR: conda run -n fabric-monitoring pip install -e .
```

**For terminal commands**, always prefix with the conda environment:
```bash
# ✅ CORRECT - uses project environment
conda run -n fabric-monitoring python script.py
conda run -n fabric-monitoring pytest tests/

# ❌ WRONG - may use wrong Python/packages
python script.py
```

## Workflows

### Primary Commands (via Makefile)
```bash
make create                    # Create conda environment
make install                   # Install package (editable)
make monitor-hub DAYS=7        # Run Monitor Hub extraction
make star-schema               # Build star schema (incremental)
make star-schema FULL_REFRESH=1  # Full rebuild
make validate-config           # Validate config JSONs
make test-smoke                # Quick import tests
```

### CLI Entrypoints (see `pyproject.toml`)
`usf-monitor-hub`, `usf-enforce-access`, `usf-validate-config`, `usf-star-schema`

## Conventions

- **Paths**: Use `core/utils.py:resolve_path()` - auto-resolves to OneLake in Fabric
- **Logging**: Use `core/logger.py:setup_logging` (rotating files under `logs/`)
- **Config**: Business rules in `config/*.json` (inference_rules, workspace_access_targets, etc.)
- **Tests**: Unit tests in `tests/` (offline-safe); integration in `tools/dev_tests/`
- **Notebooks**: `notebooks/Fabric_Star_Schema_Builder.ipynb` - always reload modules with `importlib.reload()`

### Type Safety (v0.3.23+)
Use `core/type_safety.py` for defensive data handling:
```python
from usf_fabric_monitoring.core.type_safety import (
    safe_int64,            # Handle NaN/None → Int64
    coerce_surrogate_keys, # Enforce Int64 for *_sk columns
    safe_datetime,         # Parse datetime with format fallbacks
    safe_workspace_lookup, # Fallback chain: ID → Name → default
)

# Example: Fix Direct Lake Float64 issue
df = coerce_surrogate_keys(df)  # All *_sk columns → Int64
```

### Environment Detection (v0.3.23+)
Use `core/env_detection.py` for cross-platform handling:
```python
from usf_fabric_monitoring.core.env_detection import (
    detect_environment,     # Returns LOCAL/FABRIC_NOTEBOOK/FABRIC_PIPELINE
    is_fabric_environment,  # Boolean check
    get_default_output_path, # Environment-aware path
)

if is_fabric_environment():
    output_dir = get_default_output_path()  # /lakehouse/default/Files/exports
else:
    output_dir = get_default_output_path()  # ./exports
```

### Config Validation (v0.3.23+)
All config files have JSON schemas in `config/schemas/`:
```bash
# Validate all configs
make validate-config
# OR: python -c "from usf_fabric_monitoring.core.config_validation import validate_all_configs, print_validation_report; _, _, e = validate_all_configs(); print_validation_report(e)"
```

## Auth/Secrets
- Set `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` in `.env`
- Fallback chain in `core/auth.py`: Fabric `notebookutils` → Azure Identity → fail with clear error

## Star Schema Build (Notebook Pattern)
```python
import importlib
import usf_fabric_monitoring.core.star_schema_builder as ssb_module
importlib.reload(ssb_module)  # Pick up code changes

result = ssb_module.build_star_schema_from_pipeline_output(
    pipeline_output_dir='exports/monitor_hub_analysis',
    output_directory='exports/star_schema',
    incremental=False  # Use False to prevent duplicate accumulation
)
```

## Common Pitfalls
1. **Wrong conda environment**: Always activate `fabric-monitoring` or use `conda run -n fabric-monitoring`
2. **Stale star_schema data**: Delete `exports/star_schema` before rebuilding if data looks wrong
3. **Missing activity types**: `ActivityTypeDimensionBuilder.ACTIVITY_TYPES` is hardcoded - add new types there
4. **Failures showing as Unknown**: Check if activity type exists in dimension; job history types added in v0.3.8
5. **Wrong counts**: Use `('record_count', 'sum')` not `('activity_id', 'count')` - 99% of activity_ids are NULL

## Lineage Explorer (`lineage_explorer/`)

Interactive web visualization for Microsoft Fabric data lineage with Neo4j-powered graph analysis.

### Quick Start
```bash
# Start the server (loads from exports/lineage/*.csv)
make lineage-explorer
# OR: conda run -n fabric-monitoring python scripts/run_lineage_explorer.py
# OR: python -m lineage_explorer

# Optional: Start Neo4j for advanced graph analysis
cd lineage_explorer
docker compose up -d neo4j

# Open browser at http://localhost:8000
```

### Architecture
- **Backend**: FastAPI server (`server.py`) serving REST API
  - `/api/graph` - Graph data (workspaces, items, sources, edges)
  - `/api/stats` - Summary statistics
  - `/api/health` - Health check with cache status
  - `/api/refresh` - Reload CSV data
  - `/api/neo4j/*` - Neo4j graph database endpoints (24 endpoints)
- **Frontend**: Single-page app with D3.js v7 (`static/`)
  - `index.html` - Main UI with header, sidebar, graph container
  - `styles.css` - Deep space theme with glassmorphism
  - `app.js` - D3 force simulation, filters, interactions
  - `neo4j-features.js` - Neo4j-powered advanced features
  - `neo4j-features.css` - Styles for context menu, panels
- **Graph Database** (`graph_database/`):
  - `neo4j_client.py` - Connection management
  - `data_loader.py` - CSV to Neo4j graph transformation
  - `queries.py` - 20+ Cypher queries for lineage analysis
  - `docker-compose.yml` - Neo4j 5.15.0-community container

### Core Features
- **3 Layout Modes**: Force-directed (default), Radial, Tree
- **Animated Particles**: Data flow visualization along connections
- **Interactive Minimap**: Overview navigation
- **Multi-Filter Sidebar**: Workspaces, item types, source types
- **Search with Autocomplete**: Quick node finding
- **Detail Panel**: Node overview, connections, metadata tabs
- **Keyboard Shortcuts**: F=fit, +/-=zoom, /=search, Esc=deselect

### Neo4j-Powered Features (requires Neo4j running)
- **Right-Click Context Menu**: Access all analysis features from any node
- **Path Finder**: Find shortest path between any two items
- **Impact Analysis**: Show all downstream items affected by a node
- **Upstream Trace**: Show all data sources feeding into an item
- **Smart Filtering**: Filter by external source type (ADLS, S3, Snowflake)
- **Cross-Workspace View**: Highlight dependencies spanning workspaces
- **Centrality Highlighting**: Size nodes by importance score

### Neo4j Setup
```bash
# Start Neo4j container
cd lineage_explorer
docker compose up -d neo4j

# Access Neo4j Browser: http://localhost:7474
# Credentials: Set NEO4J_PASSWORD in .env

# Data is auto-loaded when server starts with Neo4j available
```

### Statistics Dashboard
Access at `http://localhost:8000/dashboard.html`:
- Summary KPIs (workspaces, items, sources, connections, tables)
- Interactive charts (items by type, sources by type)
- Workspace table with search
- External source details (ADLS, S3, SharePoint)
- Neo4j status and data loading

### Data Source
Reads from `exports/lineage/mirrored_lineage_*.csv` (generated by `extract_lineage.py`)

## Documentation Structure (`docs/`)

Organized into 7 numbered folders:
- `01_Getting_Started/` - Setup and installation
- `02_User_Guides/` - Operational guides (workspace enforcement, monitoring)
- `03_Technical_Reference/` - API comparisons, review status
- `05_Architecture_Design/` - System design specs
- `06_Historical_Reports/` - Archived compliance reports
- `07_Executive_Materials/` - Stakeholder presentations
- `08_Reference_Materials/` - Images, external references

See `docs/README.md` for navigation index.
