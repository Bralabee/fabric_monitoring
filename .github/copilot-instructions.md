# GitHub Copilot Instructions for USF Fabric Monitoring

**Version**: 0.3.8 (Validated Dec 2025) | **Library-first** Microsoft Fabric monitoring/governance toolkit.
Core logic in `src/usf_fabric_monitoring/`; scripts/notebooks are thin wrappers.

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

## Interactive Web Guide (`webapp/`)

The project includes an interactive web application for learning and utilizing the monitoring toolkit.

### Architecture
- **Backend**: FastAPI (Python) - `webapp/backend/`
  - REST API for scenarios, search, and progress tracking
  - Content stored in YAML files under `app/content/scenarios/`
  - Models in `app/models.py` (Pydantic v2)
- **Frontend**: React + TypeScript + Tailwind CSS - `webapp/frontend/`
  - shadcn/ui component patterns
  - React Query for data fetching
  - Progress tracking with local state

### Quick Start
```bash
cd webapp
make install    # Install backend + frontend dependencies
make dev        # Start both servers (backend: 8001, frontend: 5173)
```

### Development
```bash
# Backend only
cd webapp/backend && pip install -r requirements.txt
uvicorn app.main:app --reload --port 8001

# Frontend only
cd webapp/frontend && npm install && npm run dev

# Tests
cd webapp/backend && pytest tests/ -v
cd webapp/frontend && npm run build  # Type checking
```

### Content Structure
Scenario YAML files in `webapp/backend/app/content/scenarios/`:
- `01-getting-started.yaml` - Environment setup, credentials
- `02-monitor-hub-analysis.yaml` - Activity extraction, Smart Merge
- `03-workspace-access-enforcement.yaml` - Security compliance
- `04-star-schema-analytics.yaml` - Dimensional modeling
- `05-fabric-deployment.yaml` - Package build, Fabric Environment
- `06-troubleshooting.yaml` - Common issues and solutions
