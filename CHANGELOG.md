# Changelog

All notable changes to this project will be documented in this file.

## 0.3.9 (December 2025) - Spark-Compatible Parquet & Fabric Path Fixes

### Fixed
- **Critical: Parquet Timestamp Format** - Fixed `Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))` error
  - Root cause: Pandas writes timestamps with nanosecond precision by default, but Spark in Fabric only supports microsecond precision
  - Fix: Added `coerce_timestamps='us'` and `allow_truncated_timestamps=True` to all `to_parquet()` calls
  - Affected files: `star_schema_builder.py`, `pipeline.py`
  - Result: `fact_activity` parquet now converts to Delta tables without errors

- **Fabric Path Detection** - Improved path handling in Microsoft Fabric notebooks
  - Root cause: `resolve_path()` relies on `/lakehouse/default` existence check, which fails in Spark executor context
  - Fix: Added robust `is_fabric_env()` function with multiple environment indicators
  - Notebook 2A now uses explicit `/lakehouse/default/Files` paths in Fabric environment
  - Added `convert_to_spark_path()` helper for Spark-compatible relative paths

- **Notebook 1 Spark CSV Loading** - Fixed 400 Bad Request when loading CSV in Fabric
  - Root cause: `glob.glob()` returns local mount paths, but Spark needs relative paths from Lakehouse root
  - Fix: Added `convert_to_spark_path()` to transform `/lakehouse/default/Files/...` to `Files/...`

### Changed
- All parquet files now use microsecond timestamp precision for Spark/Fabric compatibility
- Notebook 2A configuration cell now explicitly detects and handles Fabric environment
- Delta table conversion cell uses Spark-compatible paths

---

## 0.3.8 (December 2025) - Job History Activity Types Fix & Smart Merge Validation

### Fixed
- **Critical: Activity Types for Failures** - Failed activities now show correct type instead of "Unknown"
  - Root cause: `ActivityTypeDimensionBuilder.ACTIVITY_TYPES` was hardcoded with only Audit Log activity types
  - Job History activity types (Pipeline, PipelineRunNotebook, Refresh, Publish, RunNotebookInteractive) were missing
  - Fix: Added 9 new activity types to dimension builder: Pipeline, PipelineRunNotebook, Refresh, Publish, RunNotebookInteractive, DataflowGen2, SparkJob, ScheduledNotebook, OneLakeShortcut

### Validated (Production Run - December 17, 2025)
- **Smart Merge Pipeline** - Complete 28-day extraction validated with fresh data:
  - Total Activities: 1,286,374
  - Failed Activities: 6,218 (correctly captured)
  - Success Rate: 99.52%
  - Workspaces: 512
  - Users: 1,506
  - Item Types: 24
  - Activity Types: 46 (12 with failures, 34 with 0 failures)

- **Failure Distribution by Activity Type**:
  | Activity Type | Succeeded | Failed | Failure % |
  |--------------|-----------|--------|----------|
  | ReadArtifact | 379,588 | 3,019 | 0.79% |
  | RunArtifact | 92,139 | 2,336 | 2.47% |
  | UpdateArtifact | 26,540 | 251 | 0.94% |
  | MountStorageByMssparkutils | 17,596 | 191 | 1.07% |
  | ViewSparkAppLog | 69,926 | 159 | 0.23% |
  | StartRunNotebook | 9,747 | 115 | 1.17% |
  | StopNotebookSession | 10,189 | 110 | 1.07% |
  | + 5 more activity types with failures |

- **Smart Merge Enrichment Stats**:
  - Activities enriched: 1,416,701
  - Missing end times fixed: 95,352
  - Duration data restored: 90,409 records
  - Total detailed jobs loaded: 15,952

### Enhanced
- **Notebook Cell 20 (Activity Type Analysis)** - Now shows both volume and failures
  - "Top 15 by Volume" table for overall activity distribution
  - "Activity Types with Failures (All)" table showing all types with failures and their counts

### Documentation
- **`.github/copilot-instructions.md`** - Comprehensive rewrite with:
  - Smart Merge algorithm documentation (how Activity Events + Job History are correlated)
  - Workspace name enrichment flow
  - Conda environment requirements (`fabric-monitoring` activation mandatory)
  - Data patterns and counting conventions (`record_count` sum vs `activity_id` count)
  - Common pitfalls and debugging guidance

---

## 0.3.7 (December 2025) - Complete Failure Dimension Coverage

### Fixed
- **Critical: Date/Time Keys for Failed Records** - Failed records now have complete dimension coverage
  - Root cause: Failed job history records have `end_time` but NULL `start_time`/`creation_time`
  - Fix: Added `end_time` fallback in `FactActivityBuilder.build_from_activities()` for date_sk/time_sk calculation
  - Result: 100% of 1,237 failed records now have valid date_sk, time_sk, workspace_sk, status_sk

- **Notebook Cell 32 (Monthly Trend)** - Fixed column name error
  - Changed `month` → `month_number` to match actual dim_date schema

### Verified
- **Consistency Check PASSED**: All dimension joins now return consistent 1,237 failures
  - After date join: 1,237 failures ✓
  - After time join: 1,237 failures ✓
  - After workspace join: 1,237 failures ✓
- Monthly breakdown: Nov 2025 = 995 failures, Dec 2025 = 242 failures
- All analytical notebook cells now show consistent failure counts

---

## 0.3.6 (December 2025) - Failure Tracking Fix

### Fixed
- **Critical: Failure Tracking Restored** - Job failures now correctly tracked after data source change
  - Root cause: Failed records from job history have `workspace_name` but NULL `workspace_id`
  - Fix: Added `workspace_name_lookup` fallback in `FactActivityBuilder` when `workspace_id` is NULL
  - Result: 1,237 failed activities now correctly mapped (was 0 due to missing workspace_sk)

### Verified
- **Failure counts by environment**: DEV=445, Unknown=721, TEST=40, UAT=31, PRD=0
- All 1,237 failed records now have valid `workspace_sk`
- Environment comparison query now shows accurate failure counts

---

## 0.3.5 (December 2025) - Data Source Correction Release

### Fixed
- **Critical Data Source Fix** - `build_star_schema_from_pipeline_output()` now loads from parquet (28 columns) instead of CSV (19 columns)
  - Priority 1: `parquet/activities_*.parquet` - Complete data with workspace_name, failure_reason, user details
  - Fallback: `activities_master_*.csv` - Limited data (legacy format)
  - **Impact**: Star schema now has full activity context including `workspace_name`, `failure_reason`, `UserId`, `UserAgent`, `ClientIP`, `source`

- **Notebook "Activity by Hour" Query** - Fixed KeyError for dim_time columns
  - Changed `hour` → `hour_24` and `period_of_day` → `time_period` to match actual schema
  - Added proper `tables` dictionary lookup for `fact_activity` and `dim_time`

### Technical
- Added module-level `logger` to `star_schema_builder.py` (was missing causing NameError)
- All notebook analytical queries now use consistent pattern: check `tables` dict, use `record_count` sum

---

## 0.3.4 (December 2025) - Workspace Name Enrichment Release

### Added
- **Workspace Name Enrichment** - `StarSchemaBuilder` now automatically enriches activity data with workspace names
  - New method `_load_workspace_lookup()` searches for workspace parquet files in common locations
  - New method `_enrich_activities_with_workspace_names()` joins workspace names to activities before dimension build
  - Constructor accepts optional `workspace_lookup_path` parameter for explicit workspace file location
  - Searches: explicit path → `exports/monitor_hub_analysis/parquet/workspaces_*.parquet` → `notebooks/monitor_hub_analysis/parquet/`

- **Expanded Notebook Analytics** - Added 8 new sample analytical queries:
  - Top 10 Most Frequently Used Items
  - Activity by Hour of Day (Peak Usage Analysis)
  - Top 10 Most Active Users
  - Activity Volume by Day of Week
  - Environment Comparison (DEV vs TEST vs UAT vs PRD)
  - Item Category Distribution
  - Long-Running Activities Analysis
  - Cross-Workspace Usage Patterns
  - Monthly Trend Analysis

### Fixed
- **"Top 10 Most Active Workspaces"** query now shows actual workspace names instead of "Unknown"
  - Root cause: Source CSV only had `workspace_id` GUID, not `workspace_name`
  - Solution: Auto-enrichment joins workspace names from separate workspace extraction
  - Result: 961,773 of 968,766 activities (99%) now have proper workspace names

- **Query Logic Fix** - Changed aggregation pattern to aggregate by SK first, then join for names
  - Previous approach caused duplicate rows due to direct merge before groupby
  - New approach: `groupby('workspace_sk')` → `merge(dim_ws)` for accurate counts

### Verified
- `dim_workspace` now has 158 records with actual workspace names (was 159 all "Unknown")
- Environment inference working correctly (DEV, TEST, UAT, PRD patterns detected)
- 24 workspaces still "Unknown" (deleted workspaces or cross-tenant references)

---

## 0.3.3 (December 2025) - Data Cleanup & Verification Release

### Fixed
- **Data Cleanup** - Removed obsolete CSV files without Smart Merge failure data
  - Kept only timestamp `20251203_212443` files which contain correct failure tracking
  - Cleaned up parquet and job history files to match
  - Resolved issue where incremental loads were mixing old (no failures) with new data

- **Star Schema Verification** - Confirmed correct failure capture after full refresh
  - 1,238 failures correctly tracked in `fact_activity` table
  - Daily metrics now show accurate failure counts per day
  - Success rate calculations working correctly (99.94% overall)

### Verified Working
- Smart Merge pipeline correctly enriches activities with job failure status
- Star Schema Builder derives `is_failed` from `status` column (Failed/Cancelled/Error → 1)
- Notebook auto-selects CSV file with failures when multiple available
- Daily Activity Trend shows proper failure counts

---

## 0.3.2 (December 2025) - Coverage Expansion Release

### Fixed
- **Expanded Activity Type Coverage** - `ACTIVITY_TYPES` dictionary now covers 61 activity types (was 19)
  - Achieved 100% coverage of observed activity types in tenant data
  - Added categories: Spark Operations, Lakehouse Operations, ML Operations, and more
  - Eliminates "Unknown" activity type classification for known operations

- **Expanded Item Type Coverage** - All item type dictionaries now cover 100% of observed types
  - `ITEM_CATEGORIES` in star_schema_builder.py: Added Pipeline, SynapseNotebook, DataFlow, CopyJob, KustoDatabase, SnowflakeDatabase, MLExperiment, Dataset, Datamart
  - `fabric_types` in pipeline.py: Added same expanded set for Fabric activity detection
  - `type_map` in enrichment.py: Expanded URL builder to support 20+ Fabric item types
  - `SUPPORTED_JOB_ITEM_TYPES` in extract_fabric_item_details.py: Added Pipeline, SynapseNotebook, DataFlow, CopyJob, Dataset

### Investigated (Not Bugs)
- **"Unknown" workspace_name values** - 968 records from deleted workspaces or cross-tenant activities
- **"Unknown" submitted_by values** - 65,796 system-initiated activities (ViewSparkAppLog, CreateCheckpoint, etc.)
- **NULL submitted_by values** - 29,176 scheduled pipeline/Snowflake runs without user identity

---

## 0.3.1 (December 2024) - Smart Merge Fix Release

### Fixed
- **Star Schema Builder** now correctly reads from CSV files with Smart Merge enriched data
  - Changed data source from parquet to `activities_master_*.csv` files
  - Fixed NaN handling in `classify_user_type()`, `extract_domain_from_upn()`, and `build_from_activities()` 
  - Fixed NaT datetime handling in fact table building
  - Failure data (is_failed=1) now properly captured from Smart Merge correlation

### Changed
- Notebook auto-selects CSV file with failure data when multiple files available
- Added Failure Analysis cell to notebook for Smart Merge validation

---

## 0.3.0 (January 2025) - Star Schema Analytics Release

### Added
- **Star Schema Builder** - Kimball-style dimensional model for analytics
  - 7 dimension tables (dim_date, dim_time, dim_workspace, dim_item, dim_user, dim_activity_type, dim_status)
  - 2 fact tables (fact_activity, fact_daily_metrics)
  - Incremental loading with high-water mark tracking
  - SCD Type 2 support for slowly changing dimensions
  - Delta Lake DDL generation for Fabric deployment
  - Validated with 1M+ activity records
- New CLI entry point: `usf-star-schema`
- Makefile targets: `star-schema`, `star-schema-ddl`, `star-schema-describe`
- Fabric deployment guide (`docs/FABRIC_DEPLOYMENT.md`)
- Ready-to-use Fabric notebook (`notebooks/Fabric_Star_Schema_Builder.ipynb`)

### Changed
- Updated documentation to reflect v0.3.0 features
- Enhanced README with star schema feature documentation
- Updated WIKI with star schema notebook guide

## 0.2.0 (December 2024) - Advanced Analytics Release

### Added
- Monitor Hub analysis pipeline with Smart Merge duration recovery
- Workspace access enforcement CLI (`assess`/`enforce`) with targets + suppressions
- Lineage extraction inventory for Mirrored Databases
- Safe-by-default pytest configuration (integration tests opt-in)
- CONTRIBUTING.md with development guidelines
- SECURITY.md with security policy
