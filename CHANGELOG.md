# Changelog

All notable changes to this project will be documented in this file.

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
