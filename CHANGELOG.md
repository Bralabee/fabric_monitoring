# Changelog

All notable changes to this project will be documented in this file.

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
