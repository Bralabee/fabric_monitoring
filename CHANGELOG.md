# Changelog

All notable changes to this project will be documented in this file.

## 0.3.33 (February 2026) - Mirrored Table Search Fix & Neo4j Data Reload

### Fixed

- **MERGE_TABLE Cypher Key Mismatch** (`lineage_explorer/graph_database/data_loader.py`):
  - `MERGE_TABLE` query referenced `row.schema_name` but the Python dict uses key `schema`
  - Caused mirrored table schema to be `null` in Neo4j, breaking search results
  - Tables like `EDW_DATA/DIM_POSITION_HIERARHCY` were invisible in Table Impact Analysis
  - Now also persists `table_type`, `status`, `processed_rows`, and `last_sync` metadata
  - Added `ON MATCH SET` to update status/rows on subsequent reloads

### Verified

- Neo4j graph now contains:
  - 2,444 FabricItem nodes
  - 1,814 mirrored table nodes + 1,798 shortcut table nodes (3,612 total)
  - 153 Workspace nodes
  - 103 ExternalSource nodes
  - 3,586 MIRRORS edges, 2,167 shortcut table edges
- `DIM_POSITION_HIERARHCY` (EDW_DATA) now searchable across DEV, DEV_2, PRD, UAT environments
- Table Impact Analysis search (`/api/tables/search`) returns all 4 instances

---

## 0.3.32 (February 2026) - MirroredDatabase Table Extraction & Neo4j Health Fix

### Added

- **MirroredDatabase Table Extraction** (`scripts/extract_lineage.py`):
  - New `get_mirrored_tables()` method calling Fabric `getTablesMirroringStatus` API
  - Captures all tables within MirroredDatabases with schema, status, and sync metrics
  - Includes `Mirrored Tables` field in lineage output (82 records with table lists)
  - Resolves issue where tables like `DIM_POSITION` were not being captured

- **Data Loader Enhancement** (`lineage_explorer/graph_database/data_loader.py`):
  - Parses new `Mirrored Tables` field from extraction output
  - Creates Table nodes with schema, table name, status, and sync metadata

### Fixed

- **Neo4j Docker Health Check** (`lineage_explorer/docker-compose.yml`):
  - Changed health check from `curl` to `wget -qO-`
  - Root cause: `curl` is not available in `neo4j:5.15.0-community` image
  - Neo4j container now correctly reports "healthy" status

### Verified

- Neo4j graph now contains:
  - 2,427 FabricItem nodes
  - 1,801 Table nodes (including DIM_POSITION)
  - 152 Workspace nodes
  - 102 ExternalSource nodes

---

## 0.3.31 (January 2026) - Specialized Graph Views

### Added

- **Tables Graph** (`/tables_graph.html`):
  - Dedicated visualization for physical tables and their parent items
  - Source type filtering (Lakehouse vs Snowflake)
  - Relationship type filtering (Uses, Provides, Mirrors)
  - Click table node → opens Table Impact Dashboard
  - Node limit: ~2000 (optimized for tables + parent items only)

- **Elements Graph** (`/elements_graph.html`):
  - Dedicated visualization for logical Fabric items (Lakehouses, Semantic Models, Reports)
  - Item type filtering with dynamic chips
  - External sources toggle for data dependencies
  - Workspace grouping visualization
  - Node limit: ~2500

- **Navigation Updates**:
  - Added "Tables" and "Elements" links to main navigation
  - Updated Dashboard, Index, and Impact pages with consistent nav

### Performance

- Addresses "SVG Wall" limitation by splitting graph into focused views
- Each specialized view stays within browser rendering limits
- Main graph remains the comprehensive overview with 500-node guardrail

---

## 0.3.30 (January 2026) - Enhanced Lineage Data Loading (Opt-In)

### Added

- **Opt-In Enhanced Item Loading** (`/api/neo4j/load?include_all_items=true`):
  - New query parameter `include_all_items` (default: `false`) for loading ALL extracted items
  - When enabled, creates `READS_FROM` edges linking Reports to their source SemanticModels
  - Addresses ingestion gap: 2,427 items extracted vs ~200 items loaded (with default)
  - **Backward compatible**: Default behavior unchanged; this is purely opt-in

### Technical Details

- Added `CREATE_READS_FROM_EDGE` Cypher query to `data_loader.py`
- Reports with `PowerBI` source type are linked to their dataset (SemanticModel) via `Connection ID`
- 811 Report→SemanticModel edges available when opt-in enabled

### Usage

```bash
# Default (unchanged behavior)
curl -X POST "http://localhost:8000/api/neo4j/load?clear_existing=true"

# Opt-in for enhanced loading
curl -X POST "http://localhost:8000/api/neo4j/load?clear_existing=true&include_all_items=true"
```

### Performance Optimizations

- **Large Graph Support** (2,400+ nodes):
  - Barnes-Hut approximation (theta=0.9) reduces force calculation from O(n²) to O(n log n)
  - Faster alpha decay (0.05) for quicker simulation convergence
  - Throttled tick rendering (skip frames for >500 nodes)
  - Disabled expensive blur glow effects for large graphs
  - Reduced force strengths and earlier simulation stop (alphaMin=0.01)

### Fixed

- Handle list-type `Source Connection` values (multi-source SemanticModels)

---

## 0.3.29 (January 2026) - Table Impact Graph Revamp

### Added

- **Table Layer Toggle** in Main Graph Explorer:
  - Diamond-shaped table nodes overlaid on the force-directed graph
  - Toggle button in toolbar to show/hide table layer
  - Color-coded by source: Lakehouse (green), Snowflake (purple)
  - Click table node to open Table Impact Dashboard
  - Dashed edges show item-to-table relationships

- **New API Endpoints** (`lineage_explorer/api_extended.py`):
  - `GET /api/tables/overview` - Aggregated table statistics with pattern analysis
  - `GET /api/items/{item_id}/table-footprint` - Compact table footprint for tooltips
  - `GET /api/graph/with-tables` - Full graph data with table layer for visualization

- **Table Pattern Detection**:
  - **Orphan Tables**: Tables with no consumers (data quality issue)
  - **High-Dependency Tables**: Tables with 5+ consumers (high impact if changed)
  - **Cross-Workspace Tables**: Tables used across multiple workspaces

### Enhanced

- **State Management**: Added `tableLayerEnabled` and `tableLayerData` properties
- **CSS Styling**: Table layer nodes with diamond shapes and color-coded source types

---

## 0.3.28 (January 2026) - Item (Lakehouse) Search Enhancement

### Added

- **Dual Search Mode**: Search for Tables OR Items (Lakehouses/MirroredDBs)
  - Radio button toggle to switch between search modes
  - Searching "share_gold" now finds SHARE_GOLD Lakehouse items
  
- **Item Impact Analysis**:
  - `GET /api/items/search?q={name}` - Search Fabric items by name
  - `GET /api/items/impact?id={id}` - Full item impact analysis
  - Visualization shows:
    - **Tables Used**: Tables the Lakehouse consumes (via USES_TABLE)
    - **Tables Provided**: Tables the Lakehouse hosts (via PROVIDES_TABLE)
    - **Downstream Items**: Items that depend on this Lakehouse

### Fixed

- **Table IDs with Slashes**: Query-based API endpoint handles special characters
  - New endpoint: `GET /api/tables/impact?id={tableId}` (preferred)
  - Fixes 404 errors for tables like `ENR_SNOW_SERV/RBS_SHARED_SERVICES`

### Enhanced

- **Search Type Toggle**: Automatic re-search when mode changes
- **Item Results Display**: Shows workspace, type badge, table count
- **Keyboard Navigation**: Works for both Tables and Items

---

## 0.3.27 (January 2026) - Table-Level Impact Analysis Dashboard

### Added

- **Table Impact Analysis Dashboard** (`lineage_explorer/static/table_impact.html`):
  - New standalone page for source table impact analysis
  - Search tables by name (partial match) with consumer counts
  - Visual impact tree showing downstream dependencies
  - Stats cards: Matching tables, direct consumers, downstream items, total impact
  - CSV export for impact analysis results
  - Integrated navigation across all dashboard pages

- **New API Endpoints** (`lineage_explorer/api_extended.py`):
  - `GET /api/tables/search?q={name}` - Search tables with consumer counts
  - `GET /api/tables/{table_id}/impact` - Full downstream impact analysis
  - `GET /api/tables` - List all tables with pagination

- **Impact Analysis Features**:
  - Recursive downstream traversal: Table → Items → Downstream Items
  - Depth-based impact visualization (up to 10 levels)
  - Path tracking through dependency chains
  - Direct consumer vs downstream separation

### Enhanced

- **Table Impact Dashboard UX**:
  - Debounced live search (auto-search after 300ms, min 2 chars)
  - Keyboard navigation (↑/↓ arrows, Enter to select)
  - Collapsible tree nodes with Expand/Collapse All buttons
  - Search result caching for faster repeated queries
  - Improved error handling with detailed messages
  - Proper CSV escaping (handles commas, quotes, newlines)
  - Additional item type icons (Dataflow, Warehouse)
  - Friendly type formatting (SemanticModel → Semantic Model)
  - **Source Type Badges**: Green=Lakehouse (1,760 tables), Purple=Snowflake (42 tables)
  - **Location Column**: Shows database.schema or Tables/path for context
  - **Impact Tree Root**: Displays source type badge and full path

### Fixed

- Added explicit `/table_impact.html` route to `server.py`
- API now accepts table name OR table ID for lookup

### Changed

- **Dashboard Navigation** (`dashboard.html`):
  - Added "Table Impact" link to navigation bar

### Important Notes
>
> **Server Restart Required**: After modifying `server.py` or `api_extended.py`,
> the Lineage Explorer server must be restarted to load new routes.

---

## 0.3.26 (January 2026) - Extended Lineage Extraction

### Added

- **Extended Lineage Extraction** - Now captures 6 item types (up from 3):
  - **SemanticModel**: Datasources, source types, connection details
  - **Dataflow**: Datasources with connection details
  - **Report**: Upstream dataset binding, cross-workspace flag

- **New Extraction Methods** (`scripts/extract_lineage.py`):
  - `get_semantic_models()` - Fetch datasets/models in workspace
  - `get_dataflows()` - Fetch dataflows in workspace
  - `get_reports()` - Fetch reports in workspace
  - `get_item_connections()` - Fetch item-level connections
  - `get_dataset_datasources()` - Power BI dataset sources
  - `get_dataset_tables()` - Dataset table metadata
  - `get_report_definition()` - Report details with dataset binding
  - `get_dataflow_datasources()` - Dataflow sources

- **ItemConnectionsExtractor Module** (`core/item_connections.py`):
  - Standalone module for Fabric `/connections` endpoint
  - Power BI `/datasources` endpoint support
  - Factory function `create_item_connections_extractor()`

- **AdminScannerClient Enhancements** (`core/admin_scanner.py`):
  - `datasetSchema=True` and `datasetExpressions=True` by default
  - Extended `normalize_lineage_results()` for richer metadata
  - Captures table/column counts, expressions, upstream datasets

### Changed

- **Iterative LineageExtractor** now captures rich data for all item types:
  - SemanticModel: Datasource count, source types, connection IDs
  - Dataflow: Datasource count, connection details
  - Report: Bound dataset ID, cross-workspace dependencies
- **Lineage Summary** now shows counts for all 6 item types

### Technical Details

- All 6 item types now have parity with rich data extraction
- Reports capture `Cross Workspace` flag for cross-workspace dataset dependencies
- Semantic Models show datasource types (SQL, AzureBlob, etc.)

---

## 0.3.25 (January 2026) - Table Lineage Panel & Frontend UX

### Added

- **Table Lineage Panel** (`lineage_explorer/static/index.html`):
  - New side panel accessible via toolbar button (table icon)
  - Displays all Fabric items (Lakehouses, Warehouses, Notebooks, Shortcuts)
  - Stats display: Items, Shortcuts, Total counts
  - Search functionality: Filter tables by name, database, or path
  - Table cards with item details, type badges, and workspace info

- **Node-Based Table Filtering**:
  - Click a graph node → Table panel filters to related items
  - "Filtered by: [node name]" indicator shows active filter
  - Clear filter via X button in indicator bar
  - Automatic filtering on node selection, clearing on deselection

- **Graph-Table Integration**:
  - `Graph.selectNode()` now triggers `TableLineage.filterByNode()`
  - `Graph.deselectNode()` now triggers `TableLineage.clearFilter()`
  - Click table card to focus corresponding node in graph

### Changed

- **TableLineage Module**: Uses graph state data directly instead of API calls
  - Derives table data from `state.graph.nodes`
  - Includes items with types: Lakehouse, Warehouse, Dataflow, Notebook, Shortcut
  - Includes external sources (Snowflake, OneLake) for complete lineage view

### Technical Details

- CSS additions: ~200 lines for panel, cards, filter indicator, stats
- JavaScript module: `TableLineage` with init/load/filter/search/render
- HTML additions: Panel structure, search input, stats grid, cards container
- Toolbar: New toggle button with table grid icon

---

## 0.3.24 (January 2026) - Dashboard Enrichment & Lineage Automation Release

### Added

- **Enhanced Dashboard Statistics** (`lineage_explorer/static/dashboard.html`):
  - 4 new stat cards: Internal Deps (172), External Deps (115), Snowflake (38), OneLake (54)
  - Total dashboard now shows 9 KPI metrics at a glance
  - **Tables by Database drill-down** - Click to expand and see table names with schema info
  - Neo4j stats integration for enriched dependency counts

- **Impact Analysis Query Category** (`lineage_explorer/static/query_explorer.html`):
  - 5 new governance-focused queries:
    - Downstream Impact Analysis - Find items with highest downstream dependencies
    - External Source Vulnerability - Sources that are single points of failure
    - Workspace Impact Matrix - Cross-workspace failure impact assessment
    - Critical Path Analysis - Longest/most fragile dependency chains
    - Snowflake Source Coverage - Track Snowflake data consumers

- **6 New API Endpoints** (`lineage_explorer/api_extended.py`):
  - `GET /api/neo4j/tables-by-database/{database}` - Tables in a database with schema info
  - `GET /api/neo4j/items-by-workspace/{name}` - List items in a workspace
  - `GET /api/neo4j/external-sources-by-type/{type}` - Sources by type with consumers
  - `GET /api/neo4j/lineage-chain/{id}` - Full upstream + downstream chain
  - `GET /api/neo4j/schema-coverage` - Tables by schema with consumer counts
  - `GET /api/neo4j/dependency-depth` - Items ranked by chain depth

- **Lineage Extraction Automation** (`Makefile`):
  - `make extract-lineage-full` - Iterative mode extraction with shortcuts (rich visualization data)
  - `make lineage-full` - Full end-to-end workflow (extract → Neo4j → explorer)

### Changed

- **Development Workflow** (`.agent/workflows/development.md`):
  - Documented critical difference between `scanner` and `iterative` extraction modes
  - Added file naming patterns and priority documentation
  - Added Neo4j integration instructions

### Documentation

- Updated Lineage Explorer API docs with new dashboard features
- Query Explorer now has 10 categories with 45+ pre-built queries

---

## 0.3.23 (January 2026) - Robustness & Defensive Data Handling Release

### Added

- **JSON Schema Validation System** (`config/schemas/`):
  - `inference_rules.schema.json` - Validates domain/location keyword structure
  - `workspace_access_targets.schema.json` - Validates security principals with UUID pattern
  - `workspace_access_suppressions.schema.json` - Validates suppression lists
  - All schemas use Draft-07 with descriptive error messages

- **Type Safety Utilities** (`core/type_safety.py`):
  - `safe_int64()` - Nullable Int64 conversion handling NaN/None (fixes Direct Lake Float64 issues)
  - `coerce_surrogate_keys()` - Enforces Int64 for all `*_sk` columns
  - `safe_datetime()` - Multi-format datetime parsing with fallbacks
  - `safe_workspace_lookup()` - Fallback chain for workspace resolution (ID → Name → default)
  - `microsecond_timestamps()` - Truncates nanoseconds for Spark compatibility
  - Plus 8 additional utility functions for defensive data handling

- **Environment Detection** (`core/env_detection.py`):
  - `detect_environment()` - Returns LOCAL, FABRIC_NOTEBOOK, FABRIC_PIPELINE, or AZURE_DEVOPS
  - `is_fabric_environment()` - Quick check for Fabric context
  - `get_default_output_path()` - Environment-aware path resolution
  - `convert_to_spark_path()` - Local mount to Spark-compatible path conversion

- **Enhanced Test Infrastructure**:
  - `conftest.py` - 9 shared fixtures for sample data, configs, and mock API responses
  - `test_config_schemas.py` - 14 tests for schema validation
  - `test_type_safety.py` - 29 tests covering type coercion edge cases
  - Total test count: 87 (previously ~45)

### Changed

- **Enhanced `config_validation.py`**:
  - Added `ConfigValidationError` exception class with detailed error messages
  - Added `validate_all_configs()` for project-wide validation
  - Added `load_schema_file()` to load external JSON schemas
  - Added `print_validation_report()` for human-readable output
  - Maintains backward compatibility with inline schemas as fallback

- **CI/CD Enhancement** (`.github/workflows/ci.yml`):
  - Added config validation step before test execution
  - Config validation failures now block CI

### Fixed

- **`.gitignore`**: Added patterns for Neo4j data, coverage reports, IDE settings, Fabric paths

### Documentation

- Updated `copilot-instructions.md` with type safety patterns
- Updated `PROJECT_ANALYSIS.md` with resolved gaps

---

## 0.3.22 (January 2026) - Project Structure Refinement & Documentation

### Changed

- **Industry-Standard `src` Layout**: Restructured project to follow Python packaging best practices
  - CLI scripts moved from `scripts/` to `src/usf_fabric_monitoring/scripts/`
  - Removed `sys.path` hacks from all entry point scripts
  - Entry points now correctly resolve via installed package

- **README Enhancement**: Added comprehensive contributor guide
  - Directory explanations table with "When to Modify" guidance
  - FAQ section explaining why `lineage_explorer/` is outside `src/`
  - CLI entry points reference table

### Fixed

- **Broken CLI Entry Points**: All `usf-*` commands now work correctly
  - `usf-monitor-hub`, `usf-extract-lineage`, `usf-enforce-access`, `usf-star-schema`, `usf-validate-config`
  - Makefile updated to use `python -m usf_fabric_monitoring.scripts.*` invocation

- **Documentation Gaps**:
  - Fixed wrong repo URL in CONTRIBUTING.md (`usf_fabric_cicd_codebase` → `fabric_monitoring`)
  - Added Admin Scanner API section to `.env.template`
  - Updated `lineage_explorer/README.md` to document JSON+CSV support

### Added

- **Workflow Documentation** (`.agent/workflows/`):
  - `add-cli-entry-point.md` - Procedure for adding new CLI commands
  - `development.md` - Standard development workflow

---

## 0.3.21 (January 2026) - Lineage Explorer Advanced Selection Features

### Added

- **Multi-Node Selection** (`lineage_explorer/static/index.html`):
  - Ctrl+Click (Cmd+Click on Mac) to add/remove nodes from selection
  - Selection count badge showing number of selected nodes
  - Clear selection button in toolbar
  - Supports selecting multiple nodes for batch connection analysis
  
- **Children/Descendants Selection Buttons**:
  - "Children" button: Select all direct children (1 level downstream) of currently selected nodes
  - "Descendants" button: Select all descendants (up to 10 levels downstream)
  - Auto-updates selection count badge
  - Works seamlessly with existing multi-selection system
  
- **Directional Arrow Coloring**:
  - Inward arrows (incoming data): Green (#10b981) - identifies data consumers
  - Outward arrows (outgoing data): Blue (#3b82f6) - identifies data producers
  - Visual distinction makes data flow direction instantly recognizable
  - Applied when hovering or clicking on nodes

### Changed

- **Selection System Refactor**: Replaced single `selectedNode` with `selectedNodes` Set for multi-select support
- **Detail Panel Behavior**: Now hides when multiple nodes are selected (shows only for single selection)
- **Connection Highlighting**: Updated `highlightConnections()` to support multiple selected nodes simultaneously

### Fixed

- **Multi-Select Arrow Colors**: Maintains directional arrow coloring (green/blue) when multiple nodes selected
- **Hover Behavior**: Only shows hover highlight when no nodes are selected (prevents visual conflicts)

---

## 0.3.20 (January 2026) - Lineage Explorer UI/UX Enhancements

### Added

- **Query Explorer Page** (`lineage_explorer/static/query_explorer.html`):
  - 10 query categories (Overview, Workspaces, Items, Dependencies, External Sources, Tables, Semantic Models, Reports, Security, Advanced)
  - ~40 pre-built Neo4j Cypher queries with descriptions
  - Interactive query execution with real-time results display
  - KPI cards showing Total Workspaces, Items, External Sources, Tables
  - Dual view modes: Table view (sortable columns) and Chart view (visualizations)
  - Export functionality: Download results as CSV or JSON
  - Category filtering for easy query discovery

### Changed

- **Unified Light Theme**: Converted all three pages to consistent light theme styling
  - Background: `#f8fafc` (light gray)
  - Text: `#0f172a` (navy)
  - Accent: `#3b82f6` (blue)
  - Replaced "deep space" dark theme with professional light theme
- **Navigation Component**: Added consistent navigation header to all pages
  - Graph icon → Main visualization (`/`)
  - Chart icon → Statistics dashboard (`/dashboard.html`)
  - Search icon → Query explorer (`/query_explorer.html`)
- **Persistent Node Selection**: Graph view now keeps clicked nodes highlighted with blue ring
- **Dashboard Theme Update**: Converted from Tailwind dark classes to custom CSS light theme
- **Chart.js Theme**: Updated default colors from dark (`#94a3b8`) to light (`#475569`)

### Fixed

- **CSS Variable Consistency**: All pages now use the same CSS custom properties
- **JavaScript Generated HTML**: Removed Tailwind classes from all dynamically generated elements
- **Navigation State**: Active page link properly highlighted in navigation

### Documentation

- **lineage_explorer/README.md**: Updated with Query Explorer features, light theme description, and navigation guide
- **CHANGELOG.md**: Documented all UI/UX improvements

---

## 0.3.19 (January 2026) - Neo4j-Powered Interactive Graph Analysis

### Added

- **Neo4j Graph Database Integration** (`lineage_explorer/graph_database/`):
  - `neo4j_client.py` - Connection management with retry logic and connection pooling
  - `data_loader.py` - Transforms lineage CSV data into Neo4j graph model
  - `queries.py` - 20+ pre-built Cypher queries for lineage analysis
  - `docker-compose.yml` - Neo4j 5.15.0-community container configuration

- **Extended API Endpoints** (`lineage_explorer/api_extended.py`):
  - `/api/stats/detailed` - Comprehensive statistics with all breakdowns
  - `/api/stats/workspaces` - Per-workspace item counts and dependencies
  - `/api/stats/external-sources` - External source breakdown (ADLS, S3, Snowflake, SharePoint)
  - `/api/stats/tables` - MirroredDatabase table statistics by database/schema
  - `/api/neo4j/health` - Neo4j connection status check
  - `/api/neo4j/load` - Load lineage data into Neo4j
  - `/api/neo4j/upstream/{id}` - Find all upstream dependencies
  - `/api/neo4j/downstream/{id}` - Impact analysis (downstream dependents)
  - `/api/neo4j/path` - Find shortest path between two items
  - `/api/neo4j/search` - Search items by name/type
  - `/api/neo4j/centrality` - Calculate node importance scores
  - `/api/neo4j/cross-workspace` - Cross-workspace dependency analysis
  - `/api/neo4j/source-type/{type}` - Items consuming specific source types

- **Neo4j-Powered D3.js Frontend Features** (`lineage_explorer/static/`):
  - `neo4j-features.js` - 650+ lines of Neo4j integration code
  - `neo4j-features.css` - Styles for context menu, panels, and node states
  - **Right-Click Context Menu**: Show Upstream Sources, Show Downstream Impact, Set as Path Source/Target, Focus on Node, Copy Node ID
  - **Path Finder Panel**: Select source/target nodes, visualize connection paths with step-by-step display
  - **Impact Analysis Panel**: Shows all downstream items affected by changes to selected node
  - **Upstream Trace Panel**: Displays all data sources feeding into selected item
  - **Smart Filtering**: Dropdown to filter items by external source type (ADLS, S3, Snowflake)
  - **Cross-Workspace Highlighting**: Button to highlight dependencies spanning workspaces
  - **Centrality Highlighting**: Button to resize/color nodes by importance score
  - **Neo4j Status Indicator**: Shows connection status in header

- **Statistics Dashboard** (`lineage_explorer/static/dashboard.html`):
  - Summary KPI cards (workspaces, items, sources, connections, tables)
  - Interactive Chart.js visualizations (items by type, sources by type)
  - Workspace table with search and filtering
  - External source details (ADLS containers, S3 buckets, SharePoint sites)
  - Table breakdown by database/schema
  - Neo4j status indicator and data loading button

### Changed

- **Server Integration**: Auto-initializes Neo4j on startup if available
- **app.js**: Exported functions for Neo4j features integration (`state`, `truncate`, `getNodeColorRaw`, `renderGraph`)

### Fixed

- **Neo4j Query Syntax**: Fixed parameterized path lengths in Cypher queries (Neo4j doesn't support `*1..$var` in shortestPath)

### Documentation

- Updated `docs/02_User_Guides/Lineage_Explorer_API.md` with all new endpoints
- Updated `.github/copilot-instructions.md` with Neo4j architecture and usage

---

## 0.3.18 (January 2026) - Enhanced Lineage Explorer & Documentation Overhaul

### Added

- **Interactive Lineage Explorer** (`lineage_explorer/`):
  - FastAPI backend serving graph data from CSV lineage exports
  - D3.js v7 force-directed graph visualization with 3 layout modes (force/radial/tree)
  - Animated particle flow along data connections
  - Interactive minimap for navigation
  - Advanced search with autocomplete dropdown
  - Multi-filter sidebar (workspaces, item types, source types)
  - Detail panel with tabs (overview/connections/metadata)
  - Keyboard shortcuts (F=fit, +/-=zoom, /=search, Esc=deselect)
  - Deep space glassmorphism design with CSS custom properties
  - Responsive design with mobile breakpoints
- **Launcher Script**: `run_lineage_explorer.py` for quick server startup
- **Makefile Target**: `make lineage-explorer` to run the visualization

### Changed

- **Documentation Reorganization**: Restructured `/docs` into 8 numbered folders:
  - `01_Getting_Started/` - Setup and installation guides
  - `02_User_Guides/` - Workspace enforcement, tenant monitoring, deployment
  - `03_Technical_Reference/` - API comparisons, review status
  - `04_API_Guides/` - Placeholder for API documentation
  - `05_Architecture_Design/` - Lineage explorer specs, visualization architecture
  - `06_Historical_Reports/` - Compliance reports, archived reviews
  - `07_Executive_Materials/` - Stakeholder presentations
  - `08_Reference_Materials/` - Images, external references
- **docs/README.md**: New navigation index with links to all documentation sections

### Fixed

- **Loading Overlay ID Mismatch**: Fixed `id="loading"` → `id="loading-overlay"` in lineage explorer HTML

### Removed

- **webapp/** directory: Removed legacy React/FastAPI tutorial webapp (functionality moved to lineage_explorer)

---

## 0.3.17 (January 2026) - Lineage & Visualization Deep Dive

### Added

- **OneLake Shortcut Extraction**: `extract_lineage.py` now scans Lakehouses and KQL Databases for Shortcuts to generic external sources (ADLS Gen2, S3) and internal OneLake paths.
- **Deep Dive Visualization Dashboard**: Completely rewritten `visualize_lineage.py` using Plotly and NetworkX.
  - **Topology Network**: Force-directed graph to visualize workspace clustering around data sources.
  - **Sankey Diagram**: Flow analysis from Workspace to External Source.
  - **Inverted Treemap**: Reverse lineage lookup (Source -> Consumer).
- **Makefile Targets**: Added `extract-lineage` and `visualize-lineage` commands.

### Changed

- **Unified Lineage Schema**: Consolidated Mirrored Database and Shortcut outputs into a single standard CSV format.

---

## 0.3.16 (December 2025) - Clean Release

### Removed

- **Removed capacity enrichment entirely** - No API calls added to pipeline
- Removed `_enrich_workspaces_with_capacity()` method
- Removed `ENABLE_CAPACITY_ENRICHMENT` and `SKIP_CAPACITY_ENRICHMENT` env vars

### Fabric vs Power BI Filtering

The `platform` and `is_fabric_native` columns in `dim_item` use existing `item_type` data:

- No additional API calls
- Based purely on item_type classification
- Filter in Power BI: `dim_item[platform] = "Fabric"`

---

## 0.3.15 (December 2025) - No Breaking Changes

### Fixed

- **Reverted capacity enrichment to OPT-IN** - The v0.3.12-0.3.14 capacity enrichment was causing pipeline failures
  - **Default behavior is now unchanged from v0.3.11** - no additional API calls
  - To enable capacity enrichment, set `ENABLE_CAPACITY_ENRICHMENT=1` before running pipeline
  - This ensures backward compatibility with existing working pipelines

### How to Enable Capacity Enrichment (Optional)

```python
import os
os.environ["ENABLE_CAPACITY_ENRICHMENT"] = "1"
# Then run pipeline
```

---

## 0.3.14 (December 2025) - Capacity Enrichment Resilience

### Fixed

- **Capacity enrichment error handling** - Made `_enrich_workspaces_with_capacity()` more resilient
  - Added `SKIP_CAPACITY_ENRICHMENT=1` environment variable to disable if causing issues
  - Better logging when API calls fail
  - Graceful fallback to basic workspace data if enrichment fails
  - Separate handling for ImportError vs general exceptions

### Note

If you see blobfuse/mount errors in Fabric after upgrading to v0.3.12+, try:

1. Set `SKIP_CAPACITY_ENRICHMENT=1` in notebook before running pipeline
2. Or ensure the Admin API is accessible from your Fabric workspace

---

## 0.3.13 (December 2025) - Fabric vs Power BI Platform Classification

### Added

- **Platform Classification** - New columns in `dim_item` to distinguish Fabric from Power BI items:
  - `platform`: 'Fabric' or 'Power BI' - easy to filter in reports
  - `is_fabric_native`: Boolean flag for Fabric-only items
  
- **Item Type Classification**:
  - `FABRIC_NATIVE_TYPES`: Lakehouse, Warehouse, Notebook, DataPipeline, Eventstream, KQLDatabase, etc.
  - `POWERBI_TYPES`: Report, Dashboard, Dataset/SemanticModel, Dataflow, Datamart

### Use Cases

- Filter failure analysis to Fabric-only items: `WHERE platform = 'Fabric'`
- Exclude Power BI reports from compute metrics: `WHERE is_fabric_native = TRUE`
- Analyze by platform: `GROUP BY platform`

---

## 0.3.12 (December 2025) - Capacity ID Enrichment

### Fixed

- **Capacity ID Missing from Star Schema** - Fixed `capacity_id` being NULL for all workspaces in `dim_workspace`
  - Root cause: Pipeline was extracting workspace data from activities (which don't have `capacityId`), not from Power BI Admin API
  - Fix: Added `_enrich_workspaces_with_capacity()` to pipeline.py that fetches full workspace metadata from Power BI Admin API `/admin/groups` endpoint
  - Updated `star_schema_builder.py` to load full workspace data (including `capacityId`) from enriched parquet files
  - Result: `dim_workspace.capacity_id` now populated for all workspaces on dedicated capacity

### Changed

- `_load_workspace_lookup()` now returns full workspace data dict instead of just name mapping
- `_enrich_activities_with_workspace_names()` now also enriches `capacity_id` from workspace lookup
- Workspaces parquet now includes: `id`, `displayName`, `capacityId`, `type`, `state`, `isOnDedicatedCapacity`

### Technical Details

- Power BI Admin API `/admin/groups` returns `capacityId` for each workspace
- Activities API returns only `workspace_id` and `workspace_name` (no capacity info)
- The enrichment happens during pipeline execution, ensuring fresh capacity assignments

---

## 0.3.11 (December 2025) - Datetime Parsing Warning Fix

### Fixed

- **Pandas Datetime Warning** - Fixed `UserWarning: Could not infer format` in csv_exporter.py
  - Root cause: `pd.to_datetime()` without explicit format triggers dateutil fallback warning
  - Fix: Added explicit `format='ISO8601'` with fallback to `format='mixed'` for datetime column parsing
  - Result: No more warnings when processing datetime columns in activity data

---

## 0.3.10 (December 2025) - Direct Lake Relationship Compatibility Fix

### Fixed

- **Critical: Surrogate Key Data Types** - Fixed Direct Lake relationship errors in Power BI/Fabric
  - Error: `The operation is not allowed because the data types of Direct Lake relationship between foreign key column 'fact_activity'[user_sk](Double) and primary key column 'dim_user'[user_sk](Int64) are incompatible`
  - Root cause: When surrogate keys contain NULL values, pandas converts the column to `float64` (Double) instead of integer
  - Fix: `_save_dataframe()` now enforces `Int64` (nullable integer) type for all `*_sk` columns before saving to parquet
  - Result: All dimension relationships now work correctly in Direct Lake semantic models

### Changed

- All surrogate key columns (`date_sk`, `time_sk`, `workspace_sk`, `item_sk`, `user_sk`, `activity_type_sk`, `status_sk`) are now saved as `Int64` type
- This ensures compatibility with Power BI Direct Lake mode which requires matching data types for relationships

---

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
