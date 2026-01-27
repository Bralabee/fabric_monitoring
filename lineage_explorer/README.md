# Lineage Explorer

Interactive web visualization for Microsoft Fabric data lineage with Neo4j-powered graph analysis.

## Quick Start

```bash
# From project root
cd usf_fabric_monitoring
conda activate fabric-monitoring

# Start the server (loads from exports/lineage/*.csv)
make lineage-explorer
# OR: python scripts/run_lineage_explorer.py
# OR: python -m lineage_explorer

# Optional: Start Neo4j for advanced graph analysis
cd lineage_explorer
docker compose up -d neo4j

# Open browser at http://localhost:8000
```

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                        Frontend                              │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐ │
│  │  index.html │  │  app.js     │  │  neo4j-features.js   │ │
│  │  (D3.js v7) │  │  (910 LOC)  │  │  (650+ LOC)          │ │
│  └─────────────┘  └─────────────┘  └──────────────────────┘ │
└────────────────────────────┬─────────────────────────────────┘
                             │ REST API
┌────────────────────────────▼─────────────────────────────────┐
│                        Backend                               │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────────────┐ │
│  │  server.py  │  │  models.py  │  │  api_extended.py     │ │
│  │  (FastAPI)  │  │  (Pydantic) │  │  (24 endpoints)      │ │
│  └─────────────┘  └─────────────┘  └──────────────────────┘ │
└────────────────────────────┬─────────────────────────────────┘
                             │
         ┌───────────────────┼───────────────────┐
         │                   │                   │
         ▼                   ▼                   ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  CSV Files  │     │  Statistics │     │   Neo4j     │
│  (lineage)  │     │  (memory)   │     │  (Docker)   │
└─────────────┘     └─────────────┘     └─────────────┘
```

## Files

| File | Description |
|------|-------------|
| `server.py` | FastAPI server with auto Neo4j initialization |
| `models.py` | Pydantic models for lineage data |
| `api_extended.py` | 24 extended API endpoints for stats and Neo4j |
| `statistics.py` | Enhanced statistics calculation module |
| `static/index.html` | **Main graph visualization** with D3.js (light theme) |
| `static/app.js` | D3.js force-directed graph with filters |
| `static/dashboard.html` | **Statistics dashboard** with Chart.js (light theme) |
| `static/query_explorer.html` | **Interactive query explorer** with 10 categories (light theme) |
| `graph_database/neo4j_client.py` | Neo4j connection management |
| `graph_database/data_loader.py` | CSV to Neo4j graph transformation |
| `graph_database/queries.py` | 20+ Cypher queries for analysis |
| `graph_database/docker-compose.yml` | Neo4j container configuration |

## Features

### Three Complementary Pages (Light Theme)
1. **Graph View** (`/`) - Interactive D3.js visualization with persistent node selection
2. **Dashboard** (`/dashboard.html`) - Statistics and charts with workspace analytics
3. **Query Explorer** (`/query_explorer.html`) - Interactive Neo4j query catalog

### Core Visualization (Graph Page)
- **3 Layout Modes**: Force-directed (default), Radial, Tree
- **Animated Particles**: Data flow visualization along connections
- **Interactive Minimap**: Overview navigation
- **Multi-Filter Sidebar**: Workspaces, item types, source types
- **Search with Autocomplete**: Quick node finding
- **Detail Panel**: Node overview, connections, metadata tabs
- **Persistent Selection**: Clicked nodes stay highlighted with blue ring
- **Keyboard Shortcuts**: F=fit, +/-=zoom, /=search, Esc=deselect

### Neo4j-Powered Features (requires Neo4j running)

| Feature | How to Access |
|---------|---------------|
| **Path Finder** | Right-click node → "Set as Path Source", then another → "Set as Path Target", click "Find Path" |
| **Impact Analysis** | Right-click node → "Show Downstream Impact" |
| **Upstream Trace** | Right-click node → "Show Upstream Sources" |
| **Smart Filtering** | Sidebar → "Show items connected to" dropdown |
| **Cross-Workspace View** | Sidebar → "Cross-WS" button |
| **Centrality Highlighting** | Sidebar → "Centrality" button |
| **Copy Node ID** | Right-click node → "Copy Node ID" |

## Neo4j Setup

```bash
# Start Neo4j container
cd lineage_explorer
docker compose up -d neo4j

# Access Neo4j Browser: http://localhost:7474
# Credentials: neo4j / fabric_lineage

# Data is auto-loaded when server starts with Neo4j available
```

## API Endpoints

### Core Endpoints
- `GET /api/health` - Server health check
- `GET /api/graph` - Full lineage graph for visualization
- `GET /api/stats` - Basic graph statistics
- `POST /api/refresh` - Reload data from CSV

### Statistics Endpoints
- `GET /api/stats/detailed` - Comprehensive statistics
- `GET /api/stats/workspaces` - Per-workspace statistics
- `GET /api/stats/external-sources` - External source breakdown
- `GET /api/stats/tables` - MirroredDatabase table statistics

### Neo4j Endpoints
- `GET /api/neo4j/health` - Neo4j connection status
- `POST /api/neo4j/load` - Load data into Neo4j
- `GET /api/neo4j/upstream/{id}` - Find upstream dependencies
- `GET /api/neo4j/downstream/{id}` - Find downstream impact
- # Query Explorer Features
- **10 Query Categories**: Overview, Workspaces, Items, Dependencies, External Sources, Tables, Semantic Models, Reports, Security, Advanced
- **~40 Pre-Built Queries**: Curated Neo4j Cypher queries for lineage analysis
- **Interactive Execution**: Run queries with one click, see real-time results
- **KPI Cards**: Quick metrics (Total Workspaces, Items, External Sources, Tables)
- **Dual View Modes**: Table view (sortable columns) and Chart view (visualizations)
- **Export Options**: Download results as CSV or JSON
- **Query Descriptions**: Each query includes clear documentation

### Dashboard Features
- **Summary KPIs**: Workspaces, items, external sources, connections, tables
- **Interactive Charts**: Chart.js pie/bar charts for distribution analysis
- **Workspace Table**: Sortable, searchable workspace analytics
- **External Source Details**: ADLS containers, S3 buckets, SharePoint sites
- **Table Analytics**: Tables by database/schema (Snowflake)
- **Neo4j Status**: Connection indicator with data loading button

## Data Source

Reads from `exports/lineage/` directory - auto-detects:
- `lineage_*.json` (from iterative mode - **preferred**, has 6 item types with rich data)
- `lineage_scanner_*.json` (from Admin Scanner mode - warehouses only)
- `mirrored_lineage_*.csv` (legacy)

### Extraction Modes

| Mode | File Pattern | Item Types |
|------|--------------|------------|
| `iterative` | `lineage_*.json` | MirroredDB, Shortcuts, SemanticModels, Dataflows, Reports |
| `scanner` | `lineage_scanner_*.json` | Warehouses only |


## Design & Navigation

All three pages share a **consistent light theme** with Inter font:
- Background: `#f8fafc` (light gray)
- Text: `#0f172a` (navy)
- Accent: `#3b82f6` (blue)

**Navigation component** appears in header of all pages:
- Graph icon → Main graph visualization
- Chart icon → Statistics dashboard
- Search icon → Query explorer
Access at `http://localhost:8000/dashboard.html`:
- Summary KPIs (workspaces, items, sources, connections, tables)
- Interactive charts (items by type, sources by type)
- Workspace table with search
- External source details (ADLS, S3, SharePoint)
- Neo4j status and data loading
