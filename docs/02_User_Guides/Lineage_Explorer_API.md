# Lineage Explorer API Reference

**Version**: 2.1.0  
**Base URL**: `http://localhost:8000`

The Lineage Explorer provides a REST API for querying Microsoft Fabric lineage data. It supports both in-memory statistics and Neo4j graph database queries for advanced analysis.

---

## Quick Start

```bash
# Start the server
cd usf_fabric_monitoring
conda activate fabric-monitoring
python scripts/run_lineage_explorer.py
# OR: python -m lineage_explorer

# Optional: Start Neo4j for graph queries
docker compose -f lineage_explorer/docker-compose.yml up -d
```

---

## Core Endpoints

### GET /api/health
Check server health and cache status.

**Response**:
```json
{
  "status": "healthy",
  "version": "2.0.0",
  "cached": true,
  "loaded_at": "2026-01-23T00:29:05.064769",
  "source_file": "/path/to/lineage_20260122.csv"
}
```

### GET /api/graph
Get the full lineage graph for visualization.

**Response**:
```json
{
  "workspaces": [...],
  "items": [...],
  "external_sources": [...],
  "edges": [...],
  "total_items": 198,
  "total_connections": 2458
}
```

### GET /api/stats
Get basic graph statistics.

**Response**:
```json
{
  "workspace_count": 76,
  "item_count": 198,
  "external_source_count": 9,
  "edge_count": 2458
}
```

### GET /api/workspaces
List all workspaces.

**Response**:
```json
{
  "workspaces": [
    {"id": "uuid-1", "name": "EDP Lakehouse [DEV]"},
    {"id": "uuid-2", "name": "ABBA Lakehouse"}
  ]
}
```

### POST /api/refresh
Force refresh the graph from source file.

**Response**:
```json
{
  "status": "refreshed",
  "items": 198,
  "edges": 2458
}
```

---

## Enhanced Statistics Endpoints

### GET /api/stats/detailed
Get comprehensive lineage statistics with all breakdowns.

**Response**:
```json
{
  "summary": {
    "total_workspaces": 76,
    "total_items": 198,
    "total_external_sources": 9,
    "total_edges": 2458,
    "total_tables": 42
  },
  "distributions": {
    "items_by_type": {"Lakehouse": 2458, "MirroredDatabase": 80},
    "sources_by_type": {"AdlsGen2": 6, "AmazonS3": 1, "OneDriveSharePoint": 3},
    "edges_by_type": {"internal": 2131, "external": 327}
  },
  "workspace_analysis": {
    "workspace_stats": [...],
    "workspaces_with_external_deps": 5,
    "workspaces_with_internal_deps": 76
  },
  "external_source_analysis": {
    "snowflake_databases": ["EMEA_DRH_DEV", "EMEA_EDW_DEV"],
    "adls_containers": ["/inboundstorage/", "/rpafilestorage/"],
    "s3_buckets": ["s3://rdata.ruk-ds.eu-central-1"],
    "sharepoint_sites": ["https://ricoheuropeplc.sharepoint.com"]
  },
  "mirrored_database_analysis": {
    "total_mirrored_databases": 80,
    "total_tables": 42,
    "tables_by_database": {"EMEA_DRH_DEV": 37},
    "tables_by_schema": {"MTINS2": 17, "SNOW_MTI": 20}
  }
}
```

### GET /api/stats/workspaces
Get per-workspace statistics.

**Response**:
```json
{
  "workspaces": [
    {
      "workspace_id": "4eb1b160-...",
      "workspace_name": "EDP Lakehouse [DEV]",
      "item_count": 30,
      "external_deps": 10,
      "internal_deps": 415
    }
  ],
  "total": 76
}
```

### GET /api/stats/external-sources
Get external source breakdown.

**Response**:
```json
{
  "by_type": {"AdlsGen2": 6, "AmazonS3": 1, "OneDriveSharePoint": 3},
  "snowflake_databases": [],
  "adls_containers": ["/inboundstorage/", "/rpafilestorage/"],
  "s3_buckets": ["s3://rdata.ruk-ds.eu-central-1"],
  "sharepoint_sites": ["https://ricoheuropeplc.sharepoint.com"]
}
```

### GET /api/stats/tables
Get MirroredDatabase table statistics.

**Response**:
```json
{
  "total_tables": 42,
  "mirrored_databases": 80,
  "tables_by_database": {"EMEA_DRH_DEV": 37, "EMEA_EDW_DEV": 2},
  "tables_by_schema": {"MTINS2": 17, "SNOW_MTI": 20, "ENR_DATA": 5}
}
```

### GET /api/stats/cross-workspace
Get cross-workspace dependency pairs.

**Response**:
```json
{
  "cross_workspace_edges": 45,
  "workspace_pairs": [
    {"source": "Workspace A", "target": "Workspace B", "count": 12}
  ]
}
```

### GET /api/stats/orphans
Get items with no dependencies.

**Response**:
```json
{
  "orphan_count": 15,
  "orphans": [
    {"id": "item-uuid", "name": "Unused Lakehouse", "type": "Lakehouse", "workspace": "Dev"}
  ]
}
```

---

## Neo4j Graph Database Endpoints

> **Note**: These endpoints require Neo4j to be running. Start with:
> ```bash
> docker compose -f lineage_explorer/docker-compose.yml up -d
> ```

### GET /api/neo4j/health
Check Neo4j connection status.

**Response**:
```json
{
  "status": "connected",
  "uri": "bolt://localhost:7687",
  "database": "neo4j"
}
```

### GET /api/neo4j/stats
Get Neo4j database statistics.

**Response**:
```json
{
  "nodes": 285,
  "relationships": 2458,
  "labels": ["Workspace", "FabricItem", "ExternalSource", "Table"],
  "relationship_types": ["CONTAINS", "DEPENDS_ON", "MIRRORS", "CONSUMES"]
}
```

### POST /api/neo4j/load
Load lineage data into Neo4j.

**Response**:
```json
{
  "status": "loaded",
  "workspaces": 76,
  "items": 198,
  "sources": 9,
  "edges": 2458,
  "tables": 42
}
```

### GET /api/neo4j/upstream/{item_id}
Find all upstream dependencies of an item.

**Parameters**:
- `item_id` (path): The item's UUID
- `depth` (query, optional): Max traversal depth (default: 5, max: 10)

**Response**:
```json
{
  "dependencies": [
    {"id": "def-456", "name": "Source Lakehouse", "type": "Lakehouse", "depth": 1, "path_names": ["Target", "Source Lakehouse"]},
    {"id": "ghi-789", "name": "ADLS Container", "type": "ExternalSource", "depth": 2, "path_names": ["Target", "Source Lakehouse", "ADLS Container"]}
  ]
}
```

### GET /api/neo4j/downstream/{item_id}
Find all downstream dependents (impact analysis).

**Parameters**:
- `item_id` (path): The item's UUID
- `depth` (query, optional): Max traversal depth (default: 5, max: 10)

**Response**:
```json
{
  "impacted_items": [
    {"id": "xyz-999", "name": "Report Dataset", "type": "SemanticModel", "depth": 1, "path_names": ["Source", "Report Dataset"]}
  ]
}
```

### GET /api/neo4j/path
Find the shortest path between two items.

**Query Parameters**:
- `source_id`: Source item UUID
- `target_id`: Target item UUID
- `max_depth` (optional): Maximum path length (default: 10, max: 15)

**Response**:
```json
{
  "paths": [
    {
      "path_length": 2,
      "path": [
        {"id": "abc-123", "name": "Source", "type": "Lakehouse"},
        {"id": "def-456", "name": "Transform", "type": "Notebook"},
        {"id": "xyz-999", "name": "Target", "type": "SemanticModel"}
      ],
      "relationship_types": ["DEPENDS_ON", "DEPENDS_ON"]
    }
  ]
}
```

### GET /api/neo4j/search
Search for items by name or type.

**Query Parameters**:
- `q`: Search query (partial match)
- `type` (optional): Filter by item type

**Response**:
```json
{
  "query": "sales",
  "results": [
    {"id": "abc-123", "name": "Sales Lakehouse", "type": "Lakehouse", "workspace": "Analytics"}
  ],
  "total": 5
}
```

### GET /api/neo4j/workspace/{workspace_name}
Get all items in a workspace with their dependencies.

**Response**:
```json
{
  "workspace": "EDP Lakehouse [DEV]",
  "items": [...],
  "internal_dependencies": 415,
  "external_dependencies": 10
}
```

### GET /api/neo4j/source-type/{source_type}
Get all items consuming a specific source type.

**Parameters**:
- `source_type`: One of `AdlsGen2`, `AmazonS3`, `OneDriveSharePoint`, `Snowflake`

**Response**:
```json
{
  "source_type": "AdlsGen2",
  "consumers": [
    {"id": "abc-123", "name": "Raw Lakehouse", "workspace": "Bronze Layer"}
  ],
  "total": 6
}
```

### GET /api/neo4j/table/{table_name}
Find items that use a specific table (via MIRRORS or USES_TABLE relationships).

**Parameters**:
- `table_name` (path): Table name (partial match supported)

**Response**:
```json
[
  {
    "table_name": "CUSTOMER",
    "table_schema": "MTINS2",
    "table_database": "EMEA_DRH_DEV",
    "item_id": "abc-123",
    "item_name": "Customer Mirror",
    "item_type": "MirroredDatabase",
    "workspace": "EDP Lakehouse [DEV]",
    "relationship": "MIRRORS"
  },
  {
    "table_name": "CUSTOMER",
    "table_schema": null,
    "table_database": null,
    "item_id": "def-456",
    "item_name": "SHARE_GOLD",
    "item_type": "Lakehouse",
    "workspace": "Data Platform",
    "relationship": "USES_TABLE"
  }
]
```

### GET /api/neo4j/table-impact/{table_name}
**NEW** - Get comprehensive impact analysis for a table (Speaker 2's use case).

This endpoint answers: "If I change this Snowflake table, what Fabric items will be affected?"

**Parameters**:
- `table_name` (path): Table name (partial match supported)
- `max_depth` (query, optional): Maximum traversal depth for downstream impact (default: 5, max: 10)

**Response**:
```json
{
  "query": "ACTIVITY",
  "tables_found": 20,
  "results": [
    {
      "table": {
        "table_id": "tbl_emea_drh_dev_mtins2_activity",
        "table_name": "ACTIVITY",
        "database": "EMEA_DRH_DEV",
        "schema": "MTINS2",
        "full_path": "EMEA_DRH_DEV.MTINS2.ACTIVITY"
      },
      "direct_consumers": [
        {
          "item_id": "abc-123",
          "item_name": "SNOWFLAKE_DRH_DEV_MTI_MIRROR",
          "item_type": "MirroredDatabase",
          "workspace": "EDP Lakehouse [DEV]",
          "relationship": "MIRRORS"
        }
      ],
      "downstream_impact": [
        {
          "item_id": "def-456",
          "item_name": "LH Lead Management",
          "item_type": "Lakehouse",
          "workspace": "Sales Analytics",
          "depth": 1
        }
      ]
    }
  ],
  "summary": {
    "total_impacted_items": 16,
    "impacted_workspaces": ["EDP Lakehouse [DEV]", "RBS EMEA [DEV]", "..."],
    "total_impacted_workspaces": 14,
    "max_depth_found": 2
  }
}
```

### GET /api/neo4j/centrality
Calculate item importance using PageRank-style centrality.

**Query Parameters**:
- `limit` (optional): Number of results (default: 20)

**Response**:
```json
{
  "algorithm": "degree_centrality",
  "top_items": [
    {"id": "abc-123", "name": "Core Lakehouse", "score": 0.95, "connections": 415},
    {"id": "def-456", "name": "Raw Data Lake", "score": 0.87, "connections": 363}
  ]
}
```

### GET /api/neo4j/cross-workspace
Get cross-workspace dependencies.

**Response**:
```json
{
  "cross_workspace_count": 45,
  "dependencies": [
    {
      "source_workspace": "Bronze Layer",
      "target_workspace": "Silver Layer",
      "source_item": "Raw Lakehouse",
      "target_item": "Curated Lakehouse"
    }
  ]
}
```

### POST /api/neo4j/query
Execute a custom Cypher query (read-only).

**Request Body**:
```json
{
  "query": "MATCH (w:Workspace)-[:CONTAINS]->(i:FabricItem) RETURN w.name, count(i) as items ORDER BY items DESC LIMIT 10"
}
```

**Response**:
```json
{
  "results": [
    {"w.name": "EDP Lakehouse [DEV]", "items": 30}
  ],
  "row_count": 10
}
```

---

## Web Interface

### Dashboard
Access the statistics dashboard at: `http://localhost:8000/dashboard.html`

Features:
- **9 Summary KPI cards**:
  - Workspaces, Items, External Sources, Connections, Tables
  - Internal Deps, External Deps, Snowflake sources, OneLake sources
- Interactive charts (items by type, sources by type)
- Workspace table with search and filtering
- External source details (ADLS, S3, SharePoint, Snowflake)
- Table breakdown by database/schema
- Neo4j status and data loading

### Graph Visualization
Access the interactive lineage graph at: `http://localhost:8000/`

Features:
- Force-directed, radial, and tree layouts
- Animated data flow particles
- Multi-filter sidebar
- Search with autocomplete
- Detail panel with connections
- Minimap navigation

#### Neo4j-Powered Features (requires Neo4j running)
When Neo4j is connected, additional features become available:

| Feature | How to Access |
|---------|---------------|
| **Path Finder** | Right-click node → "Set as Path Source", then another → "Set as Path Target", click "Find Path" |
| **Impact Analysis** | Right-click node → "Show Downstream Impact" |
| **Upstream Trace** | Right-click node → "Show Upstream Sources" |
| **Smart Filtering** | Sidebar → "Show items connected to" dropdown (ADLS, S3, Snowflake) |
| **Cross-Workspace View** | Sidebar → "Cross-WS" button in Graph Analysis section |
| **Centrality Highlighting** | Sidebar → "Centrality" button to resize nodes by importance |
| **Copy Node ID** | Right-click node → "Copy Node ID" |
| **Focus on Node** | Right-click node → "Focus on Node" |

The Neo4j connection status is displayed in the header as a green (connected) or gray (disconnected) indicator.

---

## Error Responses

All endpoints return standard error responses:

```json
{
  "detail": "Error message here"
}
```

| Status Code | Description |
|-------------|-------------|
| 400 | Bad Request - Invalid parameters |
| 404 | Not Found - Resource doesn't exist |
| 500 | Internal Server Error |
| 503 | Service Unavailable - Neo4j not connected |

---

## Data Model

### Node Types (Neo4j Labels)
- **Workspace**: Fabric workspace container
- **FabricItem**: Lakehouse, MirroredDatabase, Notebook, etc.
- **ExternalSource**: ADLS, S3, SharePoint, Snowflake
- **Table**: Individual table from MirroredDatabase
- **Connection**: Database connection metadata

### Relationship Types
- **CONTAINS**: Workspace → FabricItem
- **DEPENDS_ON**: FabricItem → FabricItem (internal)
- **CONSUMES**: FabricItem → ExternalSource
- **MIRRORS**: MirroredDatabase → Table
- **CONNECTS_TO**: Connection → ExternalSource

---

## Authentication

Currently, the API does not require authentication. For production deployments, consider:
- Adding API key authentication
- Integrating with Azure AD
- Using a reverse proxy with auth

---

## Rate Limiting

No rate limiting is currently enforced. For production:
- Consider adding rate limiting middleware
- Implement caching for expensive queries
- Use connection pooling for Neo4j

---

## Examples

### Find all items depending on ADLS storage
```bash
curl -s "http://localhost:8000/api/neo4j/source-type/AdlsGen2" | jq
```

### Search for lakehouses containing "sales"
```bash
curl -s "http://localhost:8000/api/neo4j/search?q=sales&type=Lakehouse" | jq
```

### Get impact analysis for a critical item
```bash
curl -s "http://localhost:8000/api/neo4j/downstream/abc-123-uuid?depth=10" | jq
```

### Find path between two items
```bash
curl -s "http://localhost:8000/api/neo4j/path?source_id=abc-123&target_id=xyz-999" | jq
```

---

## Table Lineage Panel (v0.3.25)

The frontend includes an interactive Table Lineage Panel for exploring Fabric items alongside the graph.

### Features
- **Toggle Panel**: Click the table icon in the toolbar (next to labels toggle)
- **Stats Display**: Shows Items, Shortcuts, and Total counts
- **Search**: Filter tables by name, database, or path
- **Node Filtering**: Selecting a graph node automatically filters the panel

### Interaction Flow
1. Click a node in the graph → Panel shows "Filtered by: [node name]"
2. Table list filters to show related items
3. Click X in filter indicator to clear and show all items
4. Click a table card to focus the corresponding node in the graph

### Data Source
The panel derives data from the graph state (`state.graph.nodes`), including:
- Fabric items (Lakehouses, Warehouses, Notebooks, Dataflows)
- External sources (Snowflake, OneLake, ADLS)
- Shortcuts and mirrored databases

