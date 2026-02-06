# USF Fabric Monitoring — Terminology Dictionary

**Version**: 0.3.35 | **Last Updated**: 2026-02-06

This glossary defines key terms used throughout the project. Terms are organized by domain.

---

## Quick Navigation

- [Microsoft Fabric Core](#microsoft-fabric-core)
- [Monitor Hub & Pipeline](#monitor-hub--pipeline)
- [Lineage Explorer](#lineage-explorer)
- [Graph Database (Neo4j)](#graph-database-neo4j)
- [External Data Sources](#external-data-sources)
- [UI & Navigation](#ui--navigation)

---

## Microsoft Fabric Core

| Term | Definition |
|------|------------|
| **Workspace** | A container within a Fabric tenant that holds items. Analogous to a project folder. |
| **FabricItem** | Any artifact in a workspace: Lakehouse, Warehouse, Notebook, Pipeline, Report, etc. |
| **Lakehouse** | A unified storage layer combining data lake flexibility with warehouse query performance. |
| **Warehouse** | A SQL-based analytics store optimized for structured queries. |
| **MirroredDatabase** | A Fabric item that replicates tables from an external database (e.g., Snowflake). |
| **SemanticModel** | A Power BI dataset abstraction layer that defines measures, relationships, and DAX logic. |
| **OneLake** | Microsoft's unified data lake for all Fabric workloads. Single namespace across the tenant. |
| **Shortcut** | A pointer within OneLake that references external data (ADLS, S3, etc.) without copying. |
| **Dataflow** | Power Query-based data transformation pipelines in Fabric. |
| **Notebook** | Spark-based interactive coding environment (Python, Scala, SQL). |
| **Pipeline** | Orchestration workflow for data movement and transformation. |
| **Capacity** | The compute resource allocation backing a Fabric workspace. |

---

## Monitor Hub & Pipeline

| Term | Definition | Code Reference |
|------|------------|----------------|
| **Monitor Hub** | Microsoft's built-in activity tracking for Fabric. Source of audit events. | `core/pipeline.py` |
| **Activity Events API** | Audit log entries (high volume). **Always returns status="Succeeded"** because events record that an action occurred, not its outcome. | `core/pipeline.py:_extract_activities()` |
| **Job History API** | Actual job execution data (lower volume). Contains `end_time`, `failure_reason`, `duration`. | `core/pipeline.py:_extract_job_details()` |
| **Smart Merge** | Algorithm that correlates Activity Events with Job History to enrich activities with execution details. Uses `merge_asof` with 5-minute tolerance. | `core/pipeline.py:_merge_activities()` |
| **Star Schema** | Kimball-style dimensional model with fact and dimension tables. | `core/star_schema_builder.py` |
| **record_count** | The correct column to sum for activity counts. (**Not** `activity_id`, which is NULL for 99% of records.) | — |
| **dim_workspace** | Dimension table containing workspace metadata. | — |
| **fact_activity** | Fact table with granular activity events. | — |

---

## Lineage Explorer

| Term | Definition | Code Reference |
|------|------------|----------------|
| **Lineage Explorer** | Interactive web application for visualizing Fabric data dependencies. | `lineage_explorer/` |
| **Graph Schema** | The API contract defining nodes (workspaces, items, sources) and edges (relationships). | `lineage_explorer/graph_builder.py` |
| **Multi-View Architecture** | Design pattern splitting the UI into persona-driven pages (Overview, Tables, Items) to handle 7,000+ entities. | v0.3.31 |
| **500-Node Guardrail** | Client-side pruning that limits DOM complexity for 60FPS interactivity. | `index.html` |
| **Table Layer** | Toggle overlay that displays physical table nodes (diamond shapes) connected to their parent containers. | v0.3.29 |
| **Impact Analysis** | Tracing all downstream items affected by a change to a table or item. | `/api/neo4j/table-impact/` |
| **Upstream Trace** | Finding all data sources and dependencies feeding into an item. | `/api/neo4j/upstream/` |
| **Flow Particles** | Animated dots along edges indicating data movement direction. | `index.html` |

---

## Graph Database (Neo4j)

| Term | Definition |
|------|------------|
| **Node** | A vertex in the graph (Workspace, FabricItem, ExternalSource, Table). |
| **Relationship / Edge** | A directed connection between nodes. |
| **CONTAINS** | Relationship: Workspace → FabricItem. Indicates membership. |
| **DEPENDS_ON** | Relationship: FabricItem → FabricItem. Internal logical lineage. |
| **CONSUMES** | Relationship: FabricItem → ExternalSource. External data dependency. |
| **MIRRORS** | Relationship: MirroredDatabase → Table. Physical table replication. |
| **USES_TABLE** | Relationship: Lakehouse → Table (via shortcut). |
| **PROVIDES_TABLE** | Relationship: Lakehouse → Table. Tables exposed by a container. |
| **Cypher** | Neo4j's declarative query language. |
| **Centrality** | Graph algorithm measuring a node's importance based on connections. |
| **Path Finder** | Feature to find shortest path between two nodes in the graph. |

---

## External Data Sources

| Term | Definition |
|------|------------|
| **ExternalSource** | Any data source outside Fabric that items consume. |
| **AdlsGen2** | Azure Data Lake Storage Gen2. Cloud object storage. |
| **Snowflake** | Third-party cloud data warehouse. Primary source for MirroredDatabases in this project. |
| **AmazonS3** | AWS object storage service. |
| **OneDriveSharePoint** | Microsoft 365 document storage (Excel files, etc.). |
| **OneLake Shortcut** | A reference to data in another OneLake location or external source. |
| **Connection** | Metadata object describing how an item connects to an external source. |

---

## UI & Navigation

| Term | Definition |
|------|------------|
| **Grouped Navigation** | The v0.3.31 nav structure: **Visualizations** (Overview, Tables, Items) and **Analysis** (Stats, Impact, Queries). |
| **`.nav-separator`** | CSS class for the vertical divider between nav groups. |
| **Force Layout** | D3.js physics simulation where nodes repel and edges act as springs. |
| **Radial Layout** | Circular arrangement with root at center. |
| **Tree Layout** | Hierarchical top-down arrangement using Dagre.js. |
| **Detail Panel** | Right-side panel showing node metadata, connections, and actions. |
| **Minimap** | Overview navigation widget in the corner of graph views. |
| **Hard Refresh** | `Ctrl+Shift+R` — Clears browser cache to load latest frontend changes. |

---

## Acronyms

| Acronym | Expansion |
|---------|-----------|
| **USF** | Unified Services Fabric (internal project codename) |
| **API** | Application Programming Interface |
| **ADLS** | Azure Data Lake Storage |
| **DAX** | Data Analysis Expressions (Power BI formula language) |
| **DOM** | Document Object Model |
| **FPS** | Frames Per Second |
| **KPI** | Key Performance Indicator |
| **REST** | Representational State Transfer |
| **SVG** | Scalable Vector Graphics |
| **UUID** | Universally Unique Identifier |

---

## Related Resources

- [Lineage Explorer API Reference](02_User_Guides/Lineage_Explorer_API.md)
- [Copilot Instructions](../.github/copilot-instructions.md)
- [Architecture Spec](05_Architecture_Design/01_Lineage_Explorer_Spec.md)
- [Microsoft Fabric Documentation](https://learn.microsoft.com/en-us/fabric/)
