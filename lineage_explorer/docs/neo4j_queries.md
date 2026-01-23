# Neo4j Cypher Queries for Fabric Lineage Explorer

A comprehensive collection of reusable Cypher queries for insight extraction, statistics generation, and lineage analysis from Microsoft Fabric metadata.

## Schema Reference

### Node Labels
| Label | Description | Key Properties |
|-------|-------------|----------------|
| `Workspace` | Fabric workspace | `id`, `name` |
| `FabricItem` | Fabric item (Lakehouse, Notebook, etc.) | `id`, `name`, `type`, `workspace_id` |
| `ExternalSource` | External data sources | `id`, `display_name`, `type`, `connection_details` |
| `Table` | Database tables (mirrored) | `id`, `name`, `database`, `schema`, `full_path` |

### Relationship Types
| Relationship | Description | Direction |
|--------------|-------------|-----------|
| `CONTAINS` | Workspace contains items | `(Workspace)-[:CONTAINS]->(FabricItem)` |
| `DEPENDS_ON` | Item depends on another | `(FabricItem)-[:DEPENDS_ON]->(FabricItem)` |
| `CONSUMES` | Item consumes external source | `(FabricItem)-[:CONSUMES]->(ExternalSource)` |
| `MIRRORS` | Item mirrors tables | `(FabricItem)-[:MIRRORS]->(Table)` |

---

## Table of Contents
- [Database Overview](#database-overview)
- [Basic Statistics](#basic-statistics)
- [Workspace Analysis](#workspace-analysis)
- [Item Analysis](#item-analysis)
- [External Source Analysis](#external-source-analysis)
- [Table Analysis](#table-analysis)
- [Dependency & Lineage Queries](#dependency--lineage-queries)
- [Impact Analysis](#impact-analysis)
- [Cross-Workspace Analysis](#cross-workspace-analysis)
- [Data Quality & Governance](#data-quality--governance)
- [Performance & Optimization](#performance--optimization)
- [Advanced Analytics](#advanced-analytics)
- [Utility Queries](#utility-queries)

---

## Database Overview

### Node Counts by Label
```cypher
MATCH (n)
RETURN labels(n)[0] AS label, count(*) AS count
ORDER BY count DESC
```

### Relationship Counts by Type
```cypher
MATCH ()-[r]->()
RETURN type(r) AS relationship_type, count(*) AS count
ORDER BY count DESC
```

### Complete Schema Overview
```cypher
// Show all labels
CALL db.labels() YIELD label
RETURN label

// Show all relationship types  
CALL db.relationshipTypes() YIELD relationshipType
RETURN relationshipType
```

---

## Basic Statistics

### Total Graph Metrics
```cypher
MATCH (n)
WITH count(n) AS total_nodes
MATCH ()-[r]->()
WITH total_nodes, count(r) AS total_relationships
MATCH (w:Workspace)
WITH total_nodes, total_relationships, count(w) AS workspaces
MATCH (i:FabricItem)
WITH total_nodes, total_relationships, workspaces, count(i) AS items
MATCH (s:ExternalSource)
WITH total_nodes, total_relationships, workspaces, items, count(s) AS external_sources
MATCH (t:Table)
RETURN total_nodes, total_relationships, workspaces, items, external_sources, count(t) AS tables
```

### Node Distribution
```cypher
MATCH (n)
WITH labels(n)[0] AS node_type, count(*) AS count
RETURN node_type, count
ORDER BY count DESC
```

### Connectivity Statistics
```cypher
MATCH (n)
WITH labels(n)[0] AS node_type, n
OPTIONAL MATCH (n)-[r]-()
WITH node_type, n, count(r) AS connections
RETURN node_type, 
       count(n) AS node_count,
       avg(connections) AS avg_connections,
       min(connections) AS min_connections,
       max(connections) AS max_connections
ORDER BY avg_connections DESC
```

---

## Workspace Analysis

### Workspace Summary
```cypher
MATCH (w:Workspace)
OPTIONAL MATCH (w)-[:CONTAINS]->(i:FabricItem)
WITH w, count(i) AS item_count
OPTIONAL MATCH (w)-[:CONTAINS]->(i:FabricItem)-[:DEPENDS_ON]->()
WITH w, item_count, count(DISTINCT i) AS items_with_dependencies
RETURN w.name AS workspace, 
       w.id AS workspace_id,
       item_count,
       items_with_dependencies,
       item_count - items_with_dependencies AS standalone_items
ORDER BY item_count DESC
LIMIT 20
```

### Workspace Item Type Distribution
```cypher
MATCH (w:Workspace)-[:CONTAINS]->(i:FabricItem)
RETURN w.name AS workspace, 
       i.type AS item_type, 
       count(*) AS count
ORDER BY w.name, count DESC
LIMIT 50
```

### Top Workspaces by Complexity
Ranked by total relationships (dependencies, data sources, etc.):
```cypher
MATCH (w:Workspace)-[:CONTAINS]->(i:FabricItem)
OPTIONAL MATCH (i)-[r:DEPENDS_ON|CONSUMES|MIRRORS]-()
WITH w, count(DISTINCT i) AS items, count(r) AS total_relationships
RETURN w.name AS workspace,
       items,
       total_relationships,
       round(1.0 * total_relationships / items, 2) AS avg_relationships_per_item
ORDER BY total_relationships DESC
LIMIT 20
```

### Isolated Workspaces
Workspaces with no external dependencies or data sources:
```cypher
MATCH (w:Workspace)-[:CONTAINS]->(i:FabricItem)
WHERE NOT EXISTS {
    MATCH (i)-[:DEPENDS_ON]->(other:FabricItem)<-[:CONTAINS]-(w2:Workspace)
    WHERE w2 <> w
}
AND NOT EXISTS {
    MATCH (i)-[:CONSUMES]->(:ExternalSource)
}
WITH w, count(i) AS item_count
RETURN w.name AS isolated_workspace, item_count
ORDER BY item_count DESC
LIMIT 20
```

---

## Item Analysis

### Item Type Statistics
```cypher
MATCH (i:FabricItem)
RETURN i.type AS item_type, count(*) AS count
ORDER BY count DESC
```

### Most Connected Items (Hub Nodes)
Items with the highest number of incoming and outgoing connections:
```cypher
MATCH (i:FabricItem)
OPTIONAL MATCH (i)-[out]->()
OPTIONAL MATCH (i)<-[in]-()
WITH i, count(DISTINCT out) AS outgoing, count(DISTINCT in) AS incoming
RETURN i.name AS item_name,
       i.type AS item_type,
       incoming,
       outgoing,
       incoming + outgoing AS total_connections
ORDER BY total_connections DESC
LIMIT 25
```

### Leaf Nodes (No Downstream Consumers)
Items that are not consumed by any other items:
```cypher
MATCH (i:FabricItem)
WHERE NOT EXISTS { MATCH (i)<-[:DEPENDS_ON]-() }
OPTIONAL MATCH (i)<-[:CONTAINS]-(w:Workspace)
RETURN i.name AS item_name, 
       i.type AS item_type,
       w.name AS workspace
ORDER BY i.type, i.name
LIMIT 30
```

### Root Nodes (No Upstream Dependencies)
Items that don't depend on other items or external sources:
```cypher
MATCH (i:FabricItem)
WHERE NOT EXISTS { MATCH (i)-[:DEPENDS_ON]->() }
AND NOT EXISTS { MATCH (i)-[:CONSUMES]->() }
OPTIONAL MATCH (i)<-[:CONTAINS]-(w:Workspace)
RETURN i.name AS item_name, 
       i.type AS item_type,
       w.name AS workspace
ORDER BY i.type, i.name
LIMIT 30
```

---

## External Source Analysis

### External Source Summary
```cypher
MATCH (s:ExternalSource)
OPTIONAL MATCH (s)<-[:CONSUMES]-(i:FabricItem)
RETURN s.display_name AS source_name,
       s.type AS source_type,
       count(DISTINCT i) AS used_by_items
ORDER BY used_by_items DESC
LIMIT 30
```

### External Source Types Distribution
```cypher
MATCH (s:ExternalSource)
RETURN s.type AS source_type, count(*) AS count
ORDER BY count DESC
```

### Most Used External Sources
```cypher
MATCH (s:ExternalSource)<-[:CONSUMES]-(i:FabricItem)
WITH s, collect(DISTINCT i.name) AS consumers, count(DISTINCT i) AS consumer_count
RETURN s.display_name AS source_name,
       s.type AS source_type,
       consumer_count,
       consumers[0..5] AS sample_consumers
ORDER BY consumer_count DESC
LIMIT 20
```

### Unused External Sources
External sources with no consumers:
```cypher
MATCH (s:ExternalSource)
WHERE NOT EXISTS { MATCH (s)<-[:CONSUMES]-() }
RETURN s.display_name AS unused_source,
       s.type AS source_type
ORDER BY s.type, s.display_name
LIMIT 30
```

### External Sources by Workspace
```cypher
MATCH (s:ExternalSource)<-[:CONSUMES]-(i:FabricItem)<-[:CONTAINS]-(w:Workspace)
RETURN w.name AS workspace,
       s.type AS source_type,
       count(DISTINCT s) AS source_count,
       collect(DISTINCT s.display_name)[0..3] AS sample_sources
ORDER BY w.name, source_count DESC
LIMIT 50
```

### Items Consuming External Sources
```cypher
MATCH (i:FabricItem)-[:CONSUMES]->(s:ExternalSource)
RETURN i.name AS item_name,
       i.type AS item_type,
       collect(s.display_name) AS sources,
       count(s) AS source_count
ORDER BY source_count DESC
LIMIT 20
```

---

## Table Analysis

### Table Statistics by Database
```cypher
MATCH (t:Table)
RETURN t.database AS database, 
       t.schema AS schema,
       count(*) AS table_count
ORDER BY table_count DESC
LIMIT 20
```

### Tables by Fabric Item
```cypher
MATCH (i:FabricItem)-[:MIRRORS]->(t:Table)
RETURN i.name AS fabric_item,
       i.type AS item_type,
       count(t) AS table_count,
       collect(t.name)[0..5] AS sample_tables
ORDER BY table_count DESC
LIMIT 20
```

### Table Full Paths
```cypher
MATCH (t:Table)
RETURN t.full_path AS full_path,
       t.database AS database,
       t.schema AS schema,
       t.name AS table_name
ORDER BY t.database, t.schema, t.name
LIMIT 50
```

---

## Dependency & Lineage Queries

### Dependency Chain Analysis
Find the longest dependency chains in the graph:
```cypher
MATCH path = (leaf:FabricItem)-[:DEPENDS_ON*]->(root)
WHERE NOT EXISTS { MATCH (leaf)<-[:DEPENDS_ON]-() }
AND NOT EXISTS { MATCH (root)-[:DEPENDS_ON]->() }
WITH path, length(path) AS chain_length
ORDER BY chain_length DESC
LIMIT 10
RETURN [n IN nodes(path) | n.name] AS chain, chain_length
```

### Single Points of Failure
Items that are critical bottlenecks (many upstream and downstream dependencies):
```cypher
MATCH (i:FabricItem)
OPTIONAL MATCH (upstream)-[:DEPENDS_ON]->(i)-[:DEPENDS_ON]->(downstream)
WITH i, count(DISTINCT upstream) AS upstream_count, count(DISTINCT downstream) AS downstream_count
WHERE upstream_count > 0 AND downstream_count > 0
RETURN i.name AS bottleneck_item,
       i.type AS item_type,
       upstream_count,
       downstream_count,
       upstream_count * downstream_count AS impact_score
ORDER BY impact_score DESC
LIMIT 20
```

### Full Upstream Lineage
Find all ancestors of a specific item:
```cypher
MATCH path = (item:FabricItem {name: $itemName})-[:DEPENDS_ON|CONSUMES*1..5]->(upstream)
RETURN DISTINCT upstream.name AS name,
       labels(upstream)[0] AS label,
       CASE WHEN upstream:FabricItem THEN upstream.type ELSE upstream.type END AS type,
       length(path) AS distance
ORDER BY distance, name
```

### Full Downstream Impact
Find all descendants of a specific item:
```cypher
MATCH path = (downstream:FabricItem)-[:DEPENDS_ON*1..5]->(item:FabricItem {name: $itemName})
RETURN DISTINCT downstream.name AS name,
       downstream.type AS type,
       length(path) AS distance
ORDER BY distance, name
```

---

## Impact Analysis

### What If Analysis - Remove an Item
Show all items that would be affected if a specific item is removed:
```cypher
MATCH (target:FabricItem {name: $itemName})
OPTIONAL MATCH (dependent:FabricItem)-[:DEPENDS_ON*]->(target)
WITH target, collect(DISTINCT dependent) AS dependents
RETURN target.name AS removed_item,
       size(dependents) AS affected_count,
       [d IN dependents | d.name][0..10] AS sample_affected
```

### Items Depending on External Sources
Identify items that would be affected if external sources become unavailable:
```cypher
MATCH (s:ExternalSource)<-[:CONSUMES]-(i:FabricItem)
WITH s, collect(i.name) AS direct_consumers
OPTIONAL MATCH (dependent:FabricItem)-[:DEPENDS_ON*]->(consumer:FabricItem)-[:CONSUMES]->(s)
WITH s, direct_consumers, collect(DISTINCT dependent.name) AS indirect_dependents
RETURN s.display_name AS external_source,
       s.type AS source_type,
       size(direct_consumers) AS direct_consumer_count,
       size(indirect_dependents) AS indirect_dependent_count,
       direct_consumers[0..5] AS sample_direct,
       indirect_dependents[0..5] AS sample_indirect
ORDER BY direct_consumer_count + size(indirect_dependents) DESC
LIMIT 20
```

---

## Cross-Workspace Analysis

### Cross-Workspace Dependencies
Items that depend on items from different workspaces:
```cypher
MATCH (i1:FabricItem)-[:DEPENDS_ON]->(i2:FabricItem)
MATCH (i1)<-[:CONTAINS]-(w1:Workspace)
MATCH (i2)<-[:CONTAINS]-(w2:Workspace)
WHERE w1 <> w2
RETURN w1.name AS source_workspace,
       i1.name AS source_item,
       i2.name AS target_item,
       w2.name AS target_workspace
ORDER BY w1.name, w2.name
LIMIT 30
```

### Workspace Dependency Matrix
Summarize dependencies between workspaces:
```cypher
MATCH (i1:FabricItem)-[:DEPENDS_ON]->(i2:FabricItem)
MATCH (i1)<-[:CONTAINS]-(w1:Workspace)
MATCH (i2)<-[:CONTAINS]-(w2:Workspace)
WHERE w1 <> w2
RETURN w1.name AS from_workspace,
       w2.name AS to_workspace,
       count(*) AS dependency_count
ORDER BY dependency_count DESC
LIMIT 30
```

### Workspace Clusters
Find tightly coupled workspaces (bidirectional dependencies):
```cypher
MATCH (i1:FabricItem)-[:DEPENDS_ON]-(i2:FabricItem)
MATCH (i1)<-[:CONTAINS]-(w1:Workspace)
MATCH (i2)<-[:CONTAINS]-(w2:Workspace)
WHERE w1 <> w2
WITH w1, w2, count(*) AS connections
WHERE connections >= 3
RETURN w1.name AS workspace_a,
       w2.name AS workspace_b,
       connections
ORDER BY connections DESC
LIMIT 20
```

### Cross-Workspace Data Flow Summary
```cypher
MATCH (w1:Workspace)-[:CONTAINS]->(i1:FabricItem)-[:DEPENDS_ON]->(i2:FabricItem)<-[:CONTAINS]-(w2:Workspace)
WHERE w1 <> w2
WITH w1, w2, collect({from: i1.name, to: i2.name})[0..5] AS sample_flows, count(*) AS flow_count
RETURN w1.name AS exporting_workspace,
       w2.name AS importing_workspace,
       flow_count,
       sample_flows
ORDER BY flow_count DESC
LIMIT 20
```

---

## Data Quality & Governance

### Orphaned Items
Items with no relationships at all:
```cypher
MATCH (i:FabricItem)
WHERE NOT EXISTS { MATCH (i)-[:DEPENDS_ON]-() }
AND NOT EXISTS { MATCH (i)-[:CONSUMES]-() }
AND NOT EXISTS { MATCH (i)-[:MIRRORS]-() }
OPTIONAL MATCH (i)<-[:CONTAINS]-(w:Workspace)
RETURN i.name AS orphaned_item,
       i.type AS item_type,
       w.name AS workspace
ORDER BY w.name, i.type
LIMIT 30
```

### Circular Dependencies
Detect cycles in the dependency graph:
```cypher
MATCH path = (i:FabricItem)-[:DEPENDS_ON*2..10]->(i)
RETURN DISTINCT [n IN nodes(path) | n.name] AS circular_path,
       length(path) AS cycle_length
ORDER BY cycle_length
LIMIT 20
```

### Items Missing Workspace Assignment
```cypher
MATCH (i:FabricItem)
WHERE NOT EXISTS { MATCH (i)<-[:CONTAINS]-(:Workspace) }
RETURN i.name AS unassigned_item,
       i.type AS item_type,
       i.id AS item_id
LIMIT 30
```

### Duplicate Detection
Find items with identical names (potential duplicates):
```cypher
MATCH (i1:FabricItem), (i2:FabricItem)
WHERE i1.id < i2.id 
AND toLower(i1.name) = toLower(i2.name)
OPTIONAL MATCH (i1)<-[:CONTAINS]-(w1:Workspace)
OPTIONAL MATCH (i2)<-[:CONTAINS]-(w2:Workspace)
RETURN i1.name AS item_name,
       w1.name AS workspace_1,
       w2.name AS workspace_2,
       i1.type AS type_1,
       i2.type AS type_2
LIMIT 30
```

---

## Performance & Optimization

### Degree Centrality (Most Connected Nodes)
```cypher
MATCH (i:FabricItem)
OPTIONAL MATCH (i)-[r]-()
WITH i, count(r) AS degree
RETURN i.name AS item_name,
       i.type AS item_type,
       degree
ORDER BY degree DESC
LIMIT 30
```

### Betweenness Analysis (Approximate)
Find bridge nodes that connect different parts of the graph:
```cypher
MATCH (i:FabricItem)
WHERE EXISTS { MATCH (i)-[:DEPENDS_ON]->() }
AND EXISTS { MATCH (i)<-[:DEPENDS_ON]-() }
OPTIONAL MATCH (upstream)-[:DEPENDS_ON]->(i)
OPTIONAL MATCH (i)-[:DEPENDS_ON]->(downstream)
WITH i, 
     count(DISTINCT upstream) AS in_degree,
     count(DISTINCT downstream) AS out_degree
WHERE in_degree > 1 AND out_degree > 1
RETURN i.name AS bridge_item,
       i.type AS item_type,
       in_degree,
       out_degree,
       in_degree * out_degree AS bridge_score
ORDER BY bridge_score DESC
LIMIT 20
```

### Graph Density by Workspace
Measure how interconnected items are within each workspace:
```cypher
MATCH (w:Workspace)-[:CONTAINS]->(i:FabricItem)
WITH w, collect(i) AS items, count(i) AS n
OPTIONAL MATCH (i1:FabricItem)-[r:DEPENDS_ON]->(i2:FabricItem)
WHERE i1 IN items AND i2 IN items
WITH w, n, count(r) AS edges
RETURN w.name AS workspace,
       n AS nodes,
       edges,
       CASE WHEN n > 1 THEN round(2.0 * edges / (n * (n - 1)), 4) ELSE 0 END AS density
ORDER BY density DESC
LIMIT 20
```

---

## Advanced Analytics

### Item Type Co-occurrence
Which item types commonly depend on each other:
```cypher
MATCH (i1:FabricItem)-[:DEPENDS_ON]->(i2:FabricItem)
RETURN i1.type AS source_type,
       i2.type AS target_type,
       count(*) AS occurrence_count
ORDER BY occurrence_count DESC
LIMIT 30
```

### Workspace Maturity Score
Calculate a maturity score based on connectivity:
```cypher
MATCH (w:Workspace)-[:CONTAINS]->(i:FabricItem)
WITH w, count(i) AS item_count
OPTIONAL MATCH (w)-[:CONTAINS]->(i2:FabricItem)-[:DEPENDS_ON]->()
WITH w, item_count, count(DISTINCT i2) AS items_with_deps
OPTIONAL MATCH (w)-[:CONTAINS]->(i3:FabricItem)-[:CONSUMES]->(:ExternalSource)
WITH w, item_count, items_with_deps, count(DISTINCT i3) AS items_with_sources
RETURN w.name AS workspace,
       item_count,
       items_with_deps,
       items_with_sources,
       round(100.0 * (items_with_deps + items_with_sources) / (2 * item_count), 1) AS maturity_score
ORDER BY maturity_score DESC
LIMIT 20
```

### Source Type Diversity by Workspace
```cypher
MATCH (w:Workspace)-[:CONTAINS]->(i:FabricItem)-[:CONSUMES]->(s:ExternalSource)
WITH w, collect(DISTINCT s.type) AS source_types
RETURN w.name AS workspace,
       size(source_types) AS source_type_count,
       source_types
ORDER BY source_type_count DESC
LIMIT 20
```

---

## Utility Queries

### View Existing Indexes
```cypher
SHOW INDEXES
```

### Create Useful Indexes
```cypher
// Index for faster lookups by name
CREATE INDEX fabric_item_name IF NOT EXISTS FOR (i:FabricItem) ON (i.name);
CREATE INDEX workspace_name IF NOT EXISTS FOR (w:Workspace) ON (w.name);
CREATE INDEX external_source_name IF NOT EXISTS FOR (s:ExternalSource) ON (s.display_name);

// Index for type-based filtering
CREATE INDEX fabric_item_type IF NOT EXISTS FOR (i:FabricItem) ON (i.type);
CREATE INDEX external_source_type IF NOT EXISTS FOR (s:ExternalSource) ON (s.type);
```

### Count All Nodes and Relationships
```cypher
MATCH (n)
WITH count(n) AS nodes
MATCH ()-[r]->()
RETURN nodes, count(r) AS relationships
```

### Sample Data from Each Label
```cypher
MATCH (w:Workspace) RETURN w LIMIT 3
UNION ALL
MATCH (i:FabricItem) RETURN i LIMIT 3
UNION ALL
MATCH (s:ExternalSource) RETURN s LIMIT 3
UNION ALL
MATCH (t:Table) RETURN t LIMIT 3
```

---

## Parameterized Queries for API Use

These queries use parameters (e.g., `$workspaceName`, `$itemName`) for use in applications:

### Get Workspace Details
```cypher
MATCH (w:Workspace {name: $workspaceName})
OPTIONAL MATCH (w)-[:CONTAINS]->(i:FabricItem)
WITH w, collect(i) AS items
RETURN w.name AS name,
       w.id AS id,
       size(items) AS item_count,
       [item IN items | {name: item.name, type: item.type}] AS items
```

### Get Item Lineage (Upstream)
```cypher
MATCH path = (item:FabricItem {name: $itemName})-[:DEPENDS_ON|CONSUMES*1..5]->(upstream)
RETURN DISTINCT upstream.name AS name,
       labels(upstream)[0] AS label,
       CASE WHEN upstream:FabricItem THEN upstream.type ELSE upstream.type END AS type,
       length(path) AS distance
ORDER BY distance, name
```

### Get Item Impact (Downstream)
```cypher
MATCH path = (downstream:FabricItem)-[:DEPENDS_ON*1..5]->(item:FabricItem {name: $itemName})
RETURN DISTINCT downstream.name AS name,
       downstream.type AS type,
       length(path) AS distance
ORDER BY distance, name
```

### Search Items by Name Pattern
```cypher
MATCH (i:FabricItem)
WHERE toLower(i.name) CONTAINS toLower($searchTerm)
OPTIONAL MATCH (i)<-[:CONTAINS]-(w:Workspace)
RETURN i.name AS item_name,
       i.type AS item_type,
       w.name AS workspace
ORDER BY i.name
LIMIT 20
```

---

## Notes

- All queries are tested against Neo4j 5.15.0
- Queries use the actual schema from the Lineage Explorer data loader
- For large graphs, consider adding `LIMIT` clauses or indexes for performance
- Use `EXPLAIN` or `PROFILE` before queries to analyze execution plans
