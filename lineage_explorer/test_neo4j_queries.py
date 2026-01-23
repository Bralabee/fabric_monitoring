#!/usr/bin/env python3
"""
Test Neo4j queries using the CORRECT schema:
- Labels: Workspace, FabricItem, ExternalSource, Table
- Relationships: CONTAINS, DEPENDS_ON, CONSUMES, MIRRORS
- Properties: type (not item_type)
"""

from neo4j import GraphDatabase
from neo4j.exceptions import CypherSyntaxError, ClientError

# Corrected queries matching actual schema
QUERIES = {
    # === DATABASE OVERVIEW ===
    "Quick Schema Overview - Node counts": """
        MATCH (n)
        RETURN labels(n)[0] AS label, count(*) AS count
        ORDER BY count DESC
    """,
    
    "Quick Schema Overview - Relationship counts": """
        MATCH ()-[r]->()
        RETURN type(r) AS relationship_type, count(*) AS count
        ORDER BY count DESC
    """,
    
    # === BASIC STATISTICS ===
    "Total Graph Metrics": """
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
    """,
    
    "Node Distribution": """
        MATCH (n)
        WITH labels(n)[0] AS node_type, count(*) AS count
        RETURN node_type, count
        ORDER BY count DESC
    """,
    
    "Connectivity Statistics": """
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
    """,
    
    # === WORKSPACE ANALYSIS ===
    "Workspace Summary": """
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
    """,
    
    "Workspace Item Type Distribution": """
        MATCH (w:Workspace)-[:CONTAINS]->(i:FabricItem)
        RETURN w.name AS workspace, 
               i.type AS item_type, 
               count(*) AS count
        ORDER BY w.name, count DESC
        LIMIT 50
    """,
    
    "Top Workspaces by Complexity": """
        MATCH (w:Workspace)-[:CONTAINS]->(i:FabricItem)
        OPTIONAL MATCH (i)-[r:DEPENDS_ON|CONSUMES|MIRRORS]-()
        WITH w, count(DISTINCT i) AS items, count(r) AS total_relationships
        RETURN w.name AS workspace,
               items,
               total_relationships,
               round(1.0 * total_relationships / items, 2) AS avg_relationships_per_item
        ORDER BY total_relationships DESC
        LIMIT 20
    """,
    
    "Isolated Workspaces": """
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
    """,
    
    # === ITEM ANALYSIS ===
    "Item Type Statistics": """
        MATCH (i:FabricItem)
        RETURN i.type AS item_type, count(*) AS count
        ORDER BY count DESC
    """,
    
    "Most Connected Items (Hub Nodes)": """
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
    """,
    
    "Leaf Nodes (No Downstream Consumers)": """
        MATCH (i:FabricItem)
        WHERE NOT EXISTS { MATCH (i)<-[:DEPENDS_ON]-() }
        OPTIONAL MATCH (i)<-[:CONTAINS]-(w:Workspace)
        RETURN i.name AS item_name, 
               i.type AS item_type,
               w.name AS workspace
        ORDER BY i.type, i.name
        LIMIT 30
    """,
    
    "Root Nodes (No Upstream Dependencies)": """
        MATCH (i:FabricItem)
        WHERE NOT EXISTS { MATCH (i)-[:DEPENDS_ON]->() }
        AND NOT EXISTS { MATCH (i)-[:CONSUMES]->() }
        OPTIONAL MATCH (i)<-[:CONTAINS]-(w:Workspace)
        RETURN i.name AS item_name, 
               i.type AS item_type,
               w.name AS workspace
        ORDER BY i.type, i.name
        LIMIT 30
    """,
    
    # === EXTERNAL SOURCE ANALYSIS ===
    "External Source Summary": """
        MATCH (s:ExternalSource)
        OPTIONAL MATCH (s)<-[:CONSUMES]-(i:FabricItem)
        RETURN s.display_name AS source_name,
               s.type AS source_type,
               count(DISTINCT i) AS used_by_items
        ORDER BY used_by_items DESC
        LIMIT 30
    """,
    
    "External Source Types Distribution": """
        MATCH (s:ExternalSource)
        RETURN s.type AS source_type, count(*) AS count
        ORDER BY count DESC
    """,
    
    "Most Used External Sources": """
        MATCH (s:ExternalSource)<-[:CONSUMES]-(i:FabricItem)
        WITH s, collect(DISTINCT i.name) AS consumers, count(DISTINCT i) AS consumer_count
        RETURN s.display_name AS source_name,
               s.type AS source_type,
               consumer_count,
               consumers[0..5] AS sample_consumers
        ORDER BY consumer_count DESC
        LIMIT 20
    """,
    
    "Unused External Sources": """
        MATCH (s:ExternalSource)
        WHERE NOT EXISTS { MATCH (s)<-[:CONSUMES]-() }
        RETURN s.display_name AS unused_source,
               s.type AS source_type
        ORDER BY s.type, s.display_name
        LIMIT 30
    """,
    
    "External Sources by Workspace": """
        MATCH (s:ExternalSource)<-[:CONSUMES]-(i:FabricItem)<-[:CONTAINS]-(w:Workspace)
        RETURN w.name AS workspace,
               s.type AS source_type,
               count(DISTINCT s) AS source_count,
               collect(DISTINCT s.display_name)[0..3] AS sample_sources
        ORDER BY w.name, source_count DESC
        LIMIT 50
    """,
    
    # === DEPENDENCY & LINEAGE ===
    "Dependency Chain Analysis": """
        MATCH path = (leaf:FabricItem)-[:DEPENDS_ON*]->(root)
        WHERE NOT EXISTS { MATCH (leaf)<-[:DEPENDS_ON]-() }
        AND NOT EXISTS { MATCH (root)-[:DEPENDS_ON]->() }
        WITH path, length(path) AS chain_length
        ORDER BY chain_length DESC
        LIMIT 10
        RETURN [n IN nodes(path) | n.name] AS chain, chain_length
    """,
    
    "Single Points of Failure": """
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
    """,
    
    # === CROSS-WORKSPACE ANALYSIS ===
    "Cross-Workspace Dependencies": """
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
    """,
    
    "Workspace Dependency Matrix": """
        MATCH (i1:FabricItem)-[:DEPENDS_ON]->(i2:FabricItem)
        MATCH (i1)<-[:CONTAINS]-(w1:Workspace)
        MATCH (i2)<-[:CONTAINS]-(w2:Workspace)
        WHERE w1 <> w2
        RETURN w1.name AS from_workspace,
               w2.name AS to_workspace,
               count(*) AS dependency_count
        ORDER BY dependency_count DESC
        LIMIT 30
    """,
    
    "Workspace Clusters": """
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
    """,
    
    "Cross-Workspace Data Flow Summary": """
        MATCH (w1:Workspace)-[:CONTAINS]->(i1:FabricItem)-[:DEPENDS_ON]->(i2:FabricItem)<-[:CONTAINS]-(w2:Workspace)
        WHERE w1 <> w2
        WITH w1, w2, collect({from: i1.name, to: i2.name})[0..5] AS sample_flows, count(*) AS flow_count
        RETURN w1.name AS exporting_workspace,
               w2.name AS importing_workspace,
               flow_count,
               sample_flows
        ORDER BY flow_count DESC
        LIMIT 20
    """,
    
    # === DATA QUALITY & GOVERNANCE ===
    "Orphaned Items": """
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
    """,
    
    "Circular Dependencies": """
        MATCH path = (i:FabricItem)-[:DEPENDS_ON*2..10]->(i)
        RETURN DISTINCT [n IN nodes(path) | n.name] AS circular_path,
               length(path) AS cycle_length
        ORDER BY cycle_length
        LIMIT 20
    """,
    
    "Items Missing Workspace Assignment": """
        MATCH (i:FabricItem)
        WHERE NOT EXISTS { MATCH (i)<-[:CONTAINS]-(:Workspace) }
        RETURN i.name AS unassigned_item,
               i.type AS item_type,
               i.id AS item_id
        LIMIT 30
    """,
    
    "Duplicate Detection": """
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
    """,
    
    # === PERFORMANCE & OPTIMIZATION ===
    "Degree Centrality (Most Connected Nodes)": """
        MATCH (i:FabricItem)
        OPTIONAL MATCH (i)-[r]-()
        WITH i, count(r) AS degree
        RETURN i.name AS item_name,
               i.type AS item_type,
               degree
        ORDER BY degree DESC
        LIMIT 30
    """,
    
    "Betweenness Analysis (Approximate)": """
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
    """,
    
    "Graph Density by Workspace": """
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
    """,
    
    # === ADVANCED ANALYTICS ===
    "Item Type Co-occurrence": """
        MATCH (i1:FabricItem)-[:DEPENDS_ON]->(i2:FabricItem)
        RETURN i1.type AS source_type,
               i2.type AS target_type,
               count(*) AS occurrence_count
        ORDER BY occurrence_count DESC
        LIMIT 30
    """,
    
    "Workspace Maturity Score": """
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
    """,
    
    # === UTILITY QUERIES ===
    "View Existing Indexes": """
        SHOW INDEXES
    """,
    
    # === TABLE ANALYSIS ===
    "Table Statistics": """
        MATCH (t:Table)
        RETURN t.database AS database, 
               t.schema AS schema,
               count(*) AS table_count
        ORDER BY table_count DESC
        LIMIT 20
    """,
    
    "Tables by Item (via MIRRORS)": """
        MATCH (i:FabricItem)-[:MIRRORS]->(t:Table)
        RETURN i.name AS fabric_item,
               i.type AS item_type,
               count(t) AS table_count,
               collect(t.name)[0..5] AS sample_tables
        ORDER BY table_count DESC
        LIMIT 20
    """,
    
    "Items Consuming External Sources": """
        MATCH (i:FabricItem)-[:CONSUMES]->(s:ExternalSource)
        RETURN i.name AS item_name,
               i.type AS item_type,
               collect(s.display_name) AS sources,
               count(s) AS source_count
        ORDER BY source_count DESC
        LIMIT 20
    """,
}


def run_tests():
    driver = GraphDatabase.driver(
        "bolt://localhost:7687",
        auth=("neo4j", "fabric_lineage_2026")
    )
    
    print("=" * 80)
    print("Testing Neo4j Queries (Corrected Schema)")
    print("Labels: Workspace, FabricItem, ExternalSource, Table")
    print("Relationships: CONTAINS, DEPENDS_ON, CONSUMES, MIRRORS")
    print("=" * 80)
    print()
    
    passed = 0
    failed = 0
    failed_queries = []
    
    with driver.session() as session:
        for name, query in QUERIES.items():
            try:
                result = session.run(query)
                records = list(result)
                print(f"✅ PASS ({len(records)} records) - {name}")
                passed += 1
            except CypherSyntaxError as e:
                print(f"❌ FAIL: Syntax Error: {e.message[:100]} - {name}")
                failed += 1
                failed_queries.append((name, f"Syntax: {e.message[:100]}"))
            except ClientError as e:
                print(f"❌ FAIL: Client Error: {e.message[:100]} - {name}")
                failed += 1
                failed_queries.append((name, f"Client: {e.message[:100]}"))
            except Exception as e:
                print(f"❌ FAIL: {type(e).__name__}: {str(e)[:100]} - {name}")
                failed += 1
                failed_queries.append((name, f"{type(e).__name__}: {str(e)[:100]}"))
    
    driver.close()
    
    print()
    print("=" * 80)
    print(f"SUMMARY: {passed} passed, {failed} failed out of {len(QUERIES)} queries")
    print("=" * 80)
    
    if failed_queries:
        print("\nFailed queries:")
        for name, error in failed_queries:
            print(f"  - {name}: {error}")
    
    return failed == 0


if __name__ == "__main__":
    import sys
    success = run_tests()
    sys.exit(0 if success else 1)
