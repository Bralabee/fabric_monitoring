"""
Lineage Query Library for Neo4j
-------------------------------
Pre-built Cypher queries for common lineage analysis patterns.

Query Categories:
- Traversal: Find paths, dependencies, impacts
- Statistics: Counts, distributions, aggregations
- Discovery: Find patterns, anomalies, clusters
"""

from typing import Any, Dict, List, Optional
import logging

from .neo4j_client import Neo4jClient

logger = logging.getLogger(__name__)


class LineageQueries:
    """
    Pre-built queries for lineage analysis.
    
    All queries return results as list of dicts.
    Use client.run_query() directly for custom queries.
    """
    
    def __init__(self, client: Neo4jClient):
        """
        Initialize query helper.
        
        Args:
            client: Connected Neo4jClient instance
        """
        self.client = client
    
    # ==================== STATISTICS ====================
    
    def get_overview_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive overview statistics.
        
        Returns:
            Dict with counts by type, connections summary, etc.
        """
        result = self.client.run_query_single("""
            MATCH (w:Workspace) WITH count(w) as workspaces
            MATCH (i:FabricItem) WITH workspaces, count(i) as items
            MATCH (s:ExternalSource) WITH workspaces, items, count(s) as sources
            MATCH (t:Table) WITH workspaces, items, sources, count(t) as tables
            MATCH ()-[r:DEPENDS_ON]->() WITH workspaces, items, sources, tables, count(r) as internal_deps
            MATCH ()-[r:CONSUMES]->() WITH workspaces, items, sources, tables, internal_deps, count(r) as external_deps
            MATCH ()-[r:MIRRORS]->() 
            RETURN workspaces, items, sources, tables, internal_deps, external_deps, count(r) as mirror_deps
        """)
        return result or {}
    
    def get_items_by_type(self) -> List[Dict[str, Any]]:
        """Get count of items by type."""
        return self.client.run_query("""
            MATCH (i:FabricItem)
            RETURN i.type as type, count(i) as count
            ORDER BY count DESC
        """)
    
    def get_sources_by_type(self) -> List[Dict[str, Any]]:
        """Get count of external sources by type."""
        return self.client.run_query("""
            MATCH (s:ExternalSource)
            RETURN s.type as type, count(s) as count
            ORDER BY count DESC
        """)
    
    def get_workspace_stats(self) -> List[Dict[str, Any]]:
        """
        Get statistics per workspace.
        
        Returns:
            List of workspaces with item counts, dependency counts, etc.
        """
        return self.client.run_query("""
            MATCH (w:Workspace)
            OPTIONAL MATCH (w)-[:CONTAINS]->(i:FabricItem)
            OPTIONAL MATCH (i)-[dep:DEPENDS_ON]->()
            OPTIONAL MATCH (i)-[con:CONSUMES]->()
            RETURN 
                w.id as workspace_id,
                w.name as workspace_name,
                count(DISTINCT i) as item_count,
                count(DISTINCT dep) as internal_deps,
                count(DISTINCT con) as external_deps
            ORDER BY item_count DESC
        """)
    
    def get_top_connected_items(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get items with most incoming connections (most depended on).
        
        Args:
            limit: Max items to return
            
        Returns:
            List of items with connection counts
        """
        return self.client.run_query("""
            MATCH (target:FabricItem)<-[r:DEPENDS_ON]-()
            WITH target, count(r) as incoming
            MATCH (w:Workspace)-[:CONTAINS]->(target)
            RETURN 
                target.id as item_id,
                target.name as item_name,
                target.type as item_type,
                w.name as workspace,
                incoming as incoming_connections
            ORDER BY incoming DESC
            LIMIT $limit
        """, {"limit": limit})
    
    def get_top_external_sources(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get external sources with most consumers.
        
        Args:
            limit: Max sources to return
            
        Returns:
            List of sources with consumer counts
        """
        return self.client.run_query("""
            MATCH (s:ExternalSource)<-[r:CONSUMES]-()
            RETURN 
                s.id as source_id,
                s.display_name as display_name,
                s.type as source_type,
                s.location as location,
                count(r) as consumer_count
            ORDER BY consumer_count DESC
            LIMIT $limit
        """, {"limit": limit})
    
    # ==================== TRAVERSAL ====================
    
    def find_upstream_dependencies(
        self,
        item_id: str,
        max_depth: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Find all upstream dependencies of an item.
        
        Args:
            item_id: Target item ID
            max_depth: Maximum traversal depth
            
        Returns:
            List of upstream items with path info
        """
        return self.client.run_query("""
            MATCH path = (target:FabricItem {id: $item_id})-[:DEPENDS_ON*1..10]->(upstream)
            WHERE length(path) <= $max_depth
            RETURN 
                upstream.id as id,
                upstream.name as name,
                upstream.type as type,
                length(path) as depth,
                [n in nodes(path) | n.name] as path_names
            ORDER BY depth
        """, {"item_id": item_id, "max_depth": max_depth})
    
    def find_downstream_impact(
        self,
        item_id: str,
        max_depth: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Find all items that depend on this item (impact analysis).
        
        Args:
            item_id: Source item ID
            max_depth: Maximum traversal depth
            
        Returns:
            List of downstream items that would be impacted
        """
        return self.client.run_query("""
            MATCH path = (source:FabricItem {id: $item_id})<-[:DEPENDS_ON*1..10]-(downstream)
            WHERE length(path) <= $max_depth
            RETURN 
                downstream.id as id,
                downstream.name as name,
                downstream.type as type,
                length(path) as depth,
                [n in nodes(path) | n.name] as path_names
            ORDER BY depth
        """, {"item_id": item_id, "max_depth": max_depth})
    
    def find_path_between(
        self,
        source_id: str,
        target_id: str,
        max_depth: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Find all paths between two items.
        
        Args:
            source_id: Starting item ID
            target_id: Ending item ID
            max_depth: Maximum path length (note: actual limit capped at 15 for performance)
            
        Returns:
            List of paths with node names
        """
        # Note: Neo4j doesn't support parameterized path lengths in shortestPath
        # Using a reasonable fixed upper bound for performance
        return self.client.run_query("""
            MATCH path = shortestPath(
                (source {id: $source_id})-[*1..15]-(target {id: $target_id})
            )
            WHERE length(path) <= $max_depth
            RETURN 
                length(path) as path_length,
                [n in nodes(path) | {id: n.id, name: n.name, type: coalesce(n.type, labels(n)[0])}] as path,
                [r in relationships(path) | type(r)] as relationship_types
        """, {"source_id": source_id, "target_id": target_id, "max_depth": max_depth})
    
    def get_external_source_consumers(
        self,
        source_id: str
    ) -> List[Dict[str, Any]]:
        """
        Get all items that consume an external source.
        
        Args:
            source_id: External source ID
            
        Returns:
            List of consuming items with workspace info
        """
        return self.client.run_query("""
            MATCH (s:ExternalSource {id: $source_id})<-[:CONSUMES]-(i:FabricItem)
            MATCH (w:Workspace)-[:CONTAINS]->(i)
            RETURN 
                i.id as item_id,
                i.name as item_name,
                i.type as item_type,
                w.name as workspace,
                w.id as workspace_id
            ORDER BY w.name, i.name
        """, {"source_id": source_id})
    
    def find_items_using_table(
        self,
        table_name: str
    ) -> List[Dict[str, Any]]:
        """
        Find items that mirror or depend on a specific table.
        
        Args:
            table_name: Table name (partial match supported)
            
        Returns:
            List of items using the table
        """
        # Search both MIRRORS and USES_TABLE relationships
        return self.client.run_query("""
            MATCH (t:Table)
            WHERE toLower(t.name) CONTAINS toLower($table_name)
            // Find items that either MIRROR or USE the table
            OPTIONAL MATCH (mirror:FabricItem)-[:MIRRORS]->(t)
            OPTIONAL MATCH (user:FabricItem)-[:USES_TABLE]->(t)
            // Get workspace for each item type
            OPTIONAL MATCH (mw:Workspace)-[:CONTAINS]->(mirror)
            OPTIONAL MATCH (uw:Workspace)-[:CONTAINS]->(user)
            // Return combined results
            WITH t, 
                 CASE WHEN mirror IS NOT NULL THEN {
                     table_name: t.name,
                     table_schema: t.schema,
                     table_database: t.database,
                     item_id: mirror.id,
                     item_name: mirror.name,
                     item_type: mirror.type,
                     workspace: mw.name,
                     relationship: 'MIRRORS'
                 } ELSE NULL END as mirror_result,
                 CASE WHEN user IS NOT NULL THEN {
                     table_name: t.name,
                     table_schema: t.schema,
                     table_database: t.database,
                     item_id: user.id,
                     item_name: user.name,
                     item_type: user.type,
                     workspace: uw.name,
                     relationship: 'USES_TABLE'
                 } ELSE NULL END as use_result
            // Collect all results
            WITH collect(DISTINCT mirror_result) + collect(DISTINCT use_result) as results
            UNWIND results as r
            WITH r WHERE r IS NOT NULL
            RETURN 
                r.table_name as table_name,
                r.table_schema as table_schema,
                r.table_database as table_database,
                r.item_id as item_id,
                r.item_name as item_name,
                r.item_type as item_type,
                r.workspace as workspace,
                r.relationship as relationship
            ORDER BY r.table_database, r.table_schema, r.table_name
        """, {"table_name": table_name})

    
    # ==================== DISCOVERY ====================
    
    def find_orphan_items(self) -> List[Dict[str, Any]]:
        """
        Find items with no dependencies (neither consuming nor providing).
        
        Returns:
            List of isolated items
        """
        return self.client.run_query("""
            MATCH (i:FabricItem)
            WHERE NOT (i)-[:DEPENDS_ON]->() 
              AND NOT (i)<-[:DEPENDS_ON]-()
              AND NOT (i)-[:CONSUMES]->()
            MATCH (w:Workspace)-[:CONTAINS]->(i)
            RETURN 
                i.id as item_id,
                i.name as item_name,
                i.type as item_type,
                w.name as workspace
            ORDER BY w.name, i.name
        """)
    
    def find_cross_workspace_dependencies(self) -> List[Dict[str, Any]]:
        """
        Find dependencies that cross workspace boundaries.
        
        Returns:
            List of cross-workspace dependency edges
        """
        return self.client.run_query("""
            MATCH (w1:Workspace)-[:CONTAINS]->(i1:FabricItem)-[:DEPENDS_ON]->(i2:FabricItem)<-[:CONTAINS]-(w2:Workspace)
            WHERE w1.id <> w2.id
            RETURN 
                w1.name as source_workspace,
                i1.name as source_item,
                i1.type as source_type,
                w2.name as target_workspace,
                i2.name as target_item,
                i2.type as target_type
            ORDER BY w1.name, w2.name
        """)
    
    def find_circular_dependencies(self, max_depth: int = 5) -> List[Dict[str, Any]]:
        """
        Find circular dependency chains.
        
        Args:
            max_depth: Maximum cycle length to detect
            
        Returns:
            List of circular dependency paths
        """
        return self.client.run_query("""
            MATCH path = (i:FabricItem)-[:DEPENDS_ON*2..$max_depth]->(i)
            RETURN DISTINCT
                [n in nodes(path) | n.name] as cycle_nodes,
                length(path) as cycle_length
            ORDER BY cycle_length
            LIMIT 20
        """, {"max_depth": max_depth})
    
    def find_items_by_external_source_type(
        self,
        source_type: str
    ) -> List[Dict[str, Any]]:
        """
        Find all items consuming a specific type of external source.
        
        Args:
            source_type: External source type (e.g., 'Snowflake', 'AdlsGen2')
            
        Returns:
            List of items and their source details
        """
        return self.client.run_query("""
            MATCH (s:ExternalSource {type: $source_type})<-[:CONSUMES]-(i:FabricItem)
            MATCH (w:Workspace)-[:CONTAINS]->(i)
            RETURN 
                i.id as item_id,
                i.name as item_name,
                i.type as item_type,
                w.name as workspace,
                s.display_name as source_name,
                s.location as source_location,
                s.database as source_database
            ORDER BY w.name, i.name
        """, {"source_type": source_type})
    
    def get_workspace_lineage(
        self,
        workspace_name: str
    ) -> Dict[str, Any]:
        """
        Get complete lineage view for a workspace.
        
        Args:
            workspace_name: Workspace name
            
        Returns:
            Dict with items, internal deps, external sources
        """
        items = self.client.run_query("""
            MATCH (w:Workspace)
            WHERE toLower(w.name) CONTAINS toLower($workspace_name)
            MATCH (w)-[:CONTAINS]->(i:FabricItem)
            OPTIONAL MATCH (i)-[:DEPENDS_ON]->(upstream:FabricItem)
            OPTIONAL MATCH (downstream:FabricItem)-[:DEPENDS_ON]->(i)
            RETURN 
                i.id as id,
                i.name as name,
                i.type as type,
                collect(DISTINCT upstream.name) as depends_on,
                collect(DISTINCT downstream.name) as depended_by
            ORDER BY i.name
        """, {"workspace_name": workspace_name})
        
        external_deps = self.client.run_query("""
            MATCH (w:Workspace)
            WHERE toLower(w.name) CONTAINS toLower($workspace_name)
            MATCH (w)-[:CONTAINS]->(i:FabricItem)-[:CONSUMES]->(s:ExternalSource)
            RETURN 
                i.name as item_name,
                s.type as source_type,
                s.display_name as source_name,
                s.location as location
            ORDER BY i.name
        """, {"workspace_name": workspace_name})
        
        return {
            "items": items,
            "external_dependencies": external_deps
        }
    
    # ==================== SEARCH ====================
    
    def search_items(
        self,
        query: str,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Full-text search across items, sources, and tables.
        
        Args:
            query: Search term
            limit: Max results
            
        Returns:
            List of matching entities
        """
        return self.client.run_query("""
            CALL {
                MATCH (i:FabricItem)
                WHERE toLower(i.name) CONTAINS toLower($query)
                RETURN 'item' as entity_type, i.id as id, i.name as name, i.type as type, null as location
                UNION
                MATCH (s:ExternalSource)
                WHERE toLower(s.display_name) CONTAINS toLower($query)
                   OR toLower(s.location) CONTAINS toLower($query)
                RETURN 'source' as entity_type, s.id as id, s.display_name as name, s.type as type, s.location as location
                UNION
                MATCH (t:Table)
                WHERE toLower(t.name) CONTAINS toLower($query)
                   OR toLower(t.full_path) CONTAINS toLower($query)
                RETURN 'table' as entity_type, t.id as id, t.name as name, 'Table' as type, t.full_path as location
            }
            RETURN entity_type, id, name, type, location
            LIMIT $limit
        """, {"query": query, "limit": limit})
    
    # ==================== ADVANCED ANALYSIS ====================
    
    def calculate_item_centrality(self, top_n: int = 20) -> List[Dict[str, Any]]:
        """
        Calculate centrality scores for items (how "central" they are in the graph).
        Items with high centrality are critical junction points.
        
        Args:
            top_n: Number of top items to return
            
        Returns:
            List of items with their centrality metrics
        """
        return self.client.run_query("""
            MATCH (i:FabricItem)
            OPTIONAL MATCH (i)-[:DEPENDS_ON]->(out)
            OPTIONAL MATCH (in)-[:DEPENDS_ON]->(i)
            OPTIONAL MATCH (i)-[:CONSUMES]->(ext)
            WITH i,
                 count(DISTINCT out) as out_degree,
                 count(DISTINCT in) as in_degree,
                 count(DISTINCT ext) as ext_degree
            WITH i, 
                 out_degree + in_degree as total_internal,
                 ext_degree,
                 in_degree as importance_score
            MATCH (w:Workspace)-[:CONTAINS]->(i)
            RETURN 
                i.id as item_id,
                i.name as item_name,
                i.type as item_type,
                w.name as workspace,
                total_internal as internal_connections,
                ext_degree as external_connections,
                importance_score
            ORDER BY importance_score DESC
            LIMIT $top_n
        """, {"top_n": top_n})
    
    def get_data_flow_summary(self) -> Dict[str, Any]:
        """
        Generate a summary of data flow patterns.
        
        Returns:
            Summary with source types, transformation layers, sinks
        """
        sources = self.client.run_query("""
            MATCH (s:ExternalSource)<-[:CONSUMES]-(i:FabricItem)
            RETURN s.type as source_type, count(DISTINCT i) as consumer_count
            ORDER BY consumer_count DESC
        """)
        
        item_layers = self.client.run_query("""
            MATCH path = (s:ExternalSource)<-[:CONSUMES]-(i:FabricItem)
            WITH i, 1 as layer
            OPTIONAL MATCH (i)<-[:DEPENDS_ON*1..5]-(downstream)
            RETURN 
                i.type as item_type,
                CASE 
                    WHEN downstream IS NULL THEN 'leaf'
                    ELSE 'intermediate'
                END as position,
                count(*) as count
        """)
        
        return {
            "external_sources": sources,
            "item_distribution": item_layers
        }
    
    # ==================== TABLE-LEVEL LINEAGE ====================
    
    def get_table_lineage(self) -> Dict[str, Any]:
        """
        Get comprehensive table-level lineage graph.
        
        Returns all tables (from shortcuts and mirrored databases) with their
        relationships to Fabric items.
        
        Returns:
            Dict with tables, edges, and summary statistics
        """
        # Get all tables with their relationships
        tables = self.client.run_query("""
            MATCH (t:Table)
            OPTIONAL MATCH (provider:FabricItem)-[:PROVIDES_TABLE]->(t)
            OPTIONAL MATCH (consumer:FabricItem)-[:USES_TABLE]->(t)
            OPTIONAL MATCH (mirror:FabricItem)-[:MIRRORS]->(t)
            RETURN 
                t.id as table_id,
                t.name as table_name,
                t.full_path as full_path,
                t.database as database,
                t.schema as schema,
                t.table_type as table_type,
                t.status as status,
                t.processed_rows as processed_rows,
                t.last_sync as last_sync,
                collect(DISTINCT provider.name) as provided_by,
                collect(DISTINCT consumer.name) as used_by,
                collect(DISTINCT mirror.name) as mirrored_by
            ORDER BY t.name
        """)
        
        # Get table-to-table flow edges (via shortcuts)
        table_flows = self.client.run_query("""
            MATCH (source:FabricItem)-[:PROVIDES_TABLE]->(t:Table)<-[:USES_TABLE]-(target:FabricItem)
            MATCH (sw:Workspace)-[:CONTAINS]->(source)
            MATCH (tw:Workspace)-[:CONTAINS]->(target)
            RETURN 
                t.name as table_name,
                t.schema as schema,
                source.name as source_item,
                sw.name as source_workspace,
                target.name as target_item,
                tw.name as target_workspace
            ORDER BY t.schema, t.name
        """)
        
        # Summary stats
        stats = self.client.run_query_single("""
            MATCH (t:Table)
            WITH count(t) as total_tables
            OPTIONAL MATCH (t:Table {table_type: 'shortcut'})
            WITH total_tables, count(t) as shortcut_tables
            OPTIONAL MATCH (t:Table) WHERE t.database IS NOT NULL
            RETURN total_tables, shortcut_tables, count(t) as mirrored_tables
        """) or {}
        
        return {
            "tables": tables,
            "table_flows": table_flows,
            "summary": {
                "total_tables": stats.get("total_tables", 0),
                "shortcut_tables": stats.get("shortcut_tables", 0),
                "mirrored_tables": stats.get("mirrored_tables", 0)
            }
        }
    
    def get_table_dependencies(self, table_name: str) -> Dict[str, Any]:
        """
        Get detailed dependencies for a specific table.
        
        Args:
            table_name: Table name (partial match supported)
            
        Returns:
            Dict with upstream and downstream dependencies
        """
        result = self.client.run_query("""
            MATCH (t:Table)
            WHERE toLower(t.name) CONTAINS toLower($table_name)
            OPTIONAL MATCH (provider:FabricItem)-[:PROVIDES_TABLE]->(t)
            OPTIONAL MATCH (consumer:FabricItem)-[:USES_TABLE]->(t)
            OPTIONAL MATCH (mirror:FabricItem)-[:MIRRORS]->(t)
            OPTIONAL MATCH (pw:Workspace)-[:CONTAINS]->(provider)
            OPTIONAL MATCH (cw:Workspace)-[:CONTAINS]->(consumer)
            OPTIONAL MATCH (mw:Workspace)-[:CONTAINS]->(mirror)
            RETURN 
                t.id as table_id,
                t.name as table_name,
                t.schema as schema,
                t.full_path as full_path,
                t.database as database,
                t.table_type as table_type,
                t.status as status,
                t.processed_rows as processed_rows,
                t.last_sync as last_sync,
                [p IN collect(DISTINCT {name: provider.name, workspace: pw.name, type: provider.type}) WHERE p.name IS NOT NULL] as providers,
                [c IN collect(DISTINCT {name: consumer.name, workspace: cw.name, type: consumer.type}) WHERE c.name IS NOT NULL] as consumers,
                [m IN collect(DISTINCT {name: mirror.name, workspace: mw.name, type: mirror.type}) WHERE m.name IS NOT NULL] as mirrors
        """, {"table_name": table_name})
        
        return {
            "tables": result,
            "query": table_name
        }

    def get_table_impact_analysis(
        self,
        table_name: str,
        max_depth: int = 5
    ) -> Dict[str, Any]:
        """
        Get comprehensive impact analysis for a table.
        
        This is the key method for Speaker 2's use case:
        "Search for a table name and see all downstream dependencies"
        
        Finds:
        1. Items that directly MIRROR the table (MirroredDatabases)
        2. Items that USE_TABLE (via OneLake shortcuts)
        3. All downstream DEPENDS_ON chains from those items
        4. Cross-workspace impacts
        
        Args:
            table_name: Table name (partial match supported)
            max_depth: Maximum traversal depth for dependencies (1-10)
            
        Returns:
            Dict with table info, direct consumers, and full downstream impact
        """
        # First, find matching tables
        tables = self.client.run_query("""
            MATCH (t:Table)
            WHERE toLower(t.name) CONTAINS toLower($table_name)
            RETURN 
                t.id as table_id,
                t.name as table_name,
                t.database as database,
                t.schema as schema,
                t.full_path as full_path,
                t.table_type as table_type,
                t.status as status,
                t.processed_rows as processed_rows,
                t.last_sync as last_sync
            ORDER BY t.name
            LIMIT 20
        """, {"table_name": table_name})
        
        if not tables:
            return {
                "query": table_name,
                "tables_found": 0,
                "message": f"No tables found matching '{table_name}'"
            }
        
        # For each table, find direct consumers and downstream impact
        results = []
        all_impacted_items = set()
        all_impacted_workspaces = set()
        
        for table in tables:
            table_id = table.get("table_id")
            
            # Find items that directly consume this table (MIRRORS or USES_TABLE)
            direct_consumers = self.client.run_query("""
                MATCH (t:Table {id: $table_id})
                OPTIONAL MATCH (mirror:FabricItem)-[:MIRRORS]->(t)
                OPTIONAL MATCH (mw:Workspace)-[:CONTAINS]->(mirror)
                OPTIONAL MATCH (user:FabricItem)-[:USES_TABLE]->(t)
                OPTIONAL MATCH (uw:Workspace)-[:CONTAINS]->(user)
                WITH t, 
                     collect(DISTINCT {
                         item_id: mirror.id,
                         item_name: mirror.name, 
                         item_type: mirror.type, 
                         workspace: mw.name,
                         relationship: 'MIRRORS'
                     }) as mirrors,
                     collect(DISTINCT {
                         item_id: user.id,
                         item_name: user.name, 
                         item_type: user.type, 
                         workspace: uw.name,
                         relationship: 'USES_TABLE'
                     }) as users
                RETURN mirrors, users
            """, {"table_id": table_id})
            
            # Flatten and filter null items
            direct = []
            consumer_ids = []
            if direct_consumers:
                for row in direct_consumers:
                    for m in row.get("mirrors", []):
                        if m.get("item_name"):
                            direct.append(m)
                            consumer_ids.append(m.get("item_id"))
                            all_impacted_items.add(m.get("item_name"))
                            if m.get("workspace"):
                                all_impacted_workspaces.add(m.get("workspace"))
                    for u in row.get("users", []):
                        if u.get("item_name"):
                            direct.append(u)
                            consumer_ids.append(u.get("item_id"))
                            all_impacted_items.add(u.get("item_name"))
                            if u.get("workspace"):
                                all_impacted_workspaces.add(u.get("workspace"))
            
            # Find downstream impact from each direct consumer
            downstream = []
            if consumer_ids:
                for consumer_id in consumer_ids:
                    if not consumer_id:
                        continue
                    downstream_items = self.client.run_query("""
                        MATCH (start:FabricItem {id: $consumer_id})
                        OPTIONAL MATCH path = (start)<-[:DEPENDS_ON*1..10]-(downstream:FabricItem)
                        WHERE length(path) <= $max_depth
                        OPTIONAL MATCH (dw:Workspace)-[:CONTAINS]->(downstream)
                        RETURN DISTINCT
                            downstream.id as item_id,
                            downstream.name as item_name,
                            downstream.type as item_type,
                            dw.name as workspace,
                            length(path) as depth
                        ORDER BY depth
                    """, {"consumer_id": consumer_id, "max_depth": max_depth})
                    
                    for d in downstream_items:
                        if d.get("item_name"):
                            downstream.append(d)
                            all_impacted_items.add(d.get("item_name"))
                            if d.get("workspace"):
                                all_impacted_workspaces.add(d.get("workspace"))
            
            results.append({
                "table": table,
                "direct_consumers": direct,
                "downstream_impact": downstream
            })
        
        # Calculate max depth across all results
        max_found_depth = 0
        for r in results:
            for d in r.get("downstream_impact", []):
                if d.get("depth", 0) > max_found_depth:
                    max_found_depth = d.get("depth")
        
        return {
            "query": table_name,
            "tables_found": len(tables),
            "results": results,
            "summary": {
                "total_impacted_items": len(all_impacted_items),
                "impacted_workspaces": list(all_impacted_workspaces),
                "total_impacted_workspaces": len(all_impacted_workspaces),
                "max_depth_found": max_found_depth
            }
        }

    # ==================== CHAIN DEPTH ANALYSIS ====================
    
    def get_deep_chains(self, min_depth: int = 3) -> List[Dict[str, Any]]:
        """
        Find all dependency chains with depth >= min_depth.
        
        This method identifies the deepest upstream dependency chains in the graph,
        helping users understand the full interdependency paths from data sources
        to consuming items.
        
        Args:
            min_depth: Minimum chain depth to include (default: 3)
            
        Returns:
            List of chains with path, depth, start/end nodes, sorted deepest first
        """
        return self.client.run_query("""
            // Find all terminal items (no outgoing DEPENDS_ON)
            MATCH (terminal:FabricItem)
            WHERE NOT (terminal)-[:DEPENDS_ON]->()
            
            // Trace upstream chains from terminal items
            MATCH path = (terminal)-[:DEPENDS_ON*2..10]->(upstream)
            WHERE length(path) >= $min_depth
            
            // Get workspace info
            OPTIONAL MATCH (tw:Workspace)-[:CONTAINS]->(terminal)
            OPTIONAL MATCH (uw:Workspace)-[:CONTAINS]->(upstream)
            
            WITH path, terminal, upstream, tw, uw,
                 length(path) as depth,
                 [n in nodes(path) | n.name] as chain_names,
                 [n in nodes(path) | {id: n.id, name: n.name, type: n.type}] as chain_nodes
            
            RETURN DISTINCT
                terminal.id as terminal_id,
                terminal.name as terminal_name,
                terminal.type as terminal_type,
                tw.name as terminal_workspace,
                upstream.id as origin_id,
                upstream.name as origin_name,
                upstream.type as origin_type,
                uw.name as origin_workspace,
                depth,
                chain_names,
                chain_nodes
            ORDER BY depth DESC
            LIMIT 50
        """, {"min_depth": min_depth})
    
    def get_node_chain_depths(self) -> List[Dict[str, Any]]:
        """
        Calculate chain depth metrics for all Fabric items.
        
        For each item, computes:
        - upstream_depth: Maximum depth of upstream dependencies
        - downstream_depth: Maximum depth of downstream dependents
        - total_chain_depth: Sum representing overall centrality in chain flow
        
        This is used for the UI "Color by Chain Depth" feature.
        
        Returns:
            List of items with their chain depth metrics
        """
        return self.client.run_query("""
            MATCH (item:FabricItem)
            
            // Calculate max upstream depth
            OPTIONAL MATCH upstream_path = (item)-[:DEPENDS_ON*1..10]->(upstream)
            WITH item, max(length(upstream_path)) as upstream_depth
            
            // Calculate max downstream depth
            OPTIONAL MATCH downstream_path = (item)<-[:DEPENDS_ON*1..10]-(downstream)
            WITH item, 
                 coalesce(upstream_depth, 0) as upstream_depth,
                 max(length(downstream_path)) as downstream_depth_raw
            
            WITH item,
                 upstream_depth,
                 coalesce(downstream_depth_raw, 0) as downstream_depth
            
            RETURN 
                item.id as item_id,
                item.name as item_name,
                item.type as item_type,
                upstream_depth,
                downstream_depth,
                upstream_depth + downstream_depth as total_chain_depth
            ORDER BY total_chain_depth DESC
        """)
    
    def get_chain_depth_stats(self) -> Dict[str, Any]:
        """
        Get summary statistics about chain depths in the graph.
        
        Returns:
            Dict with max_depth, avg_depth, chain_count, depth_distribution
        """
        stats = self.client.run_query_single("""
            MATCH path = (start:FabricItem)-[:DEPENDS_ON*2..10]->(end)
            WHERE NOT (end)-[:DEPENDS_ON]->()
            WITH length(path) as depth
            RETURN 
                max(depth) as max_depth,
                avg(depth) as avg_depth,
                count(*) as chain_count
        """) or {"max_depth": 0, "avg_depth": 0, "chain_count": 0}
        
        # Get depth distribution
        distribution = self.client.run_query("""
            MATCH path = (start:FabricItem)-[:DEPENDS_ON*2..10]->(end)
            WHERE NOT (end)-[:DEPENDS_ON]->()
            WITH length(path) as depth
            RETURN depth, count(*) as count
            ORDER BY depth
        """)
        
        return {
            "max_depth": stats.get("max_depth", 0),
            "avg_depth": round(stats.get("avg_depth", 0) or 0, 2),
            "chain_count": stats.get("chain_count", 0),
            "depth_distribution": distribution
        }
