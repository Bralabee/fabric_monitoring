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
        return self.client.run_query("""
            MATCH (t:Table)
            WHERE toLower(t.name) CONTAINS toLower($table_name)
            MATCH (i:FabricItem)-[:MIRRORS]->(t)
            MATCH (w:Workspace)-[:CONTAINS]->(i)
            RETURN 
                t.name as table_name,
                t.schema as table_schema,
                t.database as table_database,
                i.id as item_id,
                i.name as item_name,
                w.name as workspace
            ORDER BY t.database, t.schema, t.name
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
