"""
Lineage Analyzer - Comprehensive Relationship Exploration

This module provides code-driven exploration of lineage relationships,
allowing you to answer specific questions like:
- What tables within a schema depend on X, Y, Z?
- What does a given Lakehouse/Warehouse/Mirror relate to?
- What external sources feed into a workspace?
- What's the full upstream/downstream chain for any item?
"""

from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Set
from collections import defaultdict
import json
import logging

logger = logging.getLogger(__name__)


@dataclass
class DependencyInfo:
    """Detailed dependency information for an item."""
    item_id: str
    item_name: str
    item_type: str
    workspace: str
    
    # Direct relationships
    depends_on: List[Dict[str, Any]] = field(default_factory=list)  # Items this depends on
    depended_by: List[Dict[str, Any]] = field(default_factory=list)  # Items that depend on this
    
    # Table relationships
    uses_tables: List[Dict[str, Any]] = field(default_factory=list)  # Tables this item uses
    provides_tables: List[Dict[str, Any]] = field(default_factory=list)  # Tables this item provides
    
    # External sources
    consumes_sources: List[Dict[str, Any]] = field(default_factory=list)  # External sources consumed
    
    # Chain depths
    upstream_depth: int = 0
    downstream_depth: int = 0


class LineageAnalyzer:
    """
    Comprehensive lineage relationship analyzer.
    
    Use this class to explore and answer specific questions about
    your Fabric lineage data through code rather than visualization.
    """
    
    def __init__(self, neo4j_client):
        """
        Initialize with a Neo4j client.
        
        Args:
            neo4j_client: Connected Neo4jClient instance
        """
        self.client = neo4j_client
    
    # =========================================================================
    # SCHEMA-LEVEL ANALYSIS
    # =========================================================================
    
    def get_tables_by_schema(self, schema_name: str = None) -> Dict[str, List[Dict]]:
        """
        Get all tables grouped by schema, with their consumers and providers.
        
        Args:
            schema_name: Optional filter for specific schema
            
        Returns:
            Dict mapping schema names to list of table info
        """
        schema_filter = "WHERE t.schema = $schema_name" if schema_name else ""
        params = {"schema_name": schema_name} if schema_name else {}
        
        results = self.client.run_query(f"""
            MATCH (t:Table)
            {schema_filter}
            OPTIONAL MATCH (t)<-[:USES_TABLE]-(consumer:FabricItem)
            OPTIONAL MATCH (t)<-[:PROVIDES_TABLE]-(provider:FabricItem)
            WITH t, 
                 collect(DISTINCT {{name: consumer.name, type: consumer.type, id: consumer.id}}) as consumers,
                 collect(DISTINCT {{name: provider.name, type: provider.type, id: provider.id}}) as providers
            RETURN t.schema as schema,
                   t.database as database,
                   t.name as table_name,
                   t.full_path as full_path,
                   size([c in consumers WHERE c.name IS NOT NULL]) as consumer_count,
                   size([p in providers WHERE p.name IS NOT NULL]) as provider_count,
                   [c in consumers WHERE c.name IS NOT NULL] as consumers,
                   [p in providers WHERE p.name IS NOT NULL] as providers
            ORDER BY t.schema, t.name
        """, params)
        
        # Group by schema
        by_schema = defaultdict(list)
        for row in results:
            by_schema[row.get("schema", "UNKNOWN")].append(row)
        return dict(by_schema)
    
    def get_schema_dependencies(self, schema_name: str) -> Dict[str, Any]:
        """
        Analyze what a specific schema depends on and what depends on it.
        
        Args:
            schema_name: Schema to analyze (e.g., 'DRH', 'CUSTOMER_DATA')
            
        Returns:
            Dict with upstream_schemas, downstream_schemas, external_sources
        """
        # Get tables in this schema and their relationships
        results = self.client.run_query("""
            MATCH (t:Table {schema: $schema})
            
            // Upstream: What provides tables in this schema?
            OPTIONAL MATCH (t)<-[:PROVIDES_TABLE]-(provider:FabricItem)
            OPTIONAL MATCH (provider)-[:CONSUMES]->(ext:ExternalSource)
            
            // Downstream: What consumes tables from this schema?
            OPTIONAL MATCH (t)<-[:USES_TABLE]-(consumer:FabricItem)
            OPTIONAL MATCH (consumer_ws:Workspace)-[:CONTAINS]->(consumer)
            
            WITH t, provider, ext, consumer, consumer_ws
            RETURN 
                collect(DISTINCT t.name) as schema_tables,
                collect(DISTINCT {name: provider.name, type: provider.type}) as providers,
                collect(DISTINCT {name: ext.display_name, type: ext.type}) as external_sources,
                collect(DISTINCT {name: consumer.name, type: consumer.type, workspace: consumer_ws.name}) as consumers
        """, {"schema": schema_name})
        
        if not results:
            return {"schema": schema_name, "tables": [], "providers": [], "consumers": [], "external_sources": []}
        
        row = results[0]
        return {
            "schema": schema_name,
            "tables": [t for t in row.get("schema_tables", []) if t],
            "providers": [p for p in row.get("providers", []) if p.get("name")],
            "consumers": [c for c in row.get("consumers", []) if c.get("name")],
            "external_sources": [e for e in row.get("external_sources", []) if e.get("name")]
        }
    
    # =========================================================================
    # ITEM-LEVEL ANALYSIS
    # =========================================================================
    
    def analyze_item(self, item_name: str) -> DependencyInfo:
        """
        Get comprehensive dependency information for a specific item.
        
        Args:
            item_name: Name of the Fabric item (Lakehouse, Warehouse, etc.)
            
        Returns:
            DependencyInfo with all relationship details
        """
        # Find the item and all its relationships
        result = self.client.run_query("""
            MATCH (item:FabricItem)
            WHERE toLower(item.name) CONTAINS toLower($name)
            OPTIONAL MATCH (ws:Workspace)-[:CONTAINS]->(item)
            
            // What this item depends on
            OPTIONAL MATCH (item)-[:DEPENDS_ON]->(dep:FabricItem)
            OPTIONAL MATCH (dep_ws:Workspace)-[:CONTAINS]->(dep)
            
            // What depends on this item
            OPTIONAL MATCH (item)<-[:DEPENDS_ON]-(dependent:FabricItem)
            OPTIONAL MATCH (dependent_ws:Workspace)-[:CONTAINS]->(dependent)
            
            // Tables used and provided
            OPTIONAL MATCH (item)-[:USES_TABLE]->(used:Table)
            OPTIONAL MATCH (item)-[:PROVIDES_TABLE]->(provided:Table)
            
            // External sources consumed
            OPTIONAL MATCH (item)-[:CONSUMES]->(ext:ExternalSource)
            
            WITH item, ws,
                 collect(DISTINCT {id: dep.id, name: dep.name, type: dep.type, workspace: dep_ws.name}) as depends_on,
                 collect(DISTINCT {id: dependent.id, name: dependent.name, type: dependent.type, workspace: dependent_ws.name}) as depended_by,
                 collect(DISTINCT {name: used.name, schema: used.schema, database: used.database}) as uses_tables,
                 collect(DISTINCT {name: provided.name, schema: provided.schema, database: provided.database}) as provides_tables,
                 collect(DISTINCT {name: ext.display_name, type: ext.type, id: ext.id}) as consumes_sources
            RETURN 
                item.id as item_id,
                item.name as item_name,
                item.type as item_type,
                ws.name as workspace,
                [d in depends_on WHERE d.name IS NOT NULL] as depends_on,
                [d in depended_by WHERE d.name IS NOT NULL] as depended_by,
                [t in uses_tables WHERE t.name IS NOT NULL] as uses_tables,
                [t in provides_tables WHERE t.name IS NOT NULL] as provides_tables,
                [s in consumes_sources WHERE s.name IS NOT NULL] as consumes_sources
            LIMIT 1
        """, {"name": item_name})
        
        if not result:
            raise ValueError(f"Item '{item_name}' not found")
        
        row = result[0]
        return DependencyInfo(
            item_id=row.get("item_id", ""),
            item_name=row.get("item_name", ""),
            item_type=row.get("item_type", ""),
            workspace=row.get("workspace", ""),
            depends_on=row.get("depends_on", []),
            depended_by=row.get("depended_by", []),
            uses_tables=row.get("uses_tables", []),
            provides_tables=row.get("provides_tables", []),
            consumes_sources=row.get("consumes_sources", [])
        )
    
    def get_item_full_chain(self, item_name: str, max_depth: int = 10) -> Dict[str, Any]:
        """
        Get the complete upstream and downstream chain for an item.
        
        Args:
            item_name: Name of the item to analyze
            max_depth: Maximum traversal depth
            
        Returns:
            Dict with full upstream and downstream chains
        """
        # Get upstream chain (what this item ultimately depends on)
        upstream = self.client.run_query(f"""
            MATCH (start:FabricItem)
            WHERE toLower(start.name) CONTAINS toLower($name)
            OPTIONAL MATCH path = (start)-[:DEPENDS_ON|CONSUMES*1..{max_depth}]->(upstream)
            WHERE upstream:FabricItem OR upstream:ExternalSource
            WITH DISTINCT upstream, length(path) as depth, labels(upstream)[0] as node_type
            WHERE upstream IS NOT NULL
            OPTIONAL MATCH (ws:Workspace)-[:CONTAINS]->(upstream)
            RETURN upstream.name as name,
                   upstream.type as type,
                   node_type,
                   ws.name as workspace,
                   depth
            ORDER BY depth
        """, {"name": item_name})
        
        # Get downstream chain (what depends on this item)
        downstream = self.client.run_query(f"""
            MATCH (start:FabricItem)
            WHERE toLower(start.name) CONTAINS toLower($name)
            OPTIONAL MATCH path = (start)<-[:DEPENDS_ON*1..{max_depth}]-(downstream:FabricItem)
            WITH DISTINCT downstream, length(path) as depth
            WHERE downstream IS NOT NULL
            OPTIONAL MATCH (ws:Workspace)-[:CONTAINS]->(downstream)
            RETURN downstream.name as name,
                   downstream.type as type,
                   'FabricItem' as node_type,
                   ws.name as workspace,
                   depth
            ORDER BY depth
        """, {"name": item_name})
        
        return {
            "item": item_name,
            "upstream": upstream,
            "upstream_count": len(upstream),
            "max_upstream_depth": max(u.get("depth", 0) for u in upstream) if upstream else 0,
            "downstream": downstream,
            "downstream_count": len(downstream),
            "max_downstream_depth": max(d.get("depth", 0) for d in downstream) if downstream else 0
        }
    
    # =========================================================================
    # TYPE-BASED ANALYSIS
    # =========================================================================
    
    def get_items_by_type(self, item_type: str) -> List[Dict[str, Any]]:
        """
        Get all items of a specific type with their relationships.
        
        Args:
            item_type: Type to filter (Lakehouse, Warehouse, MirroredDatabase, etc.)
            
        Returns:
            List of items with relationship counts
        """
        return self.client.run_query("""
            MATCH (item:FabricItem {type: $type})
            OPTIONAL MATCH (ws:Workspace)-[:CONTAINS]->(item)
            OPTIONAL MATCH (item)-[:DEPENDS_ON]->(dep)
            OPTIONAL MATCH (item)<-[:DEPENDS_ON]-(dependent)
            OPTIONAL MATCH (item)-[:USES_TABLE]->(uses_t)
            OPTIONAL MATCH (item)-[:PROVIDES_TABLE]->(provides_t)
            OPTIONAL MATCH (item)-[:CONSUMES]->(ext)
            WITH item, ws,
                 count(DISTINCT dep) as depends_on_count,
                 count(DISTINCT dependent) as depended_by_count,
                 count(DISTINCT uses_t) as uses_table_count,
                 count(DISTINCT provides_t) as provides_table_count,
                 count(DISTINCT ext) as external_source_count
            RETURN item.id as id,
                   item.name as name,
                   item.type as type,
                   ws.name as workspace,
                   depends_on_count,
                   depended_by_count,
                   uses_table_count,
                   provides_table_count,
                   external_source_count,
                   depends_on_count + depended_by_count + external_source_count as total_connections
            ORDER BY total_connections DESC
        """, {"type": item_type})
    
    def get_lakehouses_analysis(self) -> List[Dict[str, Any]]:
        """Get detailed analysis of all Lakehouses."""
        return self.get_items_by_type("Lakehouse")
    
    def get_warehouses_analysis(self) -> List[Dict[str, Any]]:
        """Get detailed analysis of all Warehouses."""
        return self.get_items_by_type("Warehouse")
    
    def get_mirrored_databases_analysis(self) -> List[Dict[str, Any]]:
        """Get detailed analysis of all MirroredDatabases."""
        return self.get_items_by_type("MirroredDatabase")
    
    # =========================================================================
    # CROSS-ANALYSIS
    # =========================================================================
    
    def find_path_between_items(self, source_name: str, target_name: str) -> List[Dict]:
        """
        Find all paths between two items.
        
        Args:
            source_name: Starting item name
            target_name: Ending item name
            
        Returns:
            List of paths with nodes and relationships
        """
        return self.client.run_query("""
            MATCH (source:FabricItem), (target:FabricItem)
            WHERE toLower(source.name) CONTAINS toLower($source)
              AND toLower(target.name) CONTAINS toLower($target)
            MATCH path = shortestPath((source)-[*..10]-(target))
            RETURN [n in nodes(path) | {name: n.name, type: coalesce(n.type, labels(n)[0])}] as path_nodes,
                   [r in relationships(path) | type(r)] as relationship_types,
                   length(path) as path_length
            ORDER BY path_length
            LIMIT 5
        """, {"source": source_name, "target": target_name})
    
    def get_cross_workspace_dependencies(self) -> List[Dict]:
        """Get all dependencies that cross workspace boundaries."""
        return self.client.run_query("""
            MATCH (source_ws:Workspace)-[:CONTAINS]->(source:FabricItem)-[:DEPENDS_ON]->(target:FabricItem)<-[:CONTAINS]-(target_ws:Workspace)
            WHERE source_ws <> target_ws
            RETURN source_ws.name as source_workspace,
                   source.name as source_item,
                   source.type as source_type,
                   target_ws.name as target_workspace,
                   target.name as target_item,
                   target.type as target_type
            ORDER BY source_ws.name, target_ws.name
        """)
    
    def find_common_dependencies(self, item_names: List[str]) -> Dict[str, Any]:
        """
        Find common dependencies shared by multiple items.
        
        Args:
            item_names: List of item names to compare
            
        Returns:
            Dict with shared dependencies and unique dependencies per item
        """
        all_deps = {}
        for name in item_names:
            chain = self.get_item_full_chain(name)
            all_deps[name] = set(u.get("name") for u in chain.get("upstream", []) if u.get("name"))
        
        # Find intersection and differences
        if len(all_deps) >= 2:
            common = set.intersection(*all_deps.values())
            unique_per_item = {name: deps - common for name, deps in all_deps.items()}
        else:
            common = set()
            unique_per_item = all_deps
        
        return {
            "items_analyzed": item_names,
            "common_dependencies": list(common),
            "common_count": len(common),
            "unique_per_item": {k: list(v) for k, v in unique_per_item.items()}
        }
    
    # =========================================================================
    # SUMMARY REPORTS
    # =========================================================================
    
    def get_overview(self) -> Dict[str, Any]:
        """Get a high-level overview of the entire lineage graph."""
        stats = self.client.run_query_single("""
            MATCH (n)
            WITH labels(n)[0] as node_type, count(n) as count
            RETURN collect({type: node_type, count: count}) as node_counts
        """)
        
        relationships = self.client.run_query("""
            MATCH ()-[r]->()
            RETURN type(r) as rel_type, count(r) as count
            ORDER BY count DESC
        """)
        
        workspaces = self.client.run_query("""
            MATCH (w:Workspace)
            OPTIONAL MATCH (w)-[:CONTAINS]->(item)
            WITH w, count(item) as item_count
            RETURN w.name as workspace, item_count
            ORDER BY item_count DESC
        """)
        
        return {
            "node_counts": stats.get("node_counts", []) if stats else [],
            "relationship_counts": relationships,
            "workspaces": workspaces
        }
    
    def generate_comprehensive_report(self) -> str:
        """
        Generate a comprehensive markdown report of all lineage relationships.
        
        Returns:
            Markdown formatted report string
        """
        lines = ["# Comprehensive Lineage Analysis Report", ""]
        
        # Overview
        overview = self.get_overview()
        lines.extend(["## Graph Overview", ""])
        lines.extend(["### Node Types", "| Type | Count |", "|------|-------|"])
        for node in overview.get("node_counts", []):
            lines.append(f"| {node.get('type', 'N/A')} | {node.get('count', 0)} |")
        
        lines.extend(["", "### Relationship Types", "| Type | Count |", "|------|-------|"])
        for rel in overview.get("relationship_counts", []):
            lines.append(f"| {rel.get('rel_type', 'N/A')} | {rel.get('count', 0)} |")
        
        # Lakehouses
        lines.extend(["", "---", "", "## Lakehouses Analysis", ""])
        lakehouses = self.get_lakehouses_analysis()
        if lakehouses:
            lines.extend(["| Name | Workspace | Uses Tables | Provides Tables | External Sources |",
                          "|------|-----------|-------------|-----------------|------------------|"])
            for lh in lakehouses[:15]:
                lines.append(f"| {lh.get('name', 'N/A')} | {lh.get('workspace', '-')} | "
                             f"{lh.get('uses_table_count', 0)} | {lh.get('provides_table_count', 0)} | "
                             f"{lh.get('external_source_count', 0)} |")
        
        # Warehouses
        lines.extend(["", "---", "", "## Warehouses Analysis", ""])
        warehouses = self.get_warehouses_analysis()
        if warehouses:
            lines.extend(["| Name | Workspace | Dependencies | Dependents |",
                          "|------|-----------|--------------|------------|"])
            for wh in warehouses[:15]:
                lines.append(f"| {wh.get('name', 'N/A')} | {wh.get('workspace', '-')} | "
                             f"{wh.get('depends_on_count', 0)} | {wh.get('depended_by_count', 0)} |")
        
        # MirroredDatabases
        lines.extend(["", "---", "", "## MirroredDatabases Analysis", ""])
        mirroreds = self.get_mirrored_databases_analysis()
        if mirroreds:
            lines.extend(["| Name | Workspace | Provides Tables | External Sources |",
                          "|------|-----------|-----------------|------------------|"])
            for md in mirroreds[:15]:
                lines.append(f"| {md.get('name', 'N/A')} | {md.get('workspace', '-')} | "
                             f"{md.get('provides_table_count', 0)} | {md.get('external_source_count', 0)} |")
        
        # Cross-workspace dependencies
        lines.extend(["", "---", "", "## Cross-Workspace Dependencies", ""])
        cross = self.get_cross_workspace_dependencies()
        if cross:
            lines.extend(["| Source Workspace | Source Item | → | Target Workspace | Target Item |",
                          "|------------------|-------------|---|------------------|-------------|"])
            for dep in cross[:20]:
                lines.append(f"| {dep.get('source_workspace', '-')} | {dep.get('source_item', '-')} | → | "
                             f"{dep.get('target_workspace', '-')} | {dep.get('target_item', '-')} |")
        
        return "\n".join(lines)


# =============================================================================
# Convenience functions for quick analysis
# =============================================================================

def analyze_with_client(neo4j_client):
    """Create an analyzer instance from a Neo4j client."""
    return LineageAnalyzer(neo4j_client)
