"""
Lineage Explorer API - Extended Endpoints
-----------------------------------------
Additional API endpoints for enhanced statistics and Neo4j integration.

This module extends the base server with:
- Enhanced statistics endpoints (/api/stats/*)
- Neo4j query endpoints (/api/neo4j/*)
- Workspace-specific lineage views (/api/workspace/*)
"""

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel
from typing import Any, Dict, List, Optional
import logging
import os

from .statistics import LineageStatsCalculator, LineageStatistics
from .graph_builder import build_graph, compute_graph_stats

logger = logging.getLogger(__name__)

# Router for extended API endpoints
extended_router = APIRouter()

# Global reference to stats calculator (set by server on load)
_stats_calculator: Optional[LineageStatsCalculator] = None
_neo4j_client = None
_neo4j_queries = None


def init_stats_calculator(file_path: str):
    """Initialize the stats calculator with a data file."""
    global _stats_calculator
    _stats_calculator = LineageStatsCalculator()
    
    if file_path.endswith('.json'):
        _stats_calculator.load_json(file_path)
    else:
        _stats_calculator.load_csv(file_path)
    
    logger.info(f"Stats calculator initialized with {file_path}")


def init_neo4j(uri: str = None, username: str = None, password: str = None):
    """Initialize Neo4j client if available."""
    global _neo4j_client, _neo4j_queries
    
    try:
        from .graph_database import Neo4jClient, LineageQueries
        
        _neo4j_client = Neo4jClient(uri=uri, username=username, password=password)
        _neo4j_client.connect()
        _neo4j_queries = LineageQueries(_neo4j_client)
        
        logger.info("Neo4j client initialized")
        return True
    except ImportError:
        logger.warning("Neo4j package not installed - Neo4j features disabled")
        return False
    except Exception as e:
        logger.warning(f"Could not connect to Neo4j: {e}")
        return False


# ==================== ENHANCED STATS ENDPOINTS ====================

@extended_router.get("/api/stats/detailed")
async def get_detailed_stats():
    """
    Get comprehensive lineage statistics.
    
    Returns detailed breakdown including:
    - Item and source distributions
    - Workspace metrics
    - Cross-workspace dependencies
    - External source details
    - MirroredDatabase table statistics
    """
    if not _stats_calculator:
        raise HTTPException(status_code=503, detail="Stats calculator not initialized")
    
    try:
        stats = _stats_calculator.calculate()
        return stats.to_dict()
    except Exception as e:
        logger.error(f"Error calculating stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error calculating statistics")


@extended_router.get("/api/stats/workspaces")
async def get_workspace_stats():
    """
    Get statistics per workspace.
    
    Returns:
        List of workspaces with item counts and dependency metrics
    """
    if not _stats_calculator:
        raise HTTPException(status_code=503, detail="Stats calculator not initialized")
    
    try:
        stats = _stats_calculator.calculate()
        return {"workspaces": stats.workspace_stats}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/stats/external-sources")
async def get_external_source_stats():
    """
    Get details of all external data sources.
    
    Returns:
        Breakdown by source type with specific instances
    """
    if not _stats_calculator:
        raise HTTPException(status_code=503, detail="Stats calculator not initialized")
    
    try:
        stats = _stats_calculator.calculate()
        return {
            "by_type": stats.sources_by_type,
            "snowflake_databases": stats.snowflake_databases,
            "adls_containers": stats.adls_containers,
            "s3_buckets": stats.s3_buckets,
            "sharepoint_sites": stats.sharepoint_sites,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/stats/tables")
async def get_table_stats():
    """
    Get MirroredDatabase table statistics.
    
    Returns:
        Table counts by database and schema
    """
    if not _stats_calculator:
        raise HTTPException(status_code=503, detail="Stats calculator not initialized")
    
    try:
        stats = _stats_calculator.calculate()
        return {
            "total_tables": stats.mirrored_tables,
            "mirrored_databases": stats.mirrored_databases,
            "tables_by_database": stats.tables_by_database,
            "tables_by_schema": stats.tables_by_schema,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/stats/cross-workspace")
async def get_cross_workspace_stats():
    """
    Get cross-workspace dependency analysis.
    
    Returns:
        Count and list of workspace pairs with dependencies
    """
    if not _stats_calculator:
        raise HTTPException(status_code=503, detail="Stats calculator not initialized")
    
    try:
        stats = _stats_calculator.calculate()
        return {
            "total_cross_workspace_edges": stats.cross_workspace_edges,
            "workspace_pairs": [
                {"source": pair[0], "target": pair[1]}
                for pair in stats.cross_workspace_pairs
            ]
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/stats/orphans")
async def get_orphan_items():
    """
    Get items with no dependencies (potential data quality issue).
    
    Returns:
        List of orphan items
    """
    if not _stats_calculator:
        raise HTTPException(status_code=503, detail="Stats calculator not initialized")
    
    try:
        stats = _stats_calculator.calculate()
        return {"orphan_items": stats.orphan_items}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== NEO4J QUERY ENDPOINTS ====================

def _require_neo4j():
    """Check if Neo4j is available."""
    if not _neo4j_client or not _neo4j_queries:
        raise HTTPException(
            status_code=503, 
            detail="Neo4j not available. Start Neo4j with docker-compose up -d"
        )


@extended_router.get("/api/neo4j/health")
async def neo4j_health():
    """Check Neo4j connection status."""
    if not _neo4j_client:
        return {"status": "not_configured", "detail": "Neo4j client not initialized"}
    
    try:
        health = _neo4j_client.health_check()
        return health
    except Exception as e:
        return {"status": "error", "detail": str(e)}


@extended_router.get("/api/neo4j/stats")
async def neo4j_stats():
    """Get Neo4j graph statistics."""
    _require_neo4j()
    
    try:
        overview = _neo4j_queries.get_overview_stats()
        items_by_type = _neo4j_queries.get_items_by_type()
        sources_by_type = _neo4j_queries.get_sources_by_type()
        
        return {
            "overview": overview,
            "items_by_type": items_by_type,
            "sources_by_type": sources_by_type
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/workspace/{workspace_name}")
async def get_workspace_lineage(workspace_name: str):
    """
    Get complete lineage view for a workspace.
    
    Args:
        workspace_name: Workspace name (partial match supported)
    """
    _require_neo4j()
    
    try:
        return _neo4j_queries.get_workspace_lineage(workspace_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/upstream/{item_id}")
async def find_upstream(
    item_id: str,
    max_depth: int = Query(default=5, ge=1, le=10)
):
    """
    Find all upstream dependencies of an item.
    
    Args:
        item_id: Target item ID
        max_depth: Maximum traversal depth (1-10)
    """
    _require_neo4j()
    
    try:
        return {"dependencies": _neo4j_queries.find_upstream_dependencies(item_id, max_depth)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/downstream/{item_id}")
async def find_downstream(
    item_id: str,
    max_depth: int = Query(default=5, ge=1, le=10)
):
    """
    Find all items that depend on this item (impact analysis).
    
    Args:
        item_id: Source item ID
        max_depth: Maximum traversal depth (1-10)
    """
    _require_neo4j()
    
    try:
        return {"impacted_items": _neo4j_queries.find_downstream_impact(item_id, max_depth)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/path")
async def find_path(
    source_id: str = Query(..., description="Source item ID"),
    target_id: str = Query(..., description="Target item ID"),
    max_depth: int = Query(default=10, ge=1, le=20)
):
    """
    Find paths between two items.
    
    Args:
        source_id: Starting item ID
        target_id: Ending item ID
        max_depth: Maximum path length
    """
    _require_neo4j()
    
    try:
        return {"paths": _neo4j_queries.find_path_between(source_id, target_id, max_depth)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/search")
async def search_graph(
    q: str = Query(..., min_length=2, description="Search query"),
    limit: int = Query(default=20, ge=1, le=100)
):
    """
    Full-text search across items, sources, and tables.
    
    Args:
        q: Search term
        limit: Maximum results
    """
    _require_neo4j()
    
    try:
        return {"results": _neo4j_queries.search_items(q, limit)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/source-type/{source_type}")
async def items_by_source_type(source_type: str):
    """
    Find all items consuming a specific type of external source.
    
    Args:
        source_type: External source type (e.g., 'Snowflake', 'AdlsGen2')
    """
    _require_neo4j()
    
    try:
        return {"items": _neo4j_queries.find_items_by_external_source_type(source_type)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/table/{table_name}")
async def items_using_table(table_name: str):
    """
    Find items that mirror or depend on a specific table.
    
    Args:
        table_name: Table name (partial match supported)
    """
    _require_neo4j()
    
    try:
        return {"items": _neo4j_queries.find_items_using_table(table_name)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/tables-by-database/{database}")
async def tables_by_database(database: str):
    """
    Get all tables for a specific database.
    
    Args:
        database: Database name (e.g., 'EMEA_DRH_DEV')
        
    Returns:
        List of tables with schema information
    """
    _require_neo4j()
    
    try:
        query = """
        MATCH (t:Table)
        WHERE t.database = $database
        RETURN t.name AS table_name, t.schema AS schema
        ORDER BY t.schema, t.name
        """
        results = _neo4j_client.run_query(query, {"database": database})
        return {
            "database": database,
            "tables": results,
            "count": len(results)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/cross-workspace")
async def cross_workspace_dependencies():
    """Find dependencies that cross workspace boundaries."""
    _require_neo4j()
    
    try:
        return {"dependencies": _neo4j_queries.find_cross_workspace_dependencies()}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/centrality")
async def item_centrality(
    top_n: int = Query(default=20, ge=1, le=100)
):
    """
    Calculate centrality scores for items.
    
    Items with high centrality are critical junction points in the data flow.
    
    Args:
        top_n: Number of top items to return
    """
    _require_neo4j()
    
    try:
        return {"top_central_items": _neo4j_queries.calculate_item_centrality(top_n)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ==================== TABLE-LEVEL LINEAGE ENDPOINTS ====================

@extended_router.get("/api/neo4j/table-lineage")
async def get_table_level_lineage():
    """
    Get comprehensive table-level lineage graph.
    
    Returns all tables (from OneLake shortcuts and MirroredDatabases) with their
    relationships to Fabric items, showing which items provide tables and which 
    consume them.
    
    Returns:
        - tables: List of all tables with providers and consumers
        - table_flows: Edges showing table data flow between items
        - summary: Statistics about table coverage
    """
    _require_neo4j()
    
    try:
        return _neo4j_queries.get_table_lineage()
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/table-deps/{table_name}")
async def get_table_dependencies(table_name: str):
    """
    Get detailed dependencies for a specific table.
    
    Args:
        table_name: Table name (partial match supported)
        
    Returns:
        Dict with table details, providers, and consumers
    """
    _require_neo4j()
    
    try:
        return _neo4j_queries.get_table_dependencies(table_name)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/table-impact/{table_name}")
async def get_table_impact(
    table_name: str,
    max_depth: int = Query(default=5, ge=1, le=10, description="Maximum depth for downstream traversal")
):
    """
    Full impact analysis for a table - Speaker 2's use case.
    
    Search for a table name (e.g., "CUSTOMER", "fact_lead") and see:
    1. All items that directly consume the table (MIRRORS, USES_TABLE)
    2. All downstream dependencies of those items (DEPENDS_ON chains)
    3. Cross-workspace impact summary
    
    This answers the question: "If I change this Snowflake table, 
    what Fabric items will be affected?"
    
    Args:
        table_name: Table name (partial match supported)
        max_depth: Maximum traversal depth for downstream impact (1-10)
        
    Returns:
        Complete impact analysis with direct consumers and downstream chain
    """
    _require_neo4j()
    
    try:
        return _neo4j_queries.get_table_impact_analysis(table_name, max_depth)
    except Exception as e:
        logger.error(f"Table impact analysis failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/items-by-workspace/{workspace_name}")
async def items_by_workspace(workspace_name: str):
    """
    Get all items in a specific workspace with their dependencies.
    
    Args:
        workspace_name: Workspace name (partial match supported)
    """
    _require_neo4j()
    
    try:
        query = """
        MATCH (w:Workspace)-[:CONTAINS]->(i:FabricItem)
        WHERE toLower(w.name) CONTAINS toLower($workspace_name)
        OPTIONAL MATCH (i)-[:DEPENDS_ON]->(dep:FabricItem)
        OPTIONAL MATCH (i)-[:CONSUMES]->(src:ExternalSource)
        WITH w, i, count(DISTINCT dep) AS internal_deps, count(DISTINCT src) AS external_deps
        RETURN w.name AS workspace,
               i.name AS item_name,
               i.type AS item_type,
               i.id AS item_id,
               internal_deps,
               external_deps
        ORDER BY i.type, i.name
        """
        results = _neo4j_client.run_query(query, {"workspace_name": workspace_name})
        return {
            "workspace": workspace_name,
            "items": results,
            "count": len(results)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/external-sources-by-type/{source_type}")
async def external_sources_by_type(source_type: str):
    """
    List all external sources of a specific type with consumer counts.
    
    Args:
        source_type: Source type (Snowflake, AdlsGen2, AmazonS3, OneLake, etc.)
    """
    _require_neo4j()
    
    try:
        query = """
        MATCH (s:ExternalSource)
        WHERE toLower(s.type) CONTAINS toLower($source_type)
        OPTIONAL MATCH (s)<-[:CONSUMES]-(i:FabricItem)
        WITH s, collect(DISTINCT i.name) AS consumers, count(DISTINCT i) AS consumer_count
        RETURN s.display_name AS source_name,
               s.type AS source_type,
               s.id AS source_id,
               consumer_count,
               consumers[0..5] AS sample_consumers
        ORDER BY consumer_count DESC
        """
        results = _neo4j_client.run_query(query, {"source_type": source_type})
        return {
            "source_type": source_type,
            "sources": results,
            "count": len(results)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/lineage-chain/{item_id}")
async def lineage_chain(
    item_id: str,
    max_depth: int = Query(default=5, ge=1, le=10)
):
    """
    Get complete lineage chain (upstream + downstream) for an item.
    
    Args:
        item_id: Item ID
        max_depth: Maximum traversal depth in each direction
    """
    _require_neo4j()
    
    try:
        # Get upstream
        upstream_query = """
        MATCH (start:FabricItem {id: $item_id})
        OPTIONAL MATCH path = (start)-[:DEPENDS_ON|CONSUMES*1..5]->(upstream)
        WHERE upstream:FabricItem OR upstream:ExternalSource
        WITH DISTINCT upstream, length(path) AS depth
        WHERE upstream IS NOT NULL
        RETURN upstream.name AS name, 
               upstream.type AS type,
               labels(upstream)[0] AS node_type,
               depth,
               'upstream' AS direction
        ORDER BY depth
        """
        
        # Get downstream
        downstream_query = """
        MATCH (start:FabricItem {id: $item_id})
        OPTIONAL MATCH path = (start)<-[:DEPENDS_ON*1..5]-(downstream:FabricItem)
        WITH DISTINCT downstream, length(path) AS depth
        WHERE downstream IS NOT NULL
        RETURN downstream.name AS name,
               downstream.type AS type,
               'FabricItem' AS node_type,
               depth,
               'downstream' AS direction
        ORDER BY depth
        """
        
        upstream = _neo4j_client.run_query(upstream_query, {"item_id": item_id})
        downstream = _neo4j_client.run_query(downstream_query, {"item_id": item_id})
        
        return {
            "item_id": item_id,
            "upstream": upstream,
            "downstream": downstream,
            "upstream_count": len(upstream),
            "downstream_count": len(downstream)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/schema-coverage")
async def schema_coverage():
    """
    Get table coverage by schema with consumer counts.
    Shows which schemas have the most tables and consumers.
    """
    _require_neo4j()
    
    try:
        query = """
        MATCH (t:Table)
        OPTIONAL MATCH (t)<-[:MIRRORS]-(m:FabricItem)
        WITH t.schema AS schema, t.database AS database,
             collect(t.name) AS tables,
             count(DISTINCT t) AS table_count,
             count(DISTINCT m) AS consumer_count
        RETURN database, schema, table_count, consumer_count,
               tables[0..10] AS sample_tables
        ORDER BY table_count DESC
        """
        results = _neo4j_client.run_query(query, {})
        return {
            "schemas": results,
            "count": len(results)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/dependency-depth")
async def dependency_depth(
    top_n: int = Query(default=20, ge=1, le=100)
):
    """
    Rank items by their maximum dependency chain depth.
    Items with deeper chains are more complex/fragile.
    
    Args:
        top_n: Number of top items to return
    """
    _require_neo4j()
    
    try:
        query = """
        MATCH (i:FabricItem)
        OPTIONAL MATCH path = (i)-[:DEPENDS_ON*]->(root)
        WHERE NOT EXISTS { MATCH (root)-[:DEPENDS_ON]->() }
        WITH i, max(length(path)) AS max_depth
        WHERE max_depth IS NOT NULL AND max_depth > 0
        OPTIONAL MATCH (i)<-[:CONTAINS]-(w:Workspace)
        RETURN i.name AS item_name,
               i.type AS item_type,
               w.name AS workspace,
               max_depth AS dependency_depth
        ORDER BY max_depth DESC
        LIMIT $top_n
        """
        results = _neo4j_client.run_query(query, {"top_n": top_n})
        return {
            "items": results,
            "count": len(results)
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class QueryRequest(BaseModel):
    """Request model for custom Cypher queries."""
    query: str
    parameters: Optional[Dict[str, Any]] = None


@extended_router.post("/api/neo4j/query")
async def run_custom_query(
    request: Request,
    body: QueryRequest
):
    """
    Run a custom Cypher query (read-only).
    
    Args:
        body: JSON body with query and optional parameters
            - query: Cypher query string (max 2000 chars)
            - parameters: Query parameters (max 20 parameters)
        
    Note: Only read queries allowed (no CREATE, DELETE, SET, etc.)
    
    Security: This endpoint performs strict validation but should be
    disabled or require authentication in production environments.
    Rate limited to prevent abuse.
    """
    _require_neo4j()
    
    query = body.query
    parameters = body.parameters
    
    # Import rate limit check from server
    try:
        from .server import check_rate_limit
        client_ip = request.client.host if request.client else "unknown"
        if not check_rate_limit(client_ip, "neo4j_query"):
            raise HTTPException(status_code=429, detail="Rate limit exceeded. Try again later.")
    except ImportError:
        pass  # Rate limiting not available
    
    # Length limits to prevent DoS
    if len(query) > 2000:
        raise HTTPException(status_code=400, detail="Query too long (max 2000 chars)")
    if parameters and len(parameters) > 20:
        raise HTTPException(status_code=400, detail="Too many parameters (max 20)")
    
    # Normalize query for security checks (case-insensitive, strip comments)
    import re
    # Remove single-line and multi-line comments
    query_cleaned = re.sub(r'//.*?$', '', query, flags=re.MULTILINE)
    query_cleaned = re.sub(r'/\*.*?\*/', '', query_cleaned, flags=re.DOTALL)
    query_upper = query_cleaned.upper()
    
    # Comprehensive forbidden keyword list (case-insensitive after normalization)
    forbidden = [
        'CREATE', 'DELETE', 'SET', 'REMOVE', 'MERGE', 'DROP', 'DETACH',
        'CALL', 'LOAD CSV', 'PERIODIC COMMIT', 'FOREACH', 'USING PERIODIC',
        'CONSTRAINT', 'INDEX', 'GRANT', 'REVOKE', 'DENY'
    ]
    for keyword in forbidden:
        # Use word boundary check to avoid false positives
        if re.search(r'\b' + re.escape(keyword) + r'\b', query_upper):
            raise HTTPException(
                status_code=400, 
                detail=f"Write/admin operations not allowed via API. Found: {keyword}"
            )
    
    try:
        return {"results": _neo4j_client.run_query(query, parameters or {})}
    except Exception as e:
        logger.error(f"Custom query failed: {e}")
        raise HTTPException(status_code=500, detail="Query execution failed")


# ==================== DATA LOADING ====================

@extended_router.post("/api/neo4j/load")
async def load_data_to_neo4j(
    clear_existing: bool = Query(default=False, description="Clear existing data before loading")
):
    """
    Load lineage data into Neo4j.
    
    This loads the currently active lineage file into the Neo4j database.
    
    Args:
        clear_existing: If True, clear database before loading
    """
    _require_neo4j()
    
    if not _stats_calculator or not _stats_calculator._data:
        raise HTTPException(status_code=503, detail="No lineage data loaded")
    
    try:
        from .graph_database import LineageDataLoader
        
        loader = LineageDataLoader(_neo4j_client)
        
        if clear_existing:
            _neo4j_client.clear_database()
            loader.setup_schema()
        
        summary = loader._load_lineage_records(_stats_calculator._data)
        return {"status": "loaded", "summary": summary}
    except Exception as e:
        logger.error(f"Error loading to Neo4j: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==================== CHAIN DEPTH ANALYSIS ENDPOINTS ====================

@extended_router.get("/api/neo4j/chains/deep")
async def get_deep_chains(
    min_depth: int = Query(default=3, ge=2, le=10, description="Minimum chain depth to include")
):
    """
    Find all dependency chains with depth >= min_depth.
    
    Returns the deepest interconnected chains in the lineage graph,
    helping identify complex data pipelines and potential brittleness.
    
    Args:
        min_depth: Minimum chain depth (default: 3)
        
    Returns:
        List of chains sorted by depth (deepest first) with full path information
    """
    _require_neo4j()
    
    try:
        chains = _neo4j_queries.get_deep_chains(min_depth)
        return {
            "min_depth": min_depth,
            "chains": chains,
            "count": len(chains)
        }
    except Exception as e:
        logger.error(f"Deep chains query failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/chains/node-depths")
async def get_node_chain_depths():
    """
    Get chain depth metrics for all Fabric items.
    
    For each item, returns:
    - upstream_depth: Maximum depth of upstream dependencies
    - downstream_depth: Maximum depth of downstream dependents
    - total_chain_depth: Sum for overall chain centrality
    
    Used by the UI "Color by Chain Depth" feature.
    """
    _require_neo4j()
    
    try:
        depths = _neo4j_queries.get_node_chain_depths()
        return {
            "node_depths": depths,
            "count": len(depths)
        }
    except Exception as e:
        logger.error(f"Node depths query failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/neo4j/chains/stats")
async def get_chain_depth_stats():
    """
    Get summary statistics about chain depths in the graph.
    
    Returns:
        - max_depth: Deepest chain in the graph
        - avg_depth: Average chain depth
        - chain_count: Total number of multi-hop chains
        - depth_distribution: Count of chains at each depth level
    """
    _require_neo4j()
    
    try:
        return _neo4j_queries.get_chain_depth_stats()
    except Exception as e:
        logger.error(f"Chain stats query failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==================== TABLE IMPACT ANALYSIS ENDPOINTS ====================

@extended_router.get("/api/tables/search")
async def search_tables(
    q: str = Query(..., min_length=1, description="Table name search query"),
    limit: int = Query(default=50, ge=1, le=200)
):
    """
    Search tables by name with consumer counts.
    
    Args:
        q: Search term (partial match)
        limit: Maximum results
        
    Returns:
        List of matching tables with direct consumer counts
    """
    _require_neo4j()
    
    try:
        query = """
        MATCH (t:Table)
        WHERE toLower(t.name) CONTAINS toLower($search)
        
        // Count direct consumers (MIRRORS + USES_TABLE)
        OPTIONAL MATCH (item:FabricItem)-[:MIRRORS|USES_TABLE]->(t)
        
        WITH t, count(DISTINCT item) as consumer_count
        
        RETURN 
            t.id as table_id,
            t.name as table_name,
            t.schema as schema,
            t.database as database,
            t.full_path as full_path,
            consumer_count
        ORDER BY consumer_count DESC, t.name
        LIMIT $limit
        """
        results = _neo4j_client.run_query(query, {"search": q, "limit": limit})
        return {
            "query": q,
            "tables": results,
            "count": len(results)
        }
    except Exception as e:
        logger.error(f"Table search failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/tables/impact")
async def get_table_impact_by_query(
    id: str = Query(..., description="Table ID or name to analyze"),
    max_depth: int = Query(default=10, ge=1, le=20)
):
    """
    Get table impact via query parameter.
    
    This endpoint handles table IDs containing special characters like slashes.
    Preferred over the path-based endpoint.
    """
    return await _get_table_impact_impl(id, max_depth)


@extended_router.get("/api/tables/{table_id}/impact")
async def get_table_impact(
    table_id: str,
    max_depth: int = Query(default=10, ge=1, le=20)
):
    """
    Get full downstream impact analysis for a table.
    
    Traces from Table → Direct Consumers → All Downstream Items.
    
    Args:
        table_id: Table ID
        max_depth: Maximum traversal depth
        
    Returns:
        Impact tree with all downstream dependencies
    """
    return await _get_table_impact_impl(table_id, max_depth)


async def _get_table_impact_impl(table_id: str, max_depth: int = 10):
    """Shared implementation for table impact analysis."""
    _require_neo4j()
    
    try:
        # First try to get table by ID, then fall back to name search
        table_query = """
        MATCH (t:Table)
        WHERE t.id = $table_id OR toLower(t.name) = toLower($table_id)
        RETURN t.id as id, t.name as name, t.schema as schema, 
               t.database as database, t.full_path as full_path
        LIMIT 1
        """
        table_results = _neo4j_client.run_query(table_query, {"table_id": table_id})
        
        if not table_results:
            raise HTTPException(status_code=404, detail=f"Table not found: {table_id}")
        
        table_info = table_results[0]
        actual_table_id = table_info["id"]  # Use the actual ID for subsequent queries
        
        # Get direct consumers (items that MIRROR or USE this table)
        consumers_query = """
        MATCH (t:Table {id: $table_id})<-[:MIRRORS|USES_TABLE]-(item:FabricItem)
        OPTIONAL MATCH (w:Workspace)-[:CONTAINS]->(item)
        RETURN 
            item.id as item_id,
            item.name as item_name,
            item.type as item_type,
            w.name as workspace,
            'direct' as relationship
        """
        direct_consumers = _neo4j_client.run_query(consumers_query, {"table_id": actual_table_id})
        
        # Get full downstream impact from each direct consumer
        all_downstream = []
        consumer_ids = [c["item_id"] for c in direct_consumers]
        
        if consumer_ids:
            downstream_query = """
            UNWIND $item_ids as start_id
            MATCH path = (start:FabricItem {id: start_id})<-[:DEPENDS_ON*1..10]-(downstream)
            WHERE length(path) <= $max_depth
            OPTIONAL MATCH (w:Workspace)-[:CONTAINS]->(downstream)
            RETURN DISTINCT
                start.id as source_id,
                start.name as source_name,
                downstream.id as item_id,
                downstream.name as item_name,
                downstream.type as item_type,
                w.name as workspace,
                length(path) as depth,
                [n in nodes(path) | n.name] as path_names
            ORDER BY depth
            """
            all_downstream = _neo4j_client.run_query(
                downstream_query, 
                {"item_ids": consumer_ids, "max_depth": max_depth}
            )
        
        return {
            "table": table_info,
            "direct_consumers": direct_consumers,
            "direct_consumer_count": len(direct_consumers),
            "downstream_items": all_downstream,
            "downstream_count": len(all_downstream),
            "total_impact": len(direct_consumers) + len(all_downstream)
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Table impact analysis failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@extended_router.get("/api/tables")
async def list_tables(
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0)
):
    """
    List all tables with consumer statistics.
    
    Returns:
        Paginated list of tables with consumer counts
    """
    _require_neo4j()
    
    try:
        query = """
        MATCH (t:Table)
        OPTIONAL MATCH (item:FabricItem)-[:MIRRORS|USES_TABLE]->(t)
        WITH t, count(DISTINCT item) as consumer_count
        RETURN 
            t.id as table_id,
            t.name as table_name,
            t.schema as schema,
            t.database as database,
            consumer_count
        ORDER BY consumer_count DESC, t.name
        SKIP $offset
        LIMIT $limit
        """
        results = _neo4j_client.run_query(query, {"limit": limit, "offset": offset})
        
        # Get total count
        count_query = "MATCH (t:Table) RETURN count(t) as total"
        total = _neo4j_client.run_query(count_query)[0]["total"]
        
        return {
            "tables": results,
            "count": len(results),
            "total": total,
            "offset": offset,
            "limit": limit
        }
    except Exception as e:
        logger.error(f"List tables failed: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))
