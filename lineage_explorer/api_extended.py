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
