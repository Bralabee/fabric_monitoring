"""
Lineage Explorer API Server
---------------------------
FastAPI backend serving the lineage graph data and static frontend.

Core Endpoints:
- GET /api/graph - Full lineage graph
- GET /api/stats - Graph statistics
- GET /api/workspaces - List of workspaces
- GET /api/health - Health check
- POST /api/refresh - Refresh graph from CSV

Extended Endpoints (see api_extended.py):
- GET /api/stats/detailed - Comprehensive statistics
- GET /api/stats/workspaces - Per-workspace metrics
- GET /api/neo4j/* - Neo4j graph database queries
"""

from fastapi import FastAPI, HTTPException, Request
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
import pandas as pd
import glob
import os
import time
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from contextlib import asynccontextmanager

# Load .env file for environment variables (including NEO4J_PASSWORD)
try:
    from dotenv import load_dotenv
    # Try to load from project root first, then from lineage_explorer directory
    env_paths = [
        Path(__file__).resolve().parent.parent / ".env",  # PROJECT_ROOT/.env
        Path(__file__).resolve().parent / ".env",  # lineage_explorer/.env
    ]
    for env_path in env_paths:
        if env_path.exists():
            load_dotenv(env_path)
            logging.info(f"Loaded environment from {env_path}")
            break
except ImportError:
    logging.warning("python-dotenv not installed - environment variables must be set manually")

from .graph_builder import (
    build_graph_from_csv, build_graph_from_json, build_graph,
    compute_graph_stats, export_graph_to_json
)
from .models import LineageGraph, GraphStats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Path Configuration
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent  # usf_fabric_monitoring root (lineage_explorer is at root level)
EXPORT_DIR = PROJECT_ROOT / "exports" / "lineage"
FRONTEND_DIR = BASE_DIR / "static"


class GraphCache:
    """In-memory cache for the lineage graph."""
    
    def __init__(self, ttl_seconds: int = 300):
        self.graph: Optional[LineageGraph] = None
        self.stats: Optional[GraphStats] = None
        self.loaded_at: Optional[datetime] = None
        self.source_file: Optional[str] = None
        self.source_mtime: Optional[float] = None
        self.ttl_seconds = ttl_seconds
        self.load_time_ms: float = 0
    
    def is_valid(self) -> bool:
        if self.graph is None or self.source_file is None:
            return False
        
        try:
            current_mtime = os.path.getmtime(self.source_file)
            if current_mtime != self.source_mtime:
                logger.info("Source file modified, cache invalidated")
                return False
        except OSError:
            return False
        
        if self.loaded_at:
            age = (datetime.now() - self.loaded_at).total_seconds()
            if age > self.ttl_seconds:
                logger.info(f"Cache TTL expired ({age:.0f}s > {self.ttl_seconds}s)")
                return False
        
        return True
    
    def set(self, graph: LineageGraph, stats: GraphStats, source_file: str, load_time_ms: float):
        self.graph = graph
        self.stats = stats
        self.source_file = source_file
        self.source_mtime = os.path.getmtime(source_file)
        self.loaded_at = datetime.now()
        self.load_time_ms = load_time_ms
        logger.info(f"Cache updated: {graph.total_items} items, {graph.total_connections} edges (loaded in {load_time_ms:.0f}ms)")
    
    def clear(self):
        self.graph = None
        self.stats = None
        self.loaded_at = None
        logger.info("Cache cleared")


# Global cache
_cache = GraphCache(ttl_seconds=300)


def find_lineage_file() -> Path:
    """Find the most recent lineage JSON or CSV file (prefers JSON)."""
    search_paths = [EXPORT_DIR]
    
    cwd_export = Path("exports/lineage")
    if cwd_export.exists():
        search_paths.append(cwd_export)
    
    for search_path in search_paths:
        if not search_path.exists():
            continue
        
        # Prefer JSON files (native format)
        json_files = glob.glob(str(search_path / "lineage_*.json"))
        if json_files:
            return Path(max(json_files, key=os.path.getmtime))
        
        # Fallback to CSV (legacy format)
        csv_files = glob.glob(str(search_path / "mirrored_lineage_*.csv"))
        if not csv_files:
            csv_files = glob.glob(str(search_path / "*.csv"))
        
        if csv_files:
            return Path(max(csv_files, key=os.path.getsize))
    
    raise FileNotFoundError(f"No lineage JSON/CSV found in: {search_paths}")


def load_graph(force_refresh: bool = False) -> tuple[LineageGraph, GraphStats]:
    """Load graph data, using cache when available."""
    global _cache
    
    if not force_refresh and _cache.is_valid():
        return _cache.graph, _cache.stats
    
    start_time = time.time()
    
    file_path = find_lineage_file()
    logger.info(f"Loading: {file_path} ({os.path.getsize(file_path)} bytes)")
    
    graph = build_graph(file_path)  # Auto-detects JSON vs CSV
    stats = compute_graph_stats(graph)
    
    load_time_ms = (time.time() - start_time) * 1000
    _cache.set(graph, stats, str(file_path), load_time_ms)
    
    return graph, stats


# FastAPI App
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Pre-load cache on startup."""
    logger.info("Starting Fabric Lineage Explorer")
    logger.info(f"Project root: {PROJECT_ROOT}")
    logger.info(f"Export dir: {EXPORT_DIR}")
    
    try:
        load_graph()
        logger.info("Initial cache loaded successfully")
        
        # Initialize extended stats calculator
        from .api_extended import init_stats_calculator, init_neo4j
        if _cache.source_file:
            init_stats_calculator(_cache.source_file)
            logger.info("Stats calculator initialized")
        
        # Try to connect to Neo4j if available
        try:
            if init_neo4j():
                logger.info("Neo4j connection established")
        except Exception as e:
            logger.info(f"Neo4j not available (optional): {e}")
            
    except Exception as e:
        logger.warning(f"Could not pre-load cache: {e}")
    
    yield
    
    # Cleanup Neo4j connection
    try:
        from .api_extended import _neo4j_client
        if _neo4j_client:
            _neo4j_client.close()
            logger.info("Neo4j connection closed")
    except Exception:
        pass
    
    logger.info("Shutting down Fabric Lineage Explorer")


app = FastAPI(
    title="Fabric Lineage Explorer",
    description="Interactive visualization of Microsoft Fabric lineage relationships",
    version="3.0.0",
    lifespan=lifespan
)

# Include extended API routes
try:
    from .api_extended import extended_router
    app.include_router(extended_router)
    logger.info("Extended API endpoints loaded")
except ImportError as e:
    logger.warning(f"Extended API not available: {e}")

# CORS configuration - restrict origins in production
# Set CORS_ORIGINS env var to comma-separated list of allowed origins
_cors_origins = os.getenv("CORS_ORIGINS", "http://localhost:8000,http://127.0.0.1:8000").split(",")
_cors_allow_all = os.getenv("CORS_ALLOW_ALL", "false").lower() == "true"

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"] if _cors_allow_all else _cors_origins,
    allow_credentials=False,  # Credentials only with specific origins
    allow_methods=["GET", "POST"],
    allow_headers=["Content-Type", "X-API-Key"],
)


# Simple in-memory rate limiting for sensitive endpoints
_rate_limit_store: dict = {}
_rate_limit_window = 60  # seconds
_rate_limit_max_requests = int(os.getenv("RATE_LIMIT_MAX", "30"))  # requests per window

def check_rate_limit(client_ip: str, endpoint: str) -> bool:
    """
    Check if client has exceeded rate limit.
    
    Returns True if request should be allowed, False if rate limited.
    """
    import time
    key = f"{client_ip}:{endpoint}"
    now = time.time()
    
    # Clean old entries
    if key in _rate_limit_store:
        requests = [t for t in _rate_limit_store[key] if now - t < _rate_limit_window]
        _rate_limit_store[key] = requests
    else:
        _rate_limit_store[key] = []
    
    # Check limit
    if len(_rate_limit_store[key]) >= _rate_limit_max_requests:
        return False
    
    # Record request
    _rate_limit_store[key].append(now)
    return True


# API Endpoints
@app.get("/api/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "version": "1.0.0",
        "cached": _cache.graph is not None,
        "loaded_at": _cache.loaded_at.isoformat() if _cache.loaded_at else None,
        # Note: source_file path removed for security - use /api/health/debug in development
    }


@app.get("/api/health/debug")
async def health_check_debug():
    """
    Debug health check with full details.
    
    Only use in development - exposes internal paths.
    """
    debug_enabled = os.getenv("DEBUG", "false").lower() == "true"
    if not debug_enabled:
        raise HTTPException(status_code=403, detail="Debug endpoint disabled. Set DEBUG=true to enable.")
    
    return {
        "status": "healthy",
        "version": "1.0.0",
        "cached": _cache.graph is not None,
        "loaded_at": _cache.loaded_at.isoformat() if _cache.loaded_at else None,
        "source_file": _cache.source_file,
        "frontend_dir": str(FRONTEND_DIR),
        "export_dir": str(EXPORT_DIR),
    }


@app.get("/api/graph")
async def get_graph():
    """Get the full lineage graph."""
    try:
        graph, _ = load_graph()
        return graph.model_dump(mode='json')
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error loading graph: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error loading graph data")


@app.get("/api/stats")
async def get_stats():
    """Get graph statistics."""
    try:
        _, stats = load_graph()
        return stats.model_dump()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        logger.error(f"Error computing stats: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error computing statistics")


@app.get("/api/workspaces")
async def list_workspaces():
    """List all workspaces with item counts."""
    try:
        graph, _ = load_graph()
        
        # Count items per workspace
        item_counts = {}
        for item in graph.items:
            ws_id = item.workspace_id
            item_counts[ws_id] = item_counts.get(ws_id, 0) + 1
        
        workspaces = [
            {
                "id": ws.id,
                "name": ws.name,
                "item_count": item_counts.get(ws.id, 0)
            }
            for ws in sorted(graph.workspaces, key=lambda w: w.name)
        ]
        
        return {"workspaces": workspaces}
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.post("/api/refresh")
async def refresh_graph(request: Request):
    """Force refresh the graph from CSV. Rate limited to prevent abuse."""
    # Apply rate limiting to this expensive operation
    client_ip = request.client.host if request.client else "unknown"
    if not check_rate_limit(client_ip, "refresh"):
        raise HTTPException(status_code=429, detail="Rate limit exceeded. Try again later.")
    
    try:
        _cache.clear()
        graph, stats = load_graph(force_refresh=True)
        return {
            "status": "refreshed",
            "items": graph.total_items,
            "edges": graph.total_connections,
            "load_time_ms": _cache.load_time_ms
        }
    except Exception as e:
        logger.error(f"Error refreshing: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Error refreshing graph data")


# Serve static frontend (unified single-page visualization)
@app.get("/")
async def serve_frontend():
    """Serve the unified lineage explorer frontend."""
    index_path = FRONTEND_DIR / "index.html"
    if index_path.exists():
        return FileResponse(index_path)
    raise HTTPException(status_code=404, detail="Frontend not found")


@app.get("/dashboard.html")
async def serve_dashboard():
    """Serve the statistics dashboard."""
    dashboard_path = FRONTEND_DIR / "dashboard.html"
    if dashboard_path.exists():
        return FileResponse(dashboard_path)
    raise HTTPException(status_code=404, detail="Dashboard not found")


@app.get("/query_explorer.html")
async def serve_query_explorer():
    """Serve the Neo4j query explorer."""
    explorer_path = FRONTEND_DIR / "query_explorer.html"
    if explorer_path.exists():
        return FileResponse(explorer_path)
    raise HTTPException(status_code=404, detail="Query explorer not found")


@app.get("/table_impact.html")
async def serve_table_impact():
    """Serve the table impact analysis dashboard."""
    impact_path = FRONTEND_DIR / "table_impact.html"
    if impact_path.exists():
        return FileResponse(impact_path)
    raise HTTPException(status_code=404, detail="Table impact page not found")


# Serve static files from root path (for index-v3.html which uses relative paths)
@app.get("/{filename:path}")
async def serve_root_static(filename: str):
    """Serve static files from root path for relative imports in HTML."""
    # Skip API routes
    if filename.startswith("api/"):
        raise HTTPException(status_code=404, detail="Not found")
    
    # Security: Resolve path and ensure it's within FRONTEND_DIR
    try:
        file_path = (FRONTEND_DIR / filename).resolve()
        frontend_resolved = FRONTEND_DIR.resolve()
        # Prevent path traversal attacks
        if not str(file_path).startswith(str(frontend_resolved)):
            raise HTTPException(status_code=403, detail="Access denied")
        if file_path.exists() and file_path.is_file():
            return FileResponse(file_path)
    except (ValueError, OSError):
        pass  # Invalid path
    raise HTTPException(status_code=404, detail=f"File not found: {filename}")


# Mount static files if directory exists
if FRONTEND_DIR.exists():
    app.mount("/static", StaticFiles(directory=FRONTEND_DIR), name="static")


# Allow overriding CSV path
_override_csv_path: Optional[str] = None


def set_csv_path(path: str):
    """Set the data file path to use (for programmatic use). Accepts JSON or CSV."""
    global _override_csv_path
    _override_csv_path = path


# Monkey patch find_lineage_file to check override
_original_find_lineage_file = find_lineage_file

def _find_lineage_file_with_override() -> Path:
    if _override_csv_path:
        return Path(_override_csv_path)
    return _original_find_lineage_file()

# Replace the function
find_lineage_file = _find_lineage_file_with_override


def run_server(csv_path: Optional[str] = None, host: str = "127.0.0.1", port: int = 8000):
    """
    Run the Lineage Explorer server.
    
    Args:
        csv_path: Optional path to lineage JSON or CSV file. Auto-detects if not provided.
        host: Server host (default: 127.0.0.1)
        port: Server port (default: 8000)
    """
    import uvicorn
    
    if csv_path:
        set_csv_path(csv_path)
    
    uvicorn.run(app, host=host, port=port)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Lineage Explorer API Server")
    parser.add_argument("--host", default="127.0.0.1", help="Server host (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=8000, help="Server port (default: 8000)")
    parser.add_argument("--csv", "--file", dest="csv_path", help="Path to lineage JSON/CSV file")
    
    args = parser.parse_args()
    run_server(csv_path=args.csv_path, host=args.host, port=args.port)
