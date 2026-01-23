"""
Fabric Lineage Explorer
-----------------------
A minimalist, intelligent visualization tool for Microsoft Fabric lineage data.

Key Design Principles:
1. Graph-native data model (JSON preferred, CSV supported for legacy)
2. Clean, minimalist UI with intuitive interactions
3. Smart filtering by workspace, item type, source type
4. Responsive design for business and technical users
5. Optional Neo4j graph database for advanced querying

Usage:
    # Option 1: CLI (basic visualization)
    python -m lineage_explorer
    
    # Option 2: Import (programmatic)
    from lineage_explorer import run_server
    run_server(csv_path="path/to/lineage.json", port=8000)
    
    # Option 3: With Neo4j (advanced querying)
    cd lineage_explorer
    docker-compose up -d  # Start Neo4j
    python -m lineage_explorer --neo4j
    
    # Option 4: Statistics only
    from lineage_explorer.statistics import compute_stats_from_file
    stats = compute_stats_from_file("exports/lineage/lineage_20260122.json")
    print(stats.to_json())
"""

__version__ = "2.0.0"

from .server import run_server
from .graph_builder import (
    build_graph_from_csv, build_graph_from_json, build_graph,
    compute_graph_stats, export_graph_to_json
)
from .models import LineageGraph, Workspace, FabricItem, ExternalSource, LineageEdge
from .statistics import LineageStatistics, LineageStatsCalculator, compute_stats_from_file

__all__ = [
    # Server
    "run_server",
    # Graph building
    "build_graph_from_csv",
    "build_graph_from_json",
    "build_graph",
    "compute_graph_stats",
    "export_graph_to_json",
    # Models
    "LineageGraph",
    "Workspace",
    "FabricItem", 
    "ExternalSource",
    "LineageEdge",
    # Statistics
    "LineageStatistics",
    "LineageStatsCalculator",
    "compute_stats_from_file",
]
