"""
Graph Builder - Simplified
--------------------------
JSON/CSV → Graph conversion (~250 LOC).

Design: Single-pass where possible, minimal string manipulation.
Supports both JSON (native) and CSV (legacy) input formats.
"""

import pandas as pd
import hashlib
import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any, Set, Union
from collections import defaultdict

from .models import (
    LineageGraph, Workspace, FabricItem, ExternalSource, 
    LineageEdge, GraphStats
)

logger = logging.getLogger(__name__)


def _parse_json(value: Any) -> Optional[Dict]:
    """Parse JSON/dict value with fallback."""
    if pd.isna(value) or value in ('', 'Unknown'):
        return None
    if isinstance(value, dict):
        return value
    try:
        return json.loads(str(value).replace("'", '"').replace("None", "null"))
    except (json.JSONDecodeError, ValueError):
        return {"raw": str(value)}


def _source_id(source_type: str, conn: Dict) -> str:
    """Generate unique source ID via signature hash."""
    extractors = {
        "OneLake": lambda c: [c.get("oneLake", {}).get("workspaceId", ""), 
                             c.get("oneLake", {}).get("itemId", "")],
        "AdlsGen2": lambda c: [c.get("azureBlob", c.get("adlsGen2", {})).get("container", ""),
                              c.get("azureBlob", c.get("adlsGen2", {})).get("account", "")],
        "AmazonS3": lambda c: [c.get("amazonS3", {}).get("bucket", "")],
        "Snowflake": lambda c: [c.get("snowflake", {}).get("connection", ""),
                               c.get("snowflake", {}).get("database", "")],
    }
    parts = [source_type] + list(filter(None, extractors.get(source_type, lambda c: [json.dumps(c, sort_keys=True)])(conn)))
    return hashlib.md5("|".join(parts).encode()).hexdigest()[:12]


def _display_name(source_type: str, conn: Dict) -> str:
    """Create human-readable source name."""
    names = {
        "OneLake": lambda c: f"OneLake: {c.get('oneLake', {}).get('path', '').split('/')[-1] or 'Item'}",
        "AdlsGen2": lambda c: f"ADLS: {c.get('azureBlob', c.get('adlsGen2', {})).get('container', 'Storage')}",
        "AzureBlob": lambda c: f"ADLS: {c.get('azureBlob', {}).get('container', 'Storage')}",
        "AmazonS3": lambda c: f"S3: {c.get('amazonS3', {}).get('bucket', 'Bucket')}",
        "Snowflake": lambda c: f"Snowflake: {c.get('snowflake', {}).get('database', 'DB')}",
        "OneDriveSharePoint": lambda c: "SharePoint",
    }
    return names.get(source_type, lambda c: source_type)(conn)


def build_graph_from_csv(csv_path: Path) -> LineageGraph:
    """Build lineage graph from CSV in two passes."""
    logger.info(f"Loading CSV: {csv_path}")
    df = pd.read_csv(csv_path)
    logger.info(f"Processing {len(df)} rows")
    
    workspaces: Dict[str, Workspace] = {}
    items: Dict[str, FabricItem] = {}
    sources: Dict[str, ExternalSource] = {}
    edges: List[LineageEdge] = []
    edge_ids: Set[str] = set()
    known_items: Set[str] = set()
    
    # Pass 1: Collect workspaces and items
    for _, row in df.iterrows():
        ws_id, item_id = str(row.get('Workspace ID', '')), str(row.get('Item ID', ''))
        
        if ws_id and ws_id != 'nan' and ws_id not in workspaces:
            workspaces[ws_id] = Workspace(id=ws_id, name=str(row.get('Workspace Name', 'Unknown')))
        
        if item_id and item_id != 'nan':
            known_items.add(item_id)
            if item_id not in items:
                items[item_id] = FabricItem(
                    id=item_id, name=str(row.get('Item Name', 'Unknown')),
                    item_type=str(row.get('Item Type', 'Unknown')), workspace_id=ws_id
                )
    
    # Pass 2: Build edges
    for _, row in df.iterrows():
        target_id = str(row.get('Item ID', ''))
        if not target_id or target_id == 'nan':
            continue
        
        conn = _parse_json(row.get('Source Connection'))
        if not conn:
            continue
        
        source_type = conn.get('type', str(row.get('Source Type', 'Unknown')))
        
        # OneLake internal reference?
        if source_type == 'OneLake':
            upstream_id = conn.get('oneLake', {}).get('itemId')
            if upstream_id and upstream_id in known_items:
                edge_id = f"{upstream_id}->{target_id}"
                if edge_id not in edge_ids:
                    edge_ids.add(edge_id)
                    edges.append(LineageEdge(id=edge_id, source_id=upstream_id, target_id=target_id,
                                            edge_type="internal", metadata={"is_internal": True}))
                continue
        
        # External source
        src_id = f"src_{_source_id(source_type, conn)}"
        if src_id not in sources:
            sources[src_id] = ExternalSource(
                id=src_id, source_type=source_type,
                display_name=_display_name(source_type, conn), connection_details=conn
            )
        
        edge_id = f"{src_id}->{target_id}"
        if edge_id not in edge_ids:
            edge_ids.add(edge_id)
            shortcut = str(row.get('Shortcut Name', ''))
            edges.append(LineageEdge(
                id=edge_id, source_id=src_id, target_id=target_id, edge_type="external",
                metadata={"shortcut": shortcut if shortcut and shortcut != 'nan' else None}
            ))
    
    graph = LineageGraph(
        workspaces=list(workspaces.values()), items=list(items.values()),
        external_sources=list(sources.values()), edges=edges,
        generated_at=datetime.now(), source_file=str(csv_path)
    )
    logger.info(f"Built: {len(workspaces)} workspaces, {len(items)} items, {len(sources)} sources, {len(edges)} edges")
    return graph


def build_graph_from_json(json_path: Path) -> LineageGraph:
    """Build lineage graph from JSON file (native format from extract_lineage.py)."""
    logger.info(f"Loading JSON: {json_path}")
    
    with open(json_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    lineage_items = data.get('lineage', [])
    logger.info(f"Processing {len(lineage_items)} items")
    
    workspaces: Dict[str, Workspace] = {}
    items: Dict[str, FabricItem] = {}
    sources: Dict[str, ExternalSource] = {}
    edges: List[LineageEdge] = []
    edge_ids: Set[str] = set()
    known_items: Set[str] = set()
    
    # Pass 1: Collect workspaces and items
    for row in lineage_items:
        ws_id = str(row.get('Workspace ID', ''))
        item_id = str(row.get('Item ID', ''))
        
        if ws_id and ws_id not in ('', 'nan', 'None'):
            if ws_id not in workspaces:
                workspaces[ws_id] = Workspace(id=ws_id, name=str(row.get('Workspace Name', 'Unknown')))
        
        if item_id and item_id not in ('', 'nan', 'None'):
            known_items.add(item_id)
            if item_id not in items:
                items[item_id] = FabricItem(
                    id=item_id, name=str(row.get('Item Name', 'Unknown')),
                    item_type=str(row.get('Item Type', 'Unknown')), workspace_id=ws_id
                )
    
    # Pass 2: Build edges
    for row in lineage_items:
        target_id = str(row.get('Item ID', ''))
        if not target_id or target_id in ('', 'nan', 'None'):
            continue
        
        # Source Connection is already a dict in JSON (no parsing needed)
        conn = row.get('Source Connection')
        if isinstance(conn, str):
            conn = _parse_json(conn)
        if not conn:
            continue
        
        # Handle list-type connections (multi-source items like SemanticModels)
        if isinstance(conn, list):
            conn = conn[0] if conn else {}
        if not isinstance(conn, dict):
            continue
        
        source_type = conn.get('type', str(row.get('Source Type', 'Unknown')))
        
        # OneLake internal reference?
        if source_type == 'OneLake':
            upstream_id = conn.get('oneLake', {}).get('itemId')
            if upstream_id and upstream_id in known_items:
                edge_id = f"{upstream_id}->{target_id}"
                if edge_id not in edge_ids:
                    edge_ids.add(edge_id)
                    edges.append(LineageEdge(id=edge_id, source_id=upstream_id, target_id=target_id,
                                            edge_type="internal", metadata={"is_internal": True}))
                continue
        
        # External source
        src_id = f"src_{_source_id(source_type, conn)}"
        if src_id not in sources:
            sources[src_id] = ExternalSource(
                id=src_id, source_type=source_type,
                display_name=_display_name(source_type, conn), connection_details=conn
            )
        
        edge_id = f"{src_id}->{target_id}"
        if edge_id not in edge_ids:
            edge_ids.add(edge_id)
            shortcut = str(row.get('Shortcut Name', ''))
            edges.append(LineageEdge(
                id=edge_id, source_id=src_id, target_id=target_id, edge_type="external",
                metadata={"shortcut": shortcut if shortcut and shortcut not in ('', 'nan', 'None') else None}
            ))
    
    graph = LineageGraph(
        workspaces=list(workspaces.values()), items=list(items.values()),
        external_sources=list(sources.values()), edges=edges,
        generated_at=datetime.now(), source_file=str(json_path)
    )
    logger.info(f"Built: {len(workspaces)} workspaces, {len(items)} items, {len(sources)} sources, {len(edges)} edges")
    return graph


def build_graph(file_path: Union[str, Path]) -> LineageGraph:
    """Build lineage graph from JSON or CSV file (auto-detects format)."""
    path = Path(file_path)
    if path.suffix.lower() == '.json':
        return build_graph_from_json(path)
    else:
        return build_graph_from_csv(path)


def compute_graph_stats(graph: LineageGraph) -> GraphStats:
    """Compute graph statistics."""
    items_by_type = defaultdict(int)
    for item in graph.items:
        items_by_type[item.item_type] += 1
    
    sources_by_type = defaultdict(int)
    for src in graph.external_sources:
        sources_by_type[src.source_type] += 1
    
    edges_by_type = defaultdict(int)
    for edge in graph.edges:
        edges_by_type[edge.edge_type] += 1
    
    # Top connected items
    item_conn = defaultdict(int)
    for e in graph.edges:
        item_conn[e.target_id] += 1
    items_by_id = {i.id: i for i in graph.items}
    top_items = [
        {"id": k, "name": items_by_id[k].name, "type": items_by_id[k].item_type, "connections": v}
        for k, v in sorted(item_conn.items(), key=lambda x: -x[1])[:10] if k in items_by_id
    ]
    
    # Top connected sources
    src_conn = defaultdict(int)
    for e in graph.edges:
        if e.source_id.startswith("src_"):
            src_conn[e.source_id] += 1
    sources_by_id = {s.id: s for s in graph.external_sources}
    top_sources = [
        {"id": k, "name": sources_by_id[k].display_name, "type": sources_by_id[k].source_type, "connections": v}
        for k, v in sorted(src_conn.items(), key=lambda x: -x[1])[:10] if k in sources_by_id
    ]
    
    return GraphStats(
        workspace_count=len(graph.workspaces), item_count=len(graph.items),
        external_source_count=len(graph.external_sources), edge_count=len(graph.edges),
        items_by_type=dict(items_by_type), sources_by_type=dict(sources_by_type),
        edges_by_type=dict(edges_by_type), top_connected_items=top_items, top_connected_sources=top_sources
    )


def export_graph_to_json(graph: LineageGraph, output_path: Path) -> None:
    """Export graph to JSON."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, 'w') as f:
        json.dump(graph.model_dump(mode='json'), f, indent=2, default=str)
    logger.info(f"Exported to {output_path}")
