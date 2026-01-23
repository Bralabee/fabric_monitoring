"""
Enhanced Lineage Statistics Module
----------------------------------
Provides comprehensive statistics extraction from lineage data.

Supports both in-memory analysis (from graph_builder) and
Neo4j-backed analysis for larger datasets.
"""

import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
import hashlib

logger = logging.getLogger(__name__)


@dataclass
class LineageStatistics:
    """
    Comprehensive lineage statistics container.
    
    Provides multi-dimensional analysis of lineage data for reporting,
    governance dashboards, and data quality monitoring.
    """
    
    # Basic Counts
    total_workspaces: int = 0
    total_items: int = 0
    total_external_sources: int = 0
    total_edges: int = 0
    total_tables: int = 0
    
    # Type Distributions
    items_by_type: Dict[str, int] = field(default_factory=dict)
    sources_by_type: Dict[str, int] = field(default_factory=dict)
    edges_by_type: Dict[str, int] = field(default_factory=dict)
    
    # Workspace Metrics
    workspace_stats: List[Dict[str, Any]] = field(default_factory=list)
    workspaces_with_external_deps: int = 0
    workspaces_with_internal_deps: int = 0
    
    # Connectivity Metrics
    top_connected_items: List[Dict[str, Any]] = field(default_factory=list)
    top_connected_sources: List[Dict[str, Any]] = field(default_factory=list)
    orphan_items: List[Dict[str, Any]] = field(default_factory=list)
    
    # Cross-Workspace Analysis
    cross_workspace_edges: int = 0
    cross_workspace_pairs: List[Tuple[str, str]] = field(default_factory=list)
    
    # External Source Analysis
    snowflake_databases: List[str] = field(default_factory=list)
    adls_containers: List[str] = field(default_factory=list)
    s3_buckets: List[str] = field(default_factory=list)
    sharepoint_sites: List[str] = field(default_factory=list)
    
    # MirroredDatabase Analysis
    mirrored_databases: int = 0
    mirrored_tables: int = 0
    tables_by_database: Dict[str, int] = field(default_factory=dict)
    tables_by_schema: Dict[str, int] = field(default_factory=dict)
    
    # Metadata
    generated_at: datetime = field(default_factory=datetime.now)
    source_file: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "summary": {
                "total_workspaces": self.total_workspaces,
                "total_items": self.total_items,
                "total_external_sources": self.total_external_sources,
                "total_edges": self.total_edges,
                "total_tables": self.total_tables,
            },
            "distributions": {
                "items_by_type": self.items_by_type,
                "sources_by_type": self.sources_by_type,
                "edges_by_type": self.edges_by_type,
            },
            "workspace_analysis": {
                "workspace_stats": self.workspace_stats,
                "with_external_deps": self.workspaces_with_external_deps,
                "with_internal_deps": self.workspaces_with_internal_deps,
            },
            "connectivity": {
                "top_items": self.top_connected_items,
                "top_sources": self.top_connected_sources,
                "orphan_count": len(self.orphan_items),
                "cross_workspace_edges": self.cross_workspace_edges,
            },
            "external_sources": {
                "snowflake_databases": self.snowflake_databases,
                "adls_containers": self.adls_containers,
                "s3_buckets": self.s3_buckets,
                "sharepoint_sites": self.sharepoint_sites,
            },
            "mirrored_data": {
                "database_count": self.mirrored_databases,
                "table_count": self.mirrored_tables,
                "tables_by_database": self.tables_by_database,
                "tables_by_schema": self.tables_by_schema,
            },
            "metadata": {
                "generated_at": self.generated_at.isoformat(),
                "source_file": self.source_file,
            }
        }
    
    def to_json(self, indent: int = 2) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict(), indent=indent, default=str)


class LineageStatsCalculator:
    """
    Calculate comprehensive statistics from lineage data.
    
    Supports loading from JSON or CSV files.
    Includes caching to avoid recalculation on every request.
    """
    
    def __init__(self):
        self._data: List[Dict] = []
        self._stats: Optional[LineageStatistics] = None
        self._data_hash: Optional[str] = None  # For cache invalidation
    
    def _compute_data_hash(self) -> str:
        """Compute a hash of the data for cache invalidation."""
        content = json.dumps(self._data, sort_keys=True, default=str)
        return hashlib.md5(content.encode()).hexdigest()
    
    def load_json(self, path: Union[str, Path]) -> "LineageStatsCalculator":
        """Load lineage data from JSON file."""
        path = Path(path)
        logger.info(f"Loading lineage from {path}")
        
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        self._data = data.get('lineage', [])
        self._stats = None  # Reset stats
        logger.info(f"Loaded {len(self._data)} records")
        return self
    
    def load_csv(self, path: Union[str, Path]) -> "LineageStatsCalculator":
        """Load lineage data from CSV file."""
        import pandas as pd
        
        path = Path(path)
        logger.info(f"Loading lineage from {path}")
        
        df = pd.read_csv(path)
        self._data = df.to_dict('records')
        self._stats = None
        logger.info(f"Loaded {len(self._data)} records")
        return self
    
    def calculate(self, force_refresh: bool = False) -> LineageStatistics:
        """
        Calculate all statistics from loaded data.
        
        Args:
            force_refresh: If True, bypass cache and recalculate
        
        Returns:
            LineageStatistics object with all metrics
        """
        if not self._data:
            raise ValueError("No data loaded. Call load_json() or load_csv() first.")
        
        # Check cache
        current_hash = self._compute_data_hash()
        if not force_refresh and self._stats and self._data_hash == current_hash:
            logger.debug("Returning cached statistics")
            return self._stats
        
        logger.info("Calculating statistics (cache miss or force refresh)")
        stats = LineageStatistics()
        
        # Collect unique entities
        workspaces = {}
        items = {}
        known_item_ids = set()
        
        items_by_type = defaultdict(int)
        sources_by_type = defaultdict(int)
        
        internal_edges = []
        external_edges = []
        
        workspace_items = defaultdict(set)
        workspace_ext_deps = defaultdict(int)
        workspace_int_deps = defaultdict(int)
        
        external_sources = {}
        tables = {}
        
        # First pass: collect items
        for record in self._data:
            item_id = record.get('Item ID')
            if item_id:
                known_item_ids.add(item_id)
        
        # Second pass: process records
        for record in self._data:
            ws_id = record.get('Workspace ID')
            ws_name = record.get('Workspace Name', 'Unknown')
            item_id = record.get('Item ID')
            item_name = record.get('Item Name', 'Unknown')
            item_type = record.get('Item Type', 'Unknown')
            
            # Track workspace
            if ws_id:
                workspaces[ws_id] = ws_name
            
            # Track item
            if item_id:
                items[item_id] = {
                    'name': item_name,
                    'type': item_type,
                    'workspace_id': ws_id
                }
                workspace_items[ws_id].add(item_id)
                items_by_type[item_type.replace(' Shortcut', '')] += 1
            
            # Parse connection
            conn = self._parse_connection(record.get('Source Connection'))
            source_type = conn.get('type') if conn else record.get('Source Type', 'Unknown')
            
            # Track dependencies
            if source_type == 'OneLake' and conn:
                upstream_id = conn.get('oneLake', {}).get('itemId')
                if upstream_id and upstream_id in known_item_ids:
                    internal_edges.append({
                        'source': upstream_id,
                        'target': item_id,
                        'workspace': ws_id
                    })
                    workspace_int_deps[ws_id] += 1
                else:
                    self._track_external_source(source_type, conn, external_sources)
                    external_edges.append({'item': item_id, 'type': source_type})
                    workspace_ext_deps[ws_id] += 1
            
            elif source_type and source_type not in ('Unknown', ''):
                self._track_external_source(source_type, conn, external_sources)
                sources_by_type[source_type] += 1
                external_edges.append({'item': item_id, 'type': source_type})
                workspace_ext_deps[ws_id] += 1
            
            # Process MirroredDatabase tables
            if item_type == 'MirroredDatabase':
                stats.mirrored_databases += 1
                self._extract_tables(record, tables)
        
        # Calculate derived metrics
        stats.total_workspaces = len(workspaces)
        stats.total_items = len(items)
        stats.total_external_sources = len(external_sources)
        stats.total_edges = len(internal_edges) + len(external_edges)
        stats.total_tables = len(tables)
        
        stats.items_by_type = dict(items_by_type)
        stats.sources_by_type = dict(sources_by_type)
        stats.edges_by_type = {
            'internal': len(internal_edges),
            'external': len(external_edges)
        }
        
        # Workspace stats
        stats.workspace_stats = [
            {
                'workspace_id': ws_id,
                'workspace_name': workspaces.get(ws_id, 'Unknown'),
                'item_count': len(workspace_items.get(ws_id, set())),
                'external_deps': workspace_ext_deps.get(ws_id, 0),
                'internal_deps': workspace_int_deps.get(ws_id, 0)
            }
            for ws_id in workspaces
        ]
        stats.workspace_stats.sort(key=lambda x: -x['item_count'])
        
        stats.workspaces_with_external_deps = sum(1 for ws in workspace_ext_deps if workspace_ext_deps[ws] > 0)
        stats.workspaces_with_internal_deps = sum(1 for ws in workspace_int_deps if workspace_int_deps[ws] > 0)
        
        # Calculate top connected items
        incoming = defaultdict(int)
        for edge in internal_edges:
            incoming[edge['source']] += 1
        
        stats.top_connected_items = [
            {
                'id': item_id,
                'name': items.get(item_id, {}).get('name', 'Unknown'),
                'type': items.get(item_id, {}).get('type', 'Unknown'),
                'connections': count
            }
            for item_id, count in sorted(incoming.items(), key=lambda x: -x[1])[:10]
        ]
        
        # Top external sources
        source_usage = defaultdict(int)
        for edge in external_edges:
            source_usage[edge['type']] += 1
        
        stats.top_connected_sources = [
            {'type': stype, 'connections': count}
            for stype, count in sorted(source_usage.items(), key=lambda x: -x[1])[:10]
        ]
        
        # Orphan items (no dependencies)
        connected_items = set()
        for edge in internal_edges:
            connected_items.add(edge['source'])
            connected_items.add(edge['target'])
        for edge in external_edges:
            connected_items.add(edge['item'])
        
        orphans = set(items.keys()) - connected_items
        stats.orphan_items = [
            {
                'id': item_id,
                'name': items[item_id]['name'],
                'type': items[item_id]['type']
            }
            for item_id in orphans
        ]
        
        # Cross-workspace analysis
        cross_ws = []
        for edge in internal_edges:
            source_ws = items.get(edge['source'], {}).get('workspace_id')
            target_ws = items.get(edge['target'], {}).get('workspace_id')
            if source_ws and target_ws and source_ws != target_ws:
                cross_ws.append((
                    workspaces.get(source_ws, 'Unknown'),
                    workspaces.get(target_ws, 'Unknown')
                ))
        
        stats.cross_workspace_edges = len(cross_ws)
        stats.cross_workspace_pairs = list(set(cross_ws))[:20]
        
        # External source details
        for source in external_sources.values():
            stype = source.get('type')
            if stype == 'Snowflake' and source.get('database'):
                if source['database'] not in stats.snowflake_databases:
                    stats.snowflake_databases.append(source['database'])
            elif stype in ('AdlsGen2', 'AzureBlob') and source.get('container'):
                if source['container'] not in stats.adls_containers:
                    stats.adls_containers.append(source['container'])
            elif stype == 'AmazonS3' and source.get('location'):
                if source['location'] not in stats.s3_buckets:
                    stats.s3_buckets.append(source['location'])
            elif stype == 'OneDriveSharePoint' and source.get('location'):
                if source['location'] not in stats.sharepoint_sites:
                    stats.sharepoint_sites.append(source['location'])
        
        # Table statistics
        stats.mirrored_tables = len(tables)
        tables_by_db = defaultdict(int)
        tables_by_schema = defaultdict(int)
        
        for table in tables.values():
            tables_by_db[table.get('database', 'Unknown')] += 1
            tables_by_schema[table.get('schema', 'Unknown')] += 1
        
        stats.tables_by_database = dict(tables_by_db)
        stats.tables_by_schema = dict(tables_by_schema)
        
        # Store in cache
        self._stats = stats
        self._data_hash = current_hash
        logger.info(f"Statistics calculated and cached (hash: {current_hash[:8]})")
        
        return stats
    
    def _parse_connection(self, conn_value) -> Optional[Dict]:
        """Parse connection value to dict."""
        if not conn_value or conn_value == 'Unknown':
            return None
        
        if isinstance(conn_value, dict):
            return conn_value
        
        if isinstance(conn_value, str):
            try:
                import ast
                return ast.literal_eval(conn_value)
            except:
                try:
                    return json.loads(conn_value.replace("'", '"').replace('None', 'null'))
                except:
                    return None
        
        return None
    
    def _track_external_source(
        self,
        source_type: str,
        conn: Optional[Dict],
        sources: Dict
    ):
        """Track unique external source."""
        if not conn:
            return
        
        # Generate unique ID
        parts = [source_type]
        
        if source_type == 'Snowflake':
            props = conn.get('typeProperties', conn.get('snowflake', {}))
            parts.extend([props.get('database', ''), props.get('connection', '')])
        elif source_type in ('AdlsGen2', 'AzureBlob'):
            blob = conn.get('adlsGen2', conn.get('azureBlob', {}))
            parts.extend([blob.get('location', ''), blob.get('container', blob.get('subpath', ''))])
        elif source_type == 'AmazonS3':
            s3 = conn.get('amazonS3', {})
            parts.extend([s3.get('location', '')])
        elif source_type == 'OneDriveSharePoint':
            sp = conn.get('oneDriveSharePoint', {})
            parts.extend([sp.get('location', '')])
        
        source_id = hashlib.md5('|'.join(filter(None, parts)).encode()).hexdigest()[:12]
        
        if source_id not in sources:
            sources[source_id] = {
                'id': source_id,
                'type': source_type,
                'database': conn.get('typeProperties', {}).get('database') if source_type == 'Snowflake' else None,
                'container': conn.get('adlsGen2', conn.get('azureBlob', {})).get('container', conn.get('adlsGen2', conn.get('azureBlob', {})).get('subpath')),
                'location': conn.get('amazonS3', conn.get('oneDriveSharePoint', {})).get('location', conn.get('adlsGen2', conn.get('azureBlob', {})).get('location'))
            }
    
    def _extract_tables(self, record: Dict, tables: Dict):
        """Extract table details from MirroredDatabase Full Definition."""
        full_def = record.get('Full Definition')
        if not full_def:
            return
        
        try:
            if isinstance(full_def, str):
                full_def = json.loads(full_def)
            
            props = full_def.get('properties', {})
            source = props.get('source', {})
            type_props = source.get('typeProperties', {})
            database = type_props.get('database', 'Unknown')
            
            for mt in props.get('mountedTables', []):
                mt_props = mt.get('source', {}).get('typeProperties', {})
                schema = mt_props.get('schemaName', 'dbo')
                table_name = mt_props.get('tableName', 'unknown')
                
                table_id = f"{database}.{schema}.{table_name}".lower()
                
                if table_id not in tables:
                    tables[table_id] = {
                        'id': table_id,
                        'name': table_name,
                        'schema': schema,
                        'database': database
                    }
        except Exception as e:
            logger.debug(f"Could not parse Full Definition: {e}")


def compute_stats_from_file(path: Union[str, Path]) -> LineageStatistics:
    """
    Convenience function to compute stats from a file.
    
    Args:
        path: Path to JSON or CSV lineage file
        
    Returns:
        LineageStatistics object
    """
    path = Path(path)
    calc = LineageStatsCalculator()
    
    if path.suffix.lower() == '.json':
        calc.load_json(path)
    else:
        calc.load_csv(path)
    
    return calc.calculate()
