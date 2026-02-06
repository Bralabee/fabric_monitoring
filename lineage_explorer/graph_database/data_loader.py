"""
Lineage Data Loader for Neo4j
-----------------------------
Transforms lineage data into Neo4j graph structure.

Graph Schema:
- (:Workspace {id, name})
- (:FabricItem {id, name, type})
- (:ExternalSource {id, type, display_name, ...})
- (:Table {name, schema, database}) - for granular MirroredDB tables
- (:Connection {id, type})

Relationships:
- (:Workspace)-[:CONTAINS]->(:FabricItem)
- (:FabricItem)-[:DEPENDS_ON]->(:FabricItem)  # internal shortcuts
- (:FabricItem)-[:CONSUMES]->(:ExternalSource)
- (:FabricItem)-[:MIRRORS]->(:Table)
- (:ExternalSource)-[:PROVIDES]->(:Table)
- (:Connection)-[:CONNECTS]->(:ExternalSource)
"""

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from datetime import datetime

from .neo4j_client import Neo4jClient

logger = logging.getLogger(__name__)


class LineageDataLoader:
    """
    Load lineage data from JSON/CSV into Neo4j.
    
    Supports:
    - JSON format (native from extract_lineage.py)
    - CSV format (legacy)
    - Incremental and full refresh modes
    """
    
    # Cypher queries for creating graph elements
    CREATE_CONSTRAINTS = """
    CREATE CONSTRAINT workspace_id IF NOT EXISTS FOR (w:Workspace) REQUIRE w.id IS UNIQUE;
    CREATE CONSTRAINT item_id IF NOT EXISTS FOR (i:FabricItem) REQUIRE i.id IS UNIQUE;
    CREATE CONSTRAINT source_id IF NOT EXISTS FOR (s:ExternalSource) REQUIRE s.id IS UNIQUE;
    CREATE CONSTRAINT table_id IF NOT EXISTS FOR (t:Table) REQUIRE t.id IS UNIQUE;
    CREATE CONSTRAINT connection_id IF NOT EXISTS FOR (c:Connection) REQUIRE c.id IS UNIQUE;
    """
    
    CREATE_INDEXES = """
    CREATE INDEX workspace_name IF NOT EXISTS FOR (w:Workspace) ON (w.name);
    CREATE INDEX item_name IF NOT EXISTS FOR (i:FabricItem) ON (i.name);
    CREATE INDEX item_type IF NOT EXISTS FOR (i:FabricItem) ON (i.type);
    CREATE INDEX source_type IF NOT EXISTS FOR (s:ExternalSource) ON (s.type);
    CREATE INDEX table_name IF NOT EXISTS FOR (t:Table) ON (t.name);
    """
    
    MERGE_WORKSPACE = """
    UNWIND $batch AS row
    MERGE (w:Workspace {id: row.workspace_id})
    ON CREATE SET w.name = row.workspace_name
    ON MATCH SET w.name = row.workspace_name
    """
    
    MERGE_FABRIC_ITEM = """
    UNWIND $batch AS row
    MERGE (i:FabricItem {id: row.item_id})
    ON CREATE SET 
        i.name = row.item_name,
        i.type = row.item_type,
        i.workspace_id = row.workspace_id
    ON MATCH SET
        i.name = row.item_name,
        i.type = row.item_type
    WITH i, row
    MATCH (w:Workspace {id: row.workspace_id})
    MERGE (w)-[:CONTAINS]->(i)
    """
    
    MERGE_EXTERNAL_SOURCE = """
    UNWIND $batch AS row
    MERGE (s:ExternalSource {id: row.source_id})
    ON CREATE SET 
        s.type = row.source_type,
        s.display_name = row.display_name,
        s.location = row.location,
        s.container = row.container,
        s.database = row.database,
        s.connection_details = row.connection_details
    """
    
    CREATE_INTERNAL_EDGE = """
    UNWIND $batch AS row
    MATCH (source:FabricItem {id: row.source_id})
    MATCH (target:FabricItem {id: row.target_id})
    MERGE (target)-[r:DEPENDS_ON]->(source)
    ON CREATE SET 
        r.shortcut_name = row.shortcut_name,
        r.shortcut_path = row.shortcut_path,
        r.table_path = row.table_path
    """
    
    CREATE_EXTERNAL_EDGE = """
    UNWIND $batch AS row
    MATCH (item:FabricItem {id: row.item_id})
    MATCH (source:ExternalSource {id: row.source_id})
    MERGE (item)-[r:CONSUMES]->(source)
    ON CREATE SET
        r.shortcut_name = row.shortcut_name,
        r.shortcut_path = row.shortcut_path
    """
    
    MERGE_TABLE = """
    UNWIND $batch AS row
    MERGE (t:Table {id: row.table_id})
    ON CREATE SET
        t.name = row.table_name,
        t.schema = row.schema_name,
        t.database = row.database_name,
        t.full_path = row.full_path
    """
    
    CREATE_MIRROR_EDGE = """
    UNWIND $batch AS row
    MATCH (item:FabricItem {id: row.item_id})
    MATCH (table:Table {id: row.table_id})
    MERGE (item)-[r:MIRRORS]->(table)
    """
    
    # NEW: Table nodes from OneLake shortcuts (table-level lineage)
    MERGE_SHORTCUT_TABLE = """
    UNWIND $batch AS row
    MERGE (t:Table {id: row.table_id})
    ON CREATE SET
        t.name = row.table_name,
        t.schema = row.schema,
        t.source_item_id = row.source_item_id,
        t.source_workspace_id = row.source_workspace_id,
        t.full_path = row.full_path,
        t.table_type = 'shortcut'
    """
    
    CREATE_SHORTCUT_TABLE_EDGE = """
    UNWIND $batch AS row
    MATCH (target:FabricItem {id: row.target_item_id})
    MATCH (table:Table {id: row.table_id})
    MERGE (target)-[r:USES_TABLE]->(table)
    ON CREATE SET
        r.shortcut_name = row.shortcut_name,
        r.shortcut_path = row.shortcut_path
    """
    
    CREATE_TABLE_SOURCE_EDGE = """
    UNWIND $batch AS row
    MATCH (source:FabricItem {id: row.source_item_id})
    MATCH (table:Table {id: row.table_id})
    MERGE (source)-[r:PROVIDES_TABLE]->(table)
    """
    
    # NEW (opt-in): Report -> SemanticModel edges via PowerBI/Dataset source types
    CREATE_READS_FROM_EDGE = """
    UNWIND $batch AS row
    MATCH (report:FabricItem {id: row.report_id})
    MATCH (dataset:FabricItem {id: row.dataset_id})
    MERGE (report)-[r:READS_FROM]->(dataset)
    """
    
    def __init__(self, client: Neo4jClient):
        """
        Initialize data loader.
        
        Args:
            client: Connected Neo4jClient instance
        """
        self.client = client
        
    def setup_schema(self):
        """Create constraints and indexes."""
        logger.info("Setting up Neo4j schema...")
        
        # Run each constraint separately (some may already exist)
        constraints = [
            "CREATE CONSTRAINT workspace_id IF NOT EXISTS FOR (w:Workspace) REQUIRE w.id IS UNIQUE",
            "CREATE CONSTRAINT item_id IF NOT EXISTS FOR (i:FabricItem) REQUIRE i.id IS UNIQUE",
            "CREATE CONSTRAINT source_id IF NOT EXISTS FOR (s:ExternalSource) REQUIRE s.id IS UNIQUE",
            "CREATE CONSTRAINT table_id IF NOT EXISTS FOR (t:Table) REQUIRE t.id IS UNIQUE",
            "CREATE CONSTRAINT connection_id IF NOT EXISTS FOR (c:Connection) REQUIRE c.id IS UNIQUE",
        ]
        
        indexes = [
            "CREATE INDEX workspace_name IF NOT EXISTS FOR (w:Workspace) ON (w.name)",
            "CREATE INDEX item_name IF NOT EXISTS FOR (i:FabricItem) ON (i.name)",
            "CREATE INDEX item_type IF NOT EXISTS FOR (i:FabricItem) ON (i.type)",
            "CREATE INDEX source_type IF NOT EXISTS FOR (s:ExternalSource) ON (s.type)",
            "CREATE INDEX table_name IF NOT EXISTS FOR (t:Table) ON (t.name)",
        ]
        
        for query in constraints + indexes:
            try:
                self.client.run_write_query(query)
            except Exception as e:
                logger.warning(f"Schema query failed (may already exist): {e}")
        
        logger.info("Schema setup complete")
    
    def load_from_json(
        self,
        json_path: Union[str, Path],
        clear_existing: bool = False
    ) -> Dict[str, Any]:
        """
        Load lineage data from JSON file into Neo4j.
        
        Args:
            json_path: Path to lineage JSON file
            clear_existing: If True, clear database before loading
            
        Returns:
            Summary of loaded data
        """
        json_path = Path(json_path)
        logger.info(f"Loading lineage from {json_path}")
        
        with open(json_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        lineage_items = data.get('lineage', [])
        logger.info(f"Found {len(lineage_items)} lineage records")
        
        if clear_existing:
            self.client.clear_database()
        
        self.setup_schema()
        
        return self._load_lineage_records(lineage_items)
    
    def load_from_csv(
        self,
        csv_path: Union[str, Path],
        clear_existing: bool = False
    ) -> Dict[str, Any]:
        """
        Load lineage data from CSV file into Neo4j.
        
        Args:
            csv_path: Path to lineage CSV file
            clear_existing: If True, clear database before loading
            
        Returns:
            Summary of loaded data
        """
        import pandas as pd
        
        csv_path = Path(csv_path)
        logger.info(f"Loading lineage from {csv_path}")
        
        df = pd.read_csv(csv_path)
        lineage_items = df.to_dict('records')
        logger.info(f"Found {len(lineage_items)} lineage records")
        
        if clear_existing:
            self.client.clear_database()
        
        self.setup_schema()
        
        return self._load_lineage_records(lineage_items)
    
    def _load_lineage_records(self, lineage_items: List[Dict], include_all_items: bool = False) -> Dict[str, Any]:
        """
        Process and load lineage records into Neo4j.
        
        Args:
            lineage_items: List of lineage records
            include_all_items: If True, load ALL items including SemanticModels, Reports, Dataflows (opt-in)
            
        Returns:
            Summary of loaded data
        """
        summary = {
            "workspaces": 0,
            "items": 0,
            "external_sources": 0,
            "tables": 0,
            "internal_edges": 0,
            "external_edges": 0,
            "mirror_edges": 0,
            "load_time_ms": 0
        }
        
        import time
        start_time = time.time()
        
        # Collect unique entities
        workspaces = {}
        items = {}
        external_sources = {}
        tables = {}
        shortcut_tables = {}  # NEW: Table nodes from OneLake shortcuts
        internal_edges = []
        external_edges = []
        mirror_edges = []
        shortcut_table_edges = []  # NEW: Edges linking items to shortcut tables
        reads_from_edges = []  # NEW: Report -> SemanticModel edges (opt-in)
        
        known_item_ids = set()
        
        # First pass: collect all items (to know which OneLake refs are internal)
        for record in lineage_items:
            item_id = record.get('Item ID')
            if item_id:
                known_item_ids.add(item_id)
        
        # Second pass: process records
        for record in lineage_items:
            # Workspace
            ws_id = record.get('Workspace ID')
            ws_name = record.get('Workspace Name', 'Unknown')
            if ws_id and ws_id not in workspaces:
                workspaces[ws_id] = {
                    'workspace_id': ws_id,
                    'workspace_name': ws_name
                }
            
            # Fabric Item
            item_id = record.get('Item ID')
            item_name = record.get('Item Name', 'Unknown')
            item_type = record.get('Item Type', 'Unknown')
            
            # Normalize item type (remove " Shortcut" suffix for cleaner types)
            base_type = item_type.replace(' Shortcut', '')
            
            if item_id and item_id not in items:
                items[item_id] = {
                    'item_id': item_id,
                    'item_name': item_name,
                    'item_type': base_type,
                    'workspace_id': ws_id
                }
            
            # Parse source connection
            conn = self._parse_connection(record.get('Source Connection'))
            # Handle list-type connections (multi-source items like SemanticModels)
            if isinstance(conn, list):
                conn = conn[0] if conn else None
            source_type = conn.get('type') if conn and isinstance(conn, dict) else record.get('Source Type', 'Unknown')
            
            # Process based on source type
            if source_type == 'OneLake' and conn:
                # Check if this is an internal reference
                upstream_id = conn.get('oneLake', {}).get('itemId')
                if upstream_id and upstream_id in known_item_ids:
                    # Internal edge + extract table from path
                    table_path = conn.get('oneLake', {}).get('path', '')
                    workspace_id = conn.get('oneLake', {}).get('workspaceId', '')
                    
                    internal_edges.append({
                        'source_id': upstream_id,
                        'target_id': item_id,
                        'shortcut_name': record.get('Shortcut Name'),
                        'shortcut_path': record.get('Shortcut Path'),
                        'table_path': table_path
                    })
                    
                    # NEW: Extract table node from shortcut path (e.g., "Tables/EDW_DATA/CUSTOMER")
                    if table_path and table_path.startswith('Tables/'):
                        # Parse table_path to extract schema and table name
                        # Format: "Tables/SCHEMA/TABLE" or just "Tables/TABLE"
                        path_parts = table_path.replace('Tables/', '').strip('/').split('/')
                        
                        if len(path_parts) >= 2:
                            # Schema/Table format: Tables/EDW_DATA/DIM_POSITION
                            schema = path_parts[0]  # e.g., "EDW_DATA"
                            table_name = path_parts[-1]  # e.g., "DIM_POSITION" or "DIM_POSITION_HIERARCHY"
                        elif len(path_parts) == 1 and path_parts[0]:
                            # Just table name: Tables/CUSTOMER
                            schema = 'dbo'  # default schema
                            table_name = path_parts[0]
                        else:
                            table_name = None  # Skip invalid paths
                        
                        if table_name:
                            table_id = f"shortcut_{upstream_id}_{schema}_{table_name}".lower().replace(' ', '_')
                        
                            if table_id not in shortcut_tables:
                                shortcut_tables[table_id] = {
                                    'table_id': table_id,
                                    'table_name': table_name,
                                    'schema': schema,
                                    'source_item_id': upstream_id,
                                    'source_workspace_id': workspace_id,
                                    'full_path': table_path
                                }
                        
                            shortcut_table_edges.append({
                                'target_item_id': item_id,
                                'table_id': table_id,
                                'shortcut_name': record.get('Shortcut Name'),
                                'shortcut_path': record.get('Shortcut Path'),
                                'source_item_id': upstream_id
                            })
                else:
                    # External OneLake reference (cross-tenant or unknown)
                    source_id = self._generate_source_id('OneLake', conn)
                    if source_id not in external_sources:
                        external_sources[source_id] = self._create_external_source(
                            source_id, 'OneLake', conn
                        )
                    external_edges.append({
                        'item_id': item_id,
                        'source_id': source_id,
                        'shortcut_name': record.get('Shortcut Name'),
                        'shortcut_path': record.get('Shortcut Path')
                    })
            
            elif source_type in ('AdlsGen2', 'AzureBlob', 'AmazonS3', 'OneDriveSharePoint', 'Snowflake', 'GoogleCloudStorage'):
                # External source
                source_id = self._generate_source_id(source_type, conn)
                if source_id not in external_sources:
                    external_sources[source_id] = self._create_external_source(
                        source_id, source_type, conn
                    )
                external_edges.append({
                    'item_id': item_id,
                    'source_id': source_id,
                    'shortcut_name': record.get('Shortcut Name'),
                    'shortcut_path': record.get('Shortcut Path')
                })
            
            elif item_type == 'MirroredDatabase':
                # Parse mounted tables from Full Definition (legacy)
                full_def = self._parse_definition(record.get('Full Definition'))
                if full_def:
                    self._extract_mirrored_tables(
                        item_id, full_def, tables, mirror_edges, external_sources, external_edges
                    )
                
                # NEW: Process Mirrored Tables from getTablesMirroringStatus API
                mirrored_tables = record.get('Mirrored Tables', [])
                if mirrored_tables and isinstance(mirrored_tables, list):
                    for tbl in mirrored_tables:
                        if not isinstance(tbl, dict):
                            continue
                        schema_name = tbl.get('schemaName', 'dbo')
                        table_name = tbl.get('tableName')
                        if not table_name:
                            continue
                        
                        # Create table ID using item_id for uniqueness
                        table_id = f"mirror_{item_id}_{schema_name}_{table_name}".lower().replace(' ', '_')
                        
                        if table_id not in tables:
                            tables[table_id] = {
                                'table_id': table_id,
                                'table_name': table_name,
                                'schema': schema_name,
                                'database_name': record.get('Source Database', 'Unknown'),
                                'full_path': f"{schema_name}.{table_name}",
                                'table_type': 'mirrored',
                                'status': tbl.get('status'),
                                'processed_rows': tbl.get('processedRows'),
                                'last_sync': tbl.get('lastSyncDateTime')
                            }
                        
                        # Create mirror edge
                        mirror_edges.append({
                            'item_id': item_id,
                            'table_id': table_id
                        })
            
            # NEW (opt-in): Handle PowerBI/Dataset source types for Report -> SemanticModel edges
            elif include_all_items and source_type == 'PowerBI' and item_type == 'Report':
                # For Reports, the Connection ID or Source Connection is the dataset ID
                dataset_id = record.get('Connection ID') or record.get('Source Connection')
                if dataset_id and isinstance(dataset_id, str) and dataset_id in known_item_ids:
                    reads_from_edges.append({
                        'report_id': item_id,
                        'dataset_id': dataset_id
                    })
        
        # Load into Neo4j
        logger.info(f"Loading {len(workspaces)} workspaces...")
        self.client.run_batch_write(self.MERGE_WORKSPACE, list(workspaces.values()))
        summary['workspaces'] = len(workspaces)
        
        logger.info(f"Loading {len(items)} items...")
        self.client.run_batch_write(self.MERGE_FABRIC_ITEM, list(items.values()))
        summary['items'] = len(items)
        
        logger.info(f"Loading {len(external_sources)} external sources...")
        self.client.run_batch_write(self.MERGE_EXTERNAL_SOURCE, list(external_sources.values()))
        summary['external_sources'] = len(external_sources)
        
        logger.info(f"Loading {len(tables)} tables...")
        if tables:
            self.client.run_batch_write(self.MERGE_TABLE, list(tables.values()))
        summary['tables'] = len(tables)
        
        logger.info(f"Creating {len(internal_edges)} internal edges...")
        if internal_edges:
            self.client.run_batch_write(self.CREATE_INTERNAL_EDGE, internal_edges)
        summary['internal_edges'] = len(internal_edges)
        
        logger.info(f"Creating {len(external_edges)} external edges...")
        if external_edges:
            self.client.run_batch_write(self.CREATE_EXTERNAL_EDGE, external_edges)
        summary['external_edges'] = len(external_edges)
        
        logger.info(f"Creating {len(mirror_edges)} mirror edges...")
        if mirror_edges:
            self.client.run_batch_write(self.CREATE_MIRROR_EDGE, mirror_edges)
        summary['mirror_edges'] = len(mirror_edges)
        
        # NEW: Load shortcut tables and edges (table-level lineage)
        logger.info(f"Loading {len(shortcut_tables)} shortcut tables...")
        if shortcut_tables:
            self.client.run_batch_write(self.MERGE_SHORTCUT_TABLE, list(shortcut_tables.values()))
        summary['shortcut_tables'] = len(shortcut_tables)
        
        logger.info(f"Creating {len(shortcut_table_edges)} shortcut table edges...")
        if shortcut_table_edges:
            self.client.run_batch_write(self.CREATE_SHORTCUT_TABLE_EDGE, shortcut_table_edges)
            # Also create PROVIDES_TABLE edges from source items
            source_edges = [{'source_item_id': e['source_item_id'], 'table_id': e['table_id']} 
                           for e in shortcut_table_edges]
            self.client.run_batch_write(self.CREATE_TABLE_SOURCE_EDGE, source_edges)
        summary['shortcut_table_edges'] = len(shortcut_table_edges)
        
        # NEW (opt-in): Load reads_from edges (Report -> SemanticModel)
        if reads_from_edges:
            logger.info(f"Creating {len(reads_from_edges)} report->dataset edges...")
            self.client.run_batch_write(self.CREATE_READS_FROM_EDGE, reads_from_edges)
        summary['reads_from_edges'] = len(reads_from_edges)
        
        summary['load_time_ms'] = int((time.time() - start_time) * 1000)
        logger.info(f"Load complete in {summary['load_time_ms']}ms")
        
        return summary
    
    def _parse_connection(self, conn_value) -> Optional[Dict]:
        """Parse connection value to dict."""
        if not conn_value or conn_value == 'Unknown':
            return None
        
        if isinstance(conn_value, dict):
            return conn_value
        
        if isinstance(conn_value, str):
            try:
                # Handle Python dict literal format
                import ast
                return ast.literal_eval(conn_value)
            except:
                try:
                    return json.loads(conn_value.replace("'", '"').replace('None', 'null'))
                except:
                    return None
        
        return None
    
    def _parse_definition(self, def_value) -> Optional[Dict]:
        """Parse Full Definition value."""
        if not def_value:
            return None
        
        if isinstance(def_value, dict):
            return def_value
        
        try:
            return json.loads(def_value)
        except:
            return None
    
    def _generate_source_id(self, source_type: str, conn: Optional[Dict]) -> str:
        """Generate unique ID for external source."""
        import hashlib
        
        if not conn:
            return f"src_{source_type}_unknown"
        
        # Create signature based on source type
        parts = [source_type]
        
        if source_type == 'OneLake':
            ol = conn.get('oneLake', {})
            parts.extend([ol.get('workspaceId', ''), ol.get('itemId', '')])
        elif source_type in ('AdlsGen2', 'AzureBlob'):
            blob = conn.get('adlsGen2', conn.get('azureBlob', {}))
            parts.extend([blob.get('location', ''), blob.get('container', blob.get('subpath', ''))])
        elif source_type == 'AmazonS3':
            s3 = conn.get('amazonS3', {})
            parts.extend([s3.get('location', ''), s3.get('subpath', '')])
        elif source_type == 'OneDriveSharePoint':
            sp = conn.get('oneDriveSharePoint', {})
            parts.extend([sp.get('location', ''), sp.get('driveId', '')])
        elif source_type == 'Snowflake':
            sf = conn.get('snowflake', conn.get('typeProperties', {}))
            parts.extend([sf.get('connection', ''), sf.get('database', '')])
        
        signature = '|'.join(filter(None, parts))
        return f"src_{hashlib.md5(signature.encode()).hexdigest()[:12]}"
    
    def _create_external_source(
        self,
        source_id: str,
        source_type: str,
        conn: Optional[Dict]
    ) -> Dict[str, Any]:
        """Create external source record."""
        record = {
            'source_id': source_id,
            'source_type': source_type,
            'display_name': source_type,
            'location': None,
            'container': None,
            'database': None,
            'connection_details': json.dumps(conn) if conn else None
        }
        
        if not conn:
            return record
        
        # Extract display details based on type
        if source_type in ('AdlsGen2', 'AzureBlob'):
            blob = conn.get('adlsGen2', conn.get('azureBlob', {}))
            record['location'] = blob.get('location', '')
            record['container'] = blob.get('container', blob.get('subpath', ''))
            record['display_name'] = f"ADLS: {record['container'] or record['location']}"
        
        elif source_type == 'AmazonS3':
            s3 = conn.get('amazonS3', {})
            record['location'] = s3.get('location', '')
            record['container'] = s3.get('subpath', '')
            record['display_name'] = f"S3: {record['location']}"
        
        elif source_type == 'OneDriveSharePoint':
            sp = conn.get('oneDriveSharePoint', {})
            record['location'] = sp.get('location', '')
            record['display_name'] = f"SharePoint: {sp.get('subpath', record['location'])}"
        
        elif source_type == 'Snowflake':
            sf = conn.get('snowflake', conn.get('typeProperties', {}))
            record['database'] = sf.get('database', '')
            record['display_name'] = f"Snowflake: {record['database']}"
        
        elif source_type == 'OneLake':
            ol = conn.get('oneLake', {})
            path = ol.get('path', '')
            record['display_name'] = f"OneLake: {path.split('/')[-1] or 'Item'}"
        
        return record
    
    def _extract_mirrored_tables(
        self,
        item_id: str,
        full_def: Dict,
        tables: Dict,
        mirror_edges: List,
        external_sources: Dict,
        external_edges: List
    ):
        """Extract table-level details from MirroredDatabase definition."""
        props = full_def.get('properties', {})
        source = props.get('source', {})
        source_type = source.get('type', 'Unknown')
        type_props = source.get('typeProperties', {})
        
        # Create external source for the database connection
        if source_type == 'Snowflake':
            source_id = self._generate_source_id(source_type, {'typeProperties': type_props})
            if source_id not in external_sources:
                external_sources[source_id] = {
                    'source_id': source_id,
                    'source_type': source_type,
                    'display_name': f"Snowflake: {type_props.get('database', 'DB')}",
                    'database': type_props.get('database'),
                    'location': type_props.get('connection'),
                    'container': None,
                    'connection_details': json.dumps(type_props)
                }
            
            # Link item to source
            external_edges.append({
                'item_id': item_id,
                'source_id': source_id,
                'shortcut_name': None,
                'shortcut_path': None
            })
        
        # Extract mounted tables
        mounted_tables = props.get('mountedTables', [])
        for mt in mounted_tables:
            mt_source = mt.get('source', {})
            mt_props = mt_source.get('typeProperties', {})
            
            schema_name = mt_props.get('schemaName', 'dbo')
            table_name = mt_props.get('tableName', 'unknown')
            
            # Generate unique table ID
            table_id = f"tbl_{type_props.get('database', '')}_{schema_name}_{table_name}".lower()
            table_id = table_id.replace(' ', '_')
            
            if table_id not in tables:
                tables[table_id] = {
                    'table_id': table_id,
                    'table_name': table_name,
                    'schema_name': schema_name,
                    'database_name': type_props.get('database', 'Unknown'),
                    'full_path': f"{type_props.get('database', '')}.{schema_name}.{table_name}"
                }
            
            mirror_edges.append({
                'item_id': item_id,
                'table_id': table_id
            })
