"""
Lineage Data Models
-------------------
Pydantic models representing the graph-native lineage data structure.
"""

from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field
from datetime import datetime


class ExternalSource(BaseModel):
    """External data source (ADLS, S3, Snowflake, SharePoint, etc.)"""
    id: str
    source_type: str = Field(description="OneLake, AdlsGen2, AmazonS3, Snowflake, OneDriveSharePoint, etc.")
    display_name: str
    connection_details: Dict[str, Any] = Field(default_factory=dict)


class FabricItem(BaseModel):
    """A Fabric item (Lakehouse, MirroredDatabase, Notebook, etc.)"""
    id: str
    name: str
    item_type: str = Field(description="Lakehouse, MirroredDatabase, Notebook, etc.")
    workspace_id: str


class Workspace(BaseModel):
    """A Fabric workspace container"""
    id: str
    name: str


class Table(BaseModel):
    """A table within a Lakehouse, Warehouse, or MirroredDatabase"""
    id: str  # Hash of full_path for uniqueness
    name: str  # e.g., "DIM_POSITION_HIERARCHY"
    schema_name: Optional[str] = Field(default=None, description="Schema name, e.g., 'EDW_DATA'")
    database: Optional[str] = Field(default=None, description="Database/item name")
    full_path: str  # e.g., "Tables/EDW_DATA/DIM_POSITION_HIERARCHY"
    table_type: str = Field(default="table", description="table, shortcut, mirrored")
    source_item_id: Optional[str] = Field(default=None, description="ID of the FabricItem providing this table")


class LineageEdge(BaseModel):
    """An edge representing data flow between nodes"""
    id: str
    source_id: str
    target_id: str
    edge_type: Literal["shortcut", "mirrored", "external", "internal"]
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Shortcut path, table name, etc.")


class LineageGraph(BaseModel):
    """Complete lineage graph"""
    workspaces: List[Workspace]
    items: List[FabricItem]
    external_sources: List[ExternalSource]
    tables: List[Table] = Field(default_factory=list)
    edges: List[LineageEdge]
    
    # Metadata
    generated_at: datetime
    source_file: Optional[str] = None
    total_workspaces: int = 0
    total_items: int = 0
    total_connections: int = 0
    
    def model_post_init(self, __context):
        """Calculate totals after initialization"""
        self.total_workspaces = len(self.workspaces)
        self.total_items = len(self.items)
        self.total_connections = len(self.edges)


class GraphStats(BaseModel):
    """Statistics about the lineage graph"""
    workspace_count: int
    item_count: int
    external_source_count: int
    edge_count: int
    
    # Breakdown by type
    items_by_type: Dict[str, int]
    sources_by_type: Dict[str, int]
    edges_by_type: Dict[str, int]
    
    # Top connections
    top_connected_items: List[Dict[str, Any]]
    top_connected_sources: List[Dict[str, Any]]
