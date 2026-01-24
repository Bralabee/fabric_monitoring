"""
Star Schema Builder for Microsoft Fabric Monitoring Analytics

This module builds dimensional models (star schema) from raw Fabric monitoring data,
enabling efficient SQL queries and semantic model construction for business intelligence.

The star schema design follows Kimball methodology with:
- Fact tables: Granular transactional events
- Dimension tables: Descriptive context for filtering/grouping
- Incremental load support: High-water mark pattern for efficient updates

Business Questions Addressed:
1. What is the failure rate by workspace/domain/user over time?
2. Which pipelines consume the most compute (duration)?
3. Who are the most active users and what are their patterns?
4. What are the long-running operations that need optimization?
5. Which items have recurring failures (constant failures)?
6. What is the trend of activity volume and success rates?
7. How does performance vary by location/domain?
8. What is the utilization pattern across different item types?
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
import pandas as pd

from .utils import resolve_path
from .logger import setup_logging

# Module logger
logger = logging.getLogger(__name__)


# =============================================================================
# STAR SCHEMA DDL DEFINITIONS
# =============================================================================

DDL_FACT_ACTIVITY = """
-- Fact Table: Activity Events (Grain: One row per activity event)
CREATE TABLE IF NOT EXISTS fact_activity (
    -- Surrogate Keys
    activity_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    
    -- Natural/Business Keys
    activity_id STRING NOT NULL,
    
    -- Foreign Keys to Dimensions
    date_sk INT,                    -- FK to dim_date
    time_sk INT,                    -- FK to dim_time
    workspace_sk INT,               -- FK to dim_workspace
    item_sk BIGINT,                 -- FK to dim_item
    user_sk INT,                    -- FK to dim_user
    activity_type_sk INT,           -- FK to dim_activity_type
    status_sk INT,                  -- FK to dim_status
    
    -- Degenerate Dimensions (transaction-specific)
    job_instance_id STRING,
    root_activity_id STRING,
    invoke_type STRING,             -- Manual, Scheduled, etc.
    
    -- Measures
    duration_seconds DOUBLE,
    duration_minutes DOUBLE,
    duration_hours DOUBLE,
    is_failed INT,                  -- 1/0 for easy aggregation
    is_success INT,                 -- 1/0 for easy aggregation
    is_long_running INT,            -- 1/0 flag for duration > threshold
    record_count INT DEFAULT 1,     -- For counting in aggregations
    
    -- Audit Columns
    source_system STRING DEFAULT 'fabric_monitor_hub',
    extracted_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Partitioning/Clustering hints
    activity_date DATE              -- For partitioning
) USING DELTA
PARTITIONED BY (activity_date)
CLUSTER BY (workspace_sk, item_sk);

-- Create indexes for common query patterns
-- (Delta handles this via Z-ORDER, add OPTIMIZE statements in production)
"""

DDL_DIM_DATE = """
-- Dimension Table: Date (Type 0 - Fixed)
CREATE TABLE IF NOT EXISTS dim_date (
    date_sk INT PRIMARY KEY,                -- YYYYMMDD format
    full_date DATE NOT NULL,
    day_of_week INT,                        -- 1=Monday, 7=Sunday
    day_of_week_name STRING,                -- Monday, Tuesday, etc.
    day_of_month INT,
    day_of_year INT,
    week_of_year INT,
    month_number INT,
    month_name STRING,
    quarter INT,
    year INT,
    is_weekend BOOLEAN,
    is_weekday BOOLEAN,
    fiscal_year INT,                        -- Configurable fiscal year
    fiscal_quarter INT
) USING DELTA;
"""

DDL_DIM_TIME = """
-- Dimension Table: Time of Day (Type 0 - Fixed)
CREATE TABLE IF NOT EXISTS dim_time (
    time_sk INT PRIMARY KEY,                -- HHMM format (0000-2359)
    hour_24 INT,                            -- 0-23
    hour_12 INT,                            -- 1-12
    minute INT,                             -- 0-59
    am_pm STRING,                           -- AM/PM
    time_period STRING,                     -- Morning, Afternoon, Evening, Night
    is_business_hours BOOLEAN               -- 9AM-5PM weekdays
) USING DELTA;
"""

DDL_DIM_WORKSPACE = """
-- Dimension Table: Workspace (Type 2 - Slowly Changing)
CREATE TABLE IF NOT EXISTS dim_workspace (
    workspace_sk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    workspace_id STRING NOT NULL,           -- Natural key
    workspace_name STRING,
    workspace_type STRING,                  -- Workspace, PersonalGroup, etc.
    capacity_id STRING,
    domain STRING,                          -- Inferred business domain
    location STRING,                        -- Inferred location/region
    environment STRING,                     -- DEV, TEST, UAT, PRD (inferred)
    
    -- SCD Type 2 tracking
    is_current BOOLEAN DEFAULT TRUE,
    effective_from DATE,
    effective_to DATE,
    version INT DEFAULT 1
) USING DELTA;
"""

DDL_DIM_ITEM = """
-- Dimension Table: Fabric Item (Type 2 - Slowly Changing)
CREATE TABLE IF NOT EXISTS dim_item (
    item_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    item_id STRING NOT NULL,                -- Natural key
    item_name STRING,
    item_type STRING,                       -- DataPipeline, Notebook, Lakehouse, etc.
    item_category STRING,                   -- Compute, Storage, Analytics (derived)
    platform STRING,                        -- 'Fabric' or 'Power BI'
    is_fabric_native BOOLEAN,               -- True for Fabric-only items
    workspace_id STRING,                    -- For joining
    workspace_name STRING,
    
    -- Source lineage (for mirrored databases)
    source_type STRING,                     -- Snowflake, AzureSQL, etc.
    source_database STRING,
    source_connection_id STRING,
    
    -- SCD Type 2 tracking
    is_current BOOLEAN DEFAULT TRUE,
    effective_from DATE,
    effective_to DATE,
    version INT DEFAULT 1
) USING DELTA;
"""

DDL_DIM_USER = """
-- Dimension Table: User (Type 2 - Slowly Changing)
CREATE TABLE IF NOT EXISTS dim_user (
    user_sk INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_principal_name STRING,             -- Natural key (UPN or GUID)
    user_display_name STRING,               -- Friendly name if available
    user_type STRING,                       -- User, ServicePrincipal, System
    domain STRING,                          -- Inferred from UPN
    department STRING,                      -- If available from directory
    
    -- Risk profiling (updated by analysis)
    risk_level STRING DEFAULT 'Low',        -- Low, Medium, High
    activity_tier STRING,                   -- Power User, Regular, Occasional
    
    -- SCD Type 2 tracking
    is_current BOOLEAN DEFAULT TRUE,
    effective_from DATE,
    effective_to DATE,
    version INT DEFAULT 1
) USING DELTA;
"""

DDL_DIM_ACTIVITY_TYPE = """
-- Dimension Table: Activity Type (Type 0 - Fixed reference)
CREATE TABLE IF NOT EXISTS dim_activity_type (
    activity_type_sk INT PRIMARY KEY,
    activity_type STRING NOT NULL,          -- RenameFileOrDirectory, ExecutePipeline, etc.
    activity_category STRING,               -- File Operations, Compute, Query, Admin
    is_compute_activity BOOLEAN,            -- TRUE for activities that consume compute
    is_fabric_native BOOLEAN,               -- TRUE for Fabric-native vs legacy Power BI
    description STRING
) USING DELTA;
"""

DDL_DIM_STATUS = """
-- Dimension Table: Status (Type 0 - Fixed reference)
CREATE TABLE IF NOT EXISTS dim_status (
    status_sk INT PRIMARY KEY,
    status_code STRING NOT NULL,            -- Succeeded, Failed, InProgress, Cancelled
    status_category STRING,                 -- Success, Failure, Pending
    is_terminal BOOLEAN,                    -- TRUE if final state
    severity INT                            -- 0=Success, 1=Warning, 2=Error
) USING DELTA;
"""

DDL_FACT_DAILY_METRICS = """
-- Aggregate Fact Table: Daily Metrics (Pre-aggregated for dashboards)
CREATE TABLE IF NOT EXISTS fact_daily_metrics (
    metric_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_sk INT NOT NULL,
    workspace_sk INT,
    domain STRING,
    item_type STRING,
    
    -- Aggregated Measures
    total_activities INT,
    successful_activities INT,
    failed_activities INT,
    in_progress_activities INT,
    cancelled_activities INT,
    
    success_rate DOUBLE,
    failure_rate DOUBLE,
    
    total_duration_seconds DOUBLE,
    avg_duration_seconds DOUBLE,
    max_duration_seconds DOUBLE,
    min_duration_seconds DOUBLE,
    
    unique_users INT,
    unique_items INT,
    
    long_running_count INT,                 -- Activities > threshold
    
    -- Audit
    aggregated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) USING DELTA
PARTITIONED BY (date_sk);
"""

DDL_FACT_USER_METRICS = """
-- Aggregate Fact Table: User Performance Metrics
CREATE TABLE IF NOT EXISTS fact_user_metrics (
    metric_sk BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    date_sk INT NOT NULL,
    user_sk INT NOT NULL,
    
    total_activities INT,
    successful_activities INT,
    failed_activities INT,
    success_rate DOUBLE,
    
    total_duration_seconds DOUBLE,
    avg_duration_seconds DOUBLE,
    
    unique_workspaces INT,
    unique_items INT,
    unique_item_types INT,
    
    -- Risk indicators
    failure_rate_7d_avg DOUBLE,
    activity_count_7d_avg DOUBLE,
    risk_score DOUBLE,
    
    aggregated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
) USING DELTA
PARTITIONED BY (date_sk);
"""

# Collect all DDLs
ALL_STAR_SCHEMA_DDLS = {
    "dim_date": DDL_DIM_DATE,
    "dim_time": DDL_DIM_TIME,
    "dim_workspace": DDL_DIM_WORKSPACE,
    "dim_item": DDL_DIM_ITEM,
    "dim_user": DDL_DIM_USER,
    "dim_activity_type": DDL_DIM_ACTIVITY_TYPE,
    "dim_status": DDL_DIM_STATUS,
    "fact_activity": DDL_FACT_ACTIVITY,
    "fact_daily_metrics": DDL_FACT_DAILY_METRICS,
    "fact_user_metrics": DDL_FACT_USER_METRICS,
}


# =============================================================================
# INCREMENTAL LOAD TRACKING
# =============================================================================

class IncrementalLoadTracker:
    """
    Tracks high-water marks for incremental data loading.
    
    Uses a simple JSON file to store last processed timestamps per entity.
    In production, this would be a Delta table or database table.
    """

    def __init__(self, tracker_path: Optional[Path] = None):
        self.tracker_path = tracker_path or resolve_path("exports/star_schema/.incremental_state.json")
        self.tracker_path.parent.mkdir(parents=True, exist_ok=True)
        self._state = self._load_state()

    def _load_state(self) -> Dict[str, Any]:
        """Load existing state or create new."""
        if self.tracker_path.exists():
            try:
                with open(self.tracker_path, 'r') as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            "high_water_marks": {},
            "last_full_load": None,
            "load_history": []
        }

    def _save_state(self) -> None:
        """Persist state to disk."""
        with open(self.tracker_path, 'w') as f:
            json.dump(self._state, f, indent=2, default=str)

    def get_high_water_mark(self, entity: str, default: Optional[datetime] = None) -> Optional[datetime]:
        """Get the last processed timestamp for an entity."""
        hwm = self._state["high_water_marks"].get(entity)
        if hwm:
            return datetime.fromisoformat(hwm)
        return default

    def set_high_water_mark(self, entity: str, timestamp: datetime) -> None:
        """Update the high water mark for an entity."""
        self._state["high_water_marks"][entity] = timestamp.isoformat()
        self._save_state()

    def record_load(self, entity: str, records_processed: int, duration_seconds: float) -> None:
        """Record a load operation for audit purposes."""
        self._state["load_history"].append({
            "entity": entity,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "records_processed": records_processed,
            "duration_seconds": round(duration_seconds, 2)
        })
        # Keep last 100 entries
        self._state["load_history"] = self._state["load_history"][-100:]
        self._save_state()


# =============================================================================
# DIMENSION BUILDERS
# =============================================================================

class DimensionBuilder:
    """Base class for building dimension tables."""

    def __init__(self, logger: Optional[logging.Logger] = None):
        self.logger = logger or logging.getLogger(__name__)

    def generate_surrogate_key(self, *values) -> int:
        """Generate a deterministic surrogate key from natural key values."""
        key_string = "|".join(str(v) for v in values)
        hash_bytes = hashlib.md5(key_string.encode()).digest()
        # Use first 4 bytes as int (deterministic across runs)
        return int.from_bytes(hash_bytes[:4], 'big') % (2**31 - 1)


class DateDimensionBuilder(DimensionBuilder):
    """Builds the Date dimension table."""

    def build(
        self,
        start_date: datetime,
        end_date: datetime,
        fiscal_year_start_month: int = 7  # July (common for fiscal years)
    ) -> pd.DataFrame:
        """
        Generate date dimension for a date range.
        
        Args:
            start_date: Start of date range
            end_date: End of date range
            fiscal_year_start_month: Month when fiscal year starts (1-12)
        """
        dates = pd.date_range(start=start_date, end=end_date, freq='D')

        records = []
        for d in dates:
            # Calculate fiscal year/quarter
            if d.month >= fiscal_year_start_month:
                fiscal_year = d.year + 1
                fiscal_month = d.month - fiscal_year_start_month + 1
            else:
                fiscal_year = d.year
                fiscal_month = d.month + (12 - fiscal_year_start_month + 1)
            fiscal_quarter = (fiscal_month - 1) // 3 + 1

            records.append({
                "date_sk": int(d.strftime("%Y%m%d")),
                "full_date": d.date(),
                "day_of_week": d.weekday() + 1,  # 1=Monday
                "day_of_week_name": d.strftime("%A"),
                "day_of_month": d.day,
                "day_of_year": d.timetuple().tm_yday,
                "week_of_year": d.isocalendar()[1],
                "month_number": d.month,
                "month_name": d.strftime("%B"),
                "quarter": (d.month - 1) // 3 + 1,
                "year": d.year,
                "is_weekend": d.weekday() >= 5,
                "is_weekday": d.weekday() < 5,
                "fiscal_year": fiscal_year,
                "fiscal_quarter": fiscal_quarter
            })

        return pd.DataFrame(records)


class TimeDimensionBuilder(DimensionBuilder):
    """Builds the Time dimension table."""

    def build(self) -> pd.DataFrame:
        """Generate time dimension for all hours/minutes of a day."""
        records = []

        for hour in range(24):
            for minute in [0, 15, 30, 45]:  # 15-minute granularity
                time_sk = hour * 100 + minute
                hour_12 = hour % 12 or 12
                am_pm = "AM" if hour < 12 else "PM"

                if 5 <= hour < 12:
                    time_period = "Morning"
                elif 12 <= hour < 17:
                    time_period = "Afternoon"
                elif 17 <= hour < 21:
                    time_period = "Evening"
                else:
                    time_period = "Night"

                is_business = 9 <= hour < 17

                records.append({
                    "time_sk": time_sk,
                    "hour_24": hour,
                    "hour_12": hour_12,
                    "minute": minute,
                    "am_pm": am_pm,
                    "time_period": time_period,
                    "is_business_hours": is_business
                })

        return pd.DataFrame(records)


class WorkspaceDimensionBuilder(DimensionBuilder):
    """Builds the Workspace dimension from activity data."""

    ENVIRONMENT_PATTERNS = {
        "DEV": ["dev", "[dev]", "-dev", "_dev", "development"],
        "TEST": ["test", "[test]", "-test", "_test", "testing", "qa"],
        "UAT": ["uat", "[uat]", "-uat", "_uat", "staging", "prj_uat", "prj uat"],
        "PRD": ["prd", "prod", "[prd]", "-prd", "_prd", "production", "live"]
    }

    def infer_environment(self, workspace_name: str) -> str:
        """Infer environment from workspace name."""
        if not workspace_name:
            return "Unknown"

        name_lower = workspace_name.lower()
        for env, patterns in self.ENVIRONMENT_PATTERNS.items():
            if any(p in name_lower for p in patterns):
                return env
        return "Unknown"

    def build_from_activities(
        self,
        activities: List[Dict[str, Any]],
        existing_dim: Optional[pd.DataFrame] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Build workspace dimension from activity records.
        
        Returns:
            Tuple of (full dimension DataFrame, new/changed records DataFrame)
        """
        # Extract unique workspaces
        workspace_data = {}
        for activity in activities:
            ws_id = activity.get("workspace_id")
            if not ws_id or ws_id in workspace_data:
                continue

            ws_name = activity.get("workspace_name") or "Unknown"
            workspace_data[ws_id] = {
                "workspace_id": ws_id,
                "workspace_name": ws_name,
                "workspace_type": activity.get("workspace_type", "Workspace"),
                "capacity_id": activity.get("capacity_id"),
                "domain": activity.get("domain", "General"),
                "location": activity.get("location", "Global"),
                "environment": self.infer_environment(ws_name),
                "is_current": True,
                "effective_from": datetime.now(timezone.utc).date(),
                "effective_to": None,
                "version": 1
            }

        new_df = pd.DataFrame(list(workspace_data.values()))

        if new_df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Generate surrogate keys
        new_df["workspace_sk"] = new_df["workspace_id"].apply(
            lambda x: self.generate_surrogate_key("workspace", x)
        )

        # If we have existing data, identify changes (SCD Type 2)
        if existing_dim is not None and not existing_dim.empty:
            # Find truly new workspaces
            existing_ids = set(existing_dim[existing_dim["is_current"]]["workspace_id"])
            new_records = new_df[~new_df["workspace_id"].isin(existing_ids)]

            # Combine with existing
            full_dim = pd.concat([existing_dim, new_records], ignore_index=True)
            return full_dim, new_records

        return new_df, new_df


class ItemDimensionBuilder(DimensionBuilder):
    """Builds the Item dimension from activity and job data."""

    # Expanded ITEM_CATEGORIES to cover all known Fabric item types
    # Coverage: 20+ item types from actual tenant data
    ITEM_CATEGORIES = {
        "Compute": [
            "DataPipeline", "Pipeline",  # Data orchestration
            "Notebook", "SynapseNotebook",  # Interactive compute
            "SparkJobDefinition",  # Batch Spark
            "Dataflow", "DataFlow",  # ETL (case variations in API)
            "CopyJob",  # Data movement
        ],
        "Storage": [
            "Lakehouse",  # Delta Lake storage
            "Warehouse",  # SQL analytics
            "KQLDatabase", "KustoDatabase",  # Real-time analytics (alias)
            "MirroredDatabase",  # Database mirroring
            "SnowflakeDatabase",  # External Snowflake connection
        ],
        "Analytics": [
            "KQLQueryset",  # KQL queries
            "SemanticModel", "Dataset",  # Power BI models
            "Report", "Dashboard",  # Power BI visuals
            "Datamart",  # Self-service analytics
        ],
        "Realtime": [
            "Eventstream",  # Event ingestion
            "Reflex",  # Real-time reactions
        ],
        "Infrastructure": [
            "Environment",  # Spark/Python environments
            "MLModel",  # Deployed ML models
            "MLExperiment",  # ML experiments
            "GraphQLApi",  # API endpoints
        ],
    }

    # Fabric-native item types (not Power BI)
    # These are items that only exist in Microsoft Fabric, not in classic Power BI
    FABRIC_NATIVE_TYPES = {
        # Compute
        "DataPipeline", "Pipeline", "Notebook", "SynapseNotebook",
        "SparkJobDefinition", "CopyJob",
        # Storage
        "Lakehouse", "Warehouse", "KQLDatabase", "KustoDatabase",
        "MirroredDatabase", "SnowflakeDatabase",
        # Realtime
        "Eventstream", "Reflex",
        # Infrastructure
        "Environment", "MLModel", "MLExperiment", "GraphQLApi",
        # Data Engineering
        "KQLQueryset",
    }

    # Power BI item types (can exist in both Power BI and Fabric)
    POWERBI_TYPES = {
        "Report", "Dashboard", "Dataset", "SemanticModel",
        "Dataflow", "DataFlow", "Datamart", "PaginatedReport",
    }

    def categorize_item(self, item_type: str) -> str:
        """Categorize item type into a broader category."""
        if not item_type:
            return "Other"

        for category, types in self.ITEM_CATEGORIES.items():
            if item_type in types:
                return category
        return "Other"

    def get_platform(self, item_type: str) -> str:
        """
        Determine the platform for an item type.
        
        Returns:
            'Fabric' - Fabric-native items (Lakehouse, Notebook, etc.)
            'Power BI' - Power BI items (Report, Dashboard, Dataset)
            'Unknown' - Unrecognized item type
        """
        if not item_type:
            return "Unknown"
        if item_type in self.FABRIC_NATIVE_TYPES:
            return "Fabric"
        if item_type in self.POWERBI_TYPES:
            return "Power BI"
        return "Unknown"

    def is_fabric_native(self, item_type: str) -> bool:
        """Check if item type is Fabric-native (not Power BI)."""
        return item_type in self.FABRIC_NATIVE_TYPES if item_type else False

    def build_from_activities(
        self,
        activities: List[Dict[str, Any]],
        lineage_data: Optional[List[Dict[str, Any]]] = None,
        existing_dim: Optional[pd.DataFrame] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Build item dimension from activity and optionally lineage data.
        """
        item_data = {}

        # Extract from activities
        for activity in activities:
            item_id = activity.get("item_id")
            if not item_id or item_id in item_data:
                continue

            item_type = activity.get("item_type") or "Unknown"
            item_data[item_id] = {
                "item_id": item_id,
                "item_name": activity.get("item_name") or "Unknown",
                "item_type": item_type,
                "item_category": self.categorize_item(item_type),
                "platform": self.get_platform(item_type),  # 'Fabric' or 'Power BI'
                "is_fabric_native": self.is_fabric_native(item_type),  # True/False
                "workspace_id": activity.get("workspace_id"),
                "workspace_name": activity.get("workspace_name"),
                "source_type": None,
                "source_database": None,
                "source_connection_id": None,
                "is_current": True,
                "effective_from": datetime.now(timezone.utc).date(),
                "effective_to": None,
                "version": 1
            }

        # Enrich with lineage data if available
        if lineage_data:
            lineage_lookup = {l.get("Item ID"): l for l in lineage_data if l.get("Item ID")}
            for item_id, item in item_data.items():
                if item_id in lineage_lookup:
                    lineage = lineage_lookup[item_id]
                    item["source_type"] = lineage.get("Source Type")
                    item["source_database"] = lineage.get("Source Database")
                    item["source_connection_id"] = lineage.get("Connection ID")

        new_df = pd.DataFrame(list(item_data.values()))

        if new_df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Generate surrogate keys
        new_df["item_sk"] = new_df["item_id"].apply(
            lambda x: self.generate_surrogate_key("item", x)
        )

        # Handle existing data (SCD Type 2)
        if existing_dim is not None and not existing_dim.empty:
            existing_ids = set(existing_dim[existing_dim["is_current"]]["item_id"])
            new_records = new_df[~new_df["item_id"].isin(existing_ids)]
            full_dim = pd.concat([existing_dim, new_records], ignore_index=True)
            return full_dim, new_records

        return new_df, new_df


class UserDimensionBuilder(DimensionBuilder):
    """Builds the User dimension from activity data."""

    def classify_user_type(self, user_value: str) -> str:
        """Determine if user is a person, service principal, or system."""
        if not user_value or not isinstance(user_value, str):
            return "Unknown"

        # GUIDs are typically service principals
        import re
        guid_pattern = r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        if re.match(guid_pattern, user_value.lower()):
            return "ServicePrincipal"

        # System accounts
        if any(x in user_value.lower() for x in ["system", "service", "app", "automation"]):
            return "System"

        return "User"

    def extract_domain_from_upn(self, upn: str) -> Optional[str]:
        """Extract email domain from UPN."""
        if not upn or not isinstance(upn, str) or "@" not in upn:
            return None
        return upn.split("@")[-1]

    def build_from_activities(
        self,
        activities: List[Dict[str, Any]],
        existing_dim: Optional[pd.DataFrame] = None
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Build user dimension from activity records."""
        user_data = {}

        for activity in activities:
            user_value = activity.get("submitted_by")
            if not user_value or not isinstance(user_value, str) or user_value in user_data:
                continue

            user_data[user_value] = {
                "user_principal_name": user_value,
                "user_display_name": user_value.split("@")[0] if "@" in user_value else user_value,
                "user_type": self.classify_user_type(user_value),
                "domain": self.extract_domain_from_upn(user_value),
                "department": None,
                "risk_level": "Low",
                "activity_tier": "Regular",
                "is_current": True,
                "effective_from": datetime.now(timezone.utc).date(),
                "effective_to": None,
                "version": 1
            }

        new_df = pd.DataFrame(list(user_data.values()))

        if new_df.empty:
            return pd.DataFrame(), pd.DataFrame()

        # Generate surrogate keys
        new_df["user_sk"] = new_df["user_principal_name"].apply(
            lambda x: self.generate_surrogate_key("user", x)
        )

        # Handle existing data
        if existing_dim is not None and not existing_dim.empty:
            existing_ids = set(existing_dim[existing_dim["is_current"]]["user_principal_name"])
            new_records = new_df[~new_df["user_principal_name"].isin(existing_ids)]
            full_dim = pd.concat([existing_dim, new_records], ignore_index=True)
            return full_dim, new_records

        return new_df, new_df


class ActivityTypeDimensionBuilder(DimensionBuilder):
    """Builds the Activity Type reference dimension."""

    # Known activity types and their classifications
    # Format: "ActivityType": (category, is_compute_activity, is_fabric_native)
    ACTIVITY_TYPES = {
        # File Operations
        "RenameFileOrDirectory": ("File Operations", False, True),
        "DeleteFile": ("File Operations", False, True),
        "DeleteFileOrBlob": ("File Operations", False, True),
        "CreateFile": ("File Operations", False, True),
        "MoveFile": ("File Operations", False, True),
        "CreateDirectory": ("File Operations", False, True),
        "SetFileProperties": ("File Operations", False, True),

        # Artifact Operations (CRUD on Fabric items)
        "ReadArtifact": ("Artifact Operations", False, True),
        "CreateArtifact": ("Artifact Operations", False, True),
        "UpdateArtifact": ("Artifact Operations", False, True),
        "DeleteArtifact": ("Artifact Operations", False, True),
        "ShareArtifact": ("Artifact Operations", False, True),

        # Compute Operations
        "ExecutePipeline": ("Compute", True, True),
        "ExecuteNotebook": ("Compute", True, True),
        "ExecuteSparkJob": ("Compute", True, True),
        "ExecuteDataflow": ("Compute", True, False),
        "RunArtifact": ("Compute", True, True),
        "CancelRunningArtifact": ("Compute", False, True),
        "StartRunNotebook": ("Compute", True, True),
        "StopNotebookSession": ("Compute", False, True),
        "ConfigureNotebook": ("Compute", False, True),

        # Job History Activity Types (from Fabric Job History API)
        "Pipeline": ("Compute", True, True),
        "PipelineRunNotebook": ("Compute", True, True),
        "Refresh": ("Compute", True, True),
        "Publish": ("Compute", True, True),
        "RunNotebookInteractive": ("Compute", True, True),
        "DataflowGen2": ("Compute", True, True),
        "SparkJob": ("Compute", True, True),
        "ScheduledNotebook": ("Compute", True, True),
        "OneLakeShortcut": ("Compute", True, True),

        # Spark/Livy Operations
        "ViewSparkAppLog": ("Spark", False, True),
        "ViewSparkApplication": ("Spark", False, True),
        "ViewSparkAppInputOutput": ("Spark", False, True),
        "LoadSparkAppLog": ("Spark", False, True),
        "LivySparkSessionAcquired": ("Spark", True, True),
        "LivySparkSessionCancelled": ("Spark", False, True),
        "CreateCheckpoint": ("Spark", False, True),
        "MountStorageByMssparkutils": ("Spark", False, True),

        # Lakehouse Operations
        "ReadLakehouse": ("Lakehouse", False, True),
        "WriteLakehouse": ("Lakehouse", False, True),
        "ListLakehouseTables": ("Lakehouse", False, True),
        "GetLakehouseTableDetails": ("Lakehouse", False, True),
        "PreviewLakehouseTable": ("Lakehouse", False, True),
        "LoadLakehouseTable": ("Lakehouse", False, True),
        "DeleteLakehouseTable": ("Lakehouse", False, True),
        "GetDataArtifactTableDetails": ("Lakehouse", False, True),

        # Shortcut Operations
        "CreateShortcut": ("Shortcut", False, True),
        "DeleteShortcut": ("Shortcut", False, True),

        # Query Operations
        "ExecuteKQLQuery": ("Query", True, True),
        "ExecuteSQLQuery": ("Query", True, True),
        "ConnectWarehouseAndSqlAnalyticsEndpointLakehouseFromExternalApp": ("Query", False, True),
        "CreateSqlQueryFromWarehouse": ("Query", False, True),
        "CreateVisualQueryFromWarehouse": ("Query", False, True),
        "DeleteSavedQueryFromWarehouse": ("Query", False, True),
        "GetArtifactSqlAuditConfiguration": ("Query", False, True),

        # Admin/Workspace Operations
        "CreateWorkspace": ("Admin", False, True),
        "UpdateWorkspace": ("Admin", False, True),
        "DeleteWorkspace": ("Admin", False, True),
        "AddWorkspaceUser": ("Admin", False, True),

        # Environment Operations
        "StartPublishEnvironment": ("Environment", False, True),
        "FinishPublishEnvironment": ("Environment", False, True),
        "ReadEnvironmentResource": ("Environment", False, True),
        "UpdateEnvironmentSparkSettings": ("Environment", False, True),

        # ML/AI Operations
        "DeleteModelEndpoint": ("ML", False, True),
        "RequestCopilot": ("ML", False, True),

        # Security/Compliance
        "SensitivityLabelApplied": ("Security", False, True),
        "SensitivityLabelChanged": ("Security", False, True),

        # Report/Analytics Operations
        "ViewReport": ("Analytics", False, False),
        "RefreshDataset": ("Compute", True, False),
    }

    def build(self) -> pd.DataFrame:
        """Generate activity type dimension with all known types."""
        records = []
        sk = 0

        for activity_type, (category, is_compute, is_fabric) in self.ACTIVITY_TYPES.items():
            sk += 1
            records.append({
                "activity_type_sk": sk,
                "activity_type": activity_type,
                "activity_category": category,
                "is_compute_activity": is_compute,
                "is_fabric_native": is_fabric,
                "description": f"{category} activity: {activity_type}"
            })

        # Add catch-all for unknown types
        records.append({
            "activity_type_sk": 999,
            "activity_type": "Unknown",
            "activity_category": "Unknown",
            "is_compute_activity": False,
            "is_fabric_native": False,
            "description": "Unknown or unmapped activity type"
        })

        return pd.DataFrame(records)


class StatusDimensionBuilder(DimensionBuilder):
    """Builds the Status reference dimension."""

    STATUSES = {
        "Succeeded": ("Success", True, 0),
        "Completed": ("Success", True, 0),
        "Failed": ("Failure", True, 2),
        "Cancelled": ("Failure", True, 1),
        "InProgress": ("Pending", False, 0),
        "Running": ("Pending", False, 0),
        "Queued": ("Pending", False, 0),
        "Unknown": ("Unknown", True, 1),
    }

    def build(self) -> pd.DataFrame:
        """Generate status dimension."""
        records = []
        sk = 0

        for status, (category, is_terminal, severity) in self.STATUSES.items():
            sk += 1
            records.append({
                "status_sk": sk,
                "status_code": status,
                "status_category": category,
                "is_terminal": is_terminal,
                "severity": severity
            })

        return pd.DataFrame(records)


# =============================================================================
# FACT TABLE BUILDER
# =============================================================================

class FactActivityBuilder(DimensionBuilder):
    """Builds the Fact Activity table from raw activity data."""

    LONG_RUNNING_THRESHOLD_SECONDS = 3600  # 1 hour

    def __init__(
        self,
        dim_workspace: pd.DataFrame,
        dim_item: pd.DataFrame,
        dim_user: pd.DataFrame,
        dim_activity_type: pd.DataFrame,
        dim_status: pd.DataFrame,
        logger: Optional[logging.Logger] = None
    ):
        super().__init__(logger)

        # Build lookup dictionaries for efficient FK resolution
        self.workspace_lookup = self._build_lookup(dim_workspace, "workspace_id", "workspace_sk")
        # Fallback lookup by workspace_name (for records without workspace_id, e.g., job failures)
        self.workspace_name_lookup = self._build_lookup(dim_workspace, "workspace_name", "workspace_sk")
        self.item_lookup = self._build_lookup(dim_item, "item_id", "item_sk")
        self.user_lookup = self._build_lookup(dim_user, "user_principal_name", "user_sk")
        self.activity_type_lookup = self._build_lookup(dim_activity_type, "activity_type", "activity_type_sk")
        self.status_lookup = self._build_lookup(dim_status, "status_code", "status_sk")

    def _build_lookup(self, df: pd.DataFrame, key_col: str, value_col: str) -> Dict[str, int]:
        """Build a lookup dictionary from a dimension DataFrame."""
        if df.empty:
            return {}

        # For SCD Type 2 dims, only use current records
        if "is_current" in df.columns:
            df = df[df["is_current"]]

        return dict(zip(df[key_col].astype(str), df[value_col]))

    def build_from_activities(
        self,
        activities: List[Dict[str, Any]],
        high_water_mark: Optional[datetime] = None
    ) -> pd.DataFrame:
        """
        Build fact activity records from raw activities.
        
        Args:
            activities: List of activity dictionaries
            high_water_mark: Only process activities after this timestamp (incremental)
        """
        records = []
        extracted_at = datetime.now(timezone.utc)

        for activity in activities:
            # Parse timestamp for incremental filtering
            start_time_str = activity.get("start_time")
            if start_time_str:
                try:
                    start_time = pd.to_datetime(start_time_str, utc=True)

                    # Skip if before high water mark (incremental load)
                    if high_water_mark and start_time <= pd.to_datetime(high_water_mark, utc=True):
                        continue
                except Exception:
                    start_time = None
            else:
                start_time = None

            # Calculate duration
            duration_seconds = float(activity.get("duration_seconds") or 0)
            duration_minutes = duration_seconds / 60
            duration_hours = duration_seconds / 3600

            # Determine status
            status = activity.get("status") or "Unknown"
            is_failed = 1 if status.lower() in ["failed", "cancelled", "error"] else 0
            is_success = 1 if status.lower() in ["succeeded", "completed", "success"] else 0
            is_long_running = 1 if duration_seconds > self.LONG_RUNNING_THRESHOLD_SECONDS else 0

            # Build date/time keys (handle NaT/None)
            # Use start_time if available, fallback to end_time (for job failures without start_time)
            event_time = start_time
            if not event_time or pd.isna(event_time):
                end_time_str = activity.get("end_time")
                if end_time_str:
                    try:
                        event_time = pd.to_datetime(end_time_str, utc=True)
                    except Exception:
                        event_time = None

            if event_time and pd.notna(event_time):
                date_sk = int(event_time.strftime("%Y%m%d"))
                # Round to nearest 15 minutes for time_sk
                hour = event_time.hour
                minute = (event_time.minute // 15) * 15
                time_sk = hour * 100 + minute
                activity_date = event_time.date()
            else:
                date_sk = None
                time_sk = None
                activity_date = None

            # Resolve foreign keys
            # Try workspace_id first, fallback to workspace_name (for job failures without workspace_id)
            workspace_id = activity.get("workspace_id")
            workspace_sk = self.workspace_lookup.get(str(workspace_id)) if workspace_id else None
            if workspace_sk is None:
                workspace_name = activity.get("workspace_name")
                if workspace_name:
                    workspace_sk = self.workspace_name_lookup.get(str(workspace_name))

            item_sk = self.item_lookup.get(str(activity.get("item_id")))
            user_sk = self.user_lookup.get(str(activity.get("submitted_by")))
            activity_type_sk = self.activity_type_lookup.get(
                activity.get("activity_type"),
                self.activity_type_lookup.get("Unknown", 999)
            )
            status_sk = self.status_lookup.get(status, self.status_lookup.get("Unknown", 8))

            records.append({
                "activity_id": activity.get("activity_id"),
                "date_sk": date_sk,
                "time_sk": time_sk,
                "workspace_sk": workspace_sk,
                "item_sk": item_sk,
                "user_sk": user_sk,
                "activity_type_sk": activity_type_sk,
                "status_sk": status_sk,
                "job_instance_id": activity.get("job_instance_id"),
                "root_activity_id": activity.get("root_activity_id"),
                "invoke_type": activity.get("invoke_type"),
                "duration_seconds": duration_seconds,
                "duration_minutes": duration_minutes,
                "duration_hours": duration_hours,
                "is_failed": is_failed,
                "is_success": is_success,
                "is_long_running": is_long_running,
                "record_count": 1,
                "source_system": "fabric_monitor_hub",
                "extracted_at": extracted_at,
                "activity_date": activity_date
            })

        return pd.DataFrame(records)


# =============================================================================
# AGGREGATE BUILDERS
# =============================================================================

class DailyMetricsBuilder:
    """Builds pre-aggregated daily metrics for dashboards."""

    def build_from_fact(self, fact_activity: pd.DataFrame) -> pd.DataFrame:
        """Aggregate fact table to daily metrics."""
        if fact_activity.empty:
            return pd.DataFrame()

        # Group by date, workspace, domain, item_type
        # (This would need joins to dimensions in a real implementation)
        grouped = fact_activity.groupby(["date_sk", "workspace_sk"]).agg({
            "record_count": "sum",
            "is_success": "sum",
            "is_failed": "sum",
            "duration_seconds": ["sum", "mean", "max", "min"],
            "is_long_running": "sum",
            "user_sk": pd.Series.nunique,
            "item_sk": pd.Series.nunique
        }).reset_index()

        # Flatten column names
        grouped.columns = [
            "date_sk", "workspace_sk",
            "total_activities", "successful_activities", "failed_activities",
            "total_duration_seconds", "avg_duration_seconds", "max_duration_seconds", "min_duration_seconds",
            "long_running_count", "unique_users", "unique_items"
        ]

        # Calculate rates
        grouped["success_rate"] = (grouped["successful_activities"] / grouped["total_activities"] * 100).round(2)
        grouped["failure_rate"] = (grouped["failed_activities"] / grouped["total_activities"] * 100).round(2)

        # Add aggregation timestamp
        grouped["aggregated_at"] = datetime.now(timezone.utc)

        return grouped


# =============================================================================
# MAIN STAR SCHEMA BUILDER
# =============================================================================

class StarSchemaBuilder:
    """
    Main orchestrator for building the star schema from raw monitoring data.
    
    Supports both full refresh and incremental loading patterns.
    """

    def __init__(
        self,
        output_directory: Optional[str] = None,
        logger: Optional[logging.Logger] = None,
        workspace_lookup_path: Optional[str] = None
    ):
        self.logger = logger or setup_logging(name="star_schema_builder")
        self.workspace_lookup_path = workspace_lookup_path
        self._workspace_lookup: Optional[Dict[str, str]] = None
        self.output_directory = resolve_path(
            output_directory or os.getenv("STAR_SCHEMA_OUTPUT_DIR", "exports/star_schema")
        )
        self.output_directory.mkdir(parents=True, exist_ok=True)

        self.tracker = IncrementalLoadTracker(self.output_directory / ".incremental_state.json")

        # Initialize dimension builders
        self.date_builder = DateDimensionBuilder(self.logger)
        self.time_builder = TimeDimensionBuilder(self.logger)
        self.workspace_builder = WorkspaceDimensionBuilder(self.logger)
        self.item_builder = ItemDimensionBuilder(self.logger)
        self.user_builder = UserDimensionBuilder(self.logger)
        self.activity_type_builder = ActivityTypeDimensionBuilder(self.logger)
        self.status_builder = StatusDimensionBuilder(self.logger)

        # Dimension DataFrames (populated during build)
        self.dim_date: Optional[pd.DataFrame] = None
        self.dim_time: Optional[pd.DataFrame] = None
        self.dim_workspace: Optional[pd.DataFrame] = None
        self.dim_item: Optional[pd.DataFrame] = None
        self.dim_user: Optional[pd.DataFrame] = None
        self.dim_activity_type: Optional[pd.DataFrame] = None
        self.dim_status: Optional[pd.DataFrame] = None

        self.fact_activity: Optional[pd.DataFrame] = None
        self.fact_daily_metrics: Optional[pd.DataFrame] = None

    def _load_workspace_lookup(self) -> Dict[str, Dict[str, Any]]:
        """
        Load workspace ID to full workspace data mapping from parquet files.
        
        Searches for workspace parquet files in common locations:
        1. Explicit path provided to constructor
        2. exports/monitor_hub_analysis/parquet/workspaces_*.parquet
        3. notebooks/monitor_hub_analysis/parquet/workspaces_*.parquet
        
        Returns:
            Dict mapping workspace_id to full workspace data dict
            (includes displayName, capacityId, type, state, etc.)
        """
        if self._workspace_lookup is not None:
            return self._workspace_lookup

        self._workspace_lookup = {}

        # Locations to search for workspace files
        search_paths = []

        if self.workspace_lookup_path:
            search_paths.append(Path(self.workspace_lookup_path))

        # Common export locations
        base_paths = [
            resolve_path("exports/monitor_hub_analysis/parquet"),
            resolve_path("notebooks/monitor_hub_analysis/parquet"),
        ]

        for base_path in base_paths:
            if base_path.exists():
                # Find the most recent workspaces file
                ws_files = sorted(base_path.glob("workspaces_*.parquet"), reverse=True)
                if ws_files:
                    search_paths.append(ws_files[0])

        # Load from first available file
        for ws_path in search_paths:
            if ws_path.exists():
                try:
                    df = pd.read_parquet(ws_path)
                    # Handle different column name conventions
                    id_col = 'id' if 'id' in df.columns else 'workspace_id'

                    if id_col in df.columns:
                        # Convert DataFrame to dict of dicts (full workspace data)
                        self._workspace_lookup = df.set_index(id_col).to_dict(orient='index')

                        # Log capacity coverage
                        capacity_col = 'capacityId' if 'capacityId' in df.columns else None
                        if capacity_col:
                            with_capacity = df[capacity_col].notna().sum()
                            self.logger.info(f"Loaded {len(self._workspace_lookup)} workspaces from {ws_path} ({with_capacity} with capacityId)")
                        else:
                            self.logger.info(f"Loaded {len(self._workspace_lookup)} workspaces from {ws_path} (no capacityId column)")
                        return self._workspace_lookup
                except Exception as e:
                    self.logger.warning(f"Could not load workspace lookup from {ws_path}: {e}")

        self.logger.warning("No workspace lookup file found - workspace data will be extracted from activities")
        return self._workspace_lookup

    def _enrich_activities_with_workspace_names(
        self,
        activities: List[Dict[str, Any]]
    ) -> List[Dict[str, Any]]:
        """
        Enrich activity records with workspace_name and capacityId from lookup.
        
        Args:
            activities: List of activity dictionaries
            
        Returns:
            List of enriched activity dictionaries with workspace_name and capacity_id populated
        """
        workspace_lookup = self._load_workspace_lookup()

        if not workspace_lookup:
            self.logger.warning("No workspace lookup available - workspace data will be extracted from activities")
            return activities

        enriched_count = 0
        capacity_enriched = 0
        for activity in activities:
            ws_id = activity.get("workspace_id")
            if ws_id and ws_id in workspace_lookup:
                ws_data = workspace_lookup[ws_id]
                # ws_data is now a dict with full workspace info
                name_col = 'displayName' if 'displayName' in ws_data else 'workspace_name'
                activity["workspace_name"] = ws_data.get(name_col, activity.get("workspace_name", "Unknown"))

                # Enrich with capacityId if available
                if ws_data.get("capacityId"):
                    activity["capacity_id"] = ws_data["capacityId"]
                    capacity_enriched += 1

                enriched_count += 1
            elif not activity.get("workspace_name"):
                activity["workspace_name"] = "Unknown"

        self.logger.info(f"Enriched {enriched_count} of {len(activities)} activities with workspace names ({capacity_enriched} with capacityId)")
        return activities

    def _load_existing_dimension(self, name: str) -> Optional[pd.DataFrame]:
        """Load existing dimension from Parquet if available."""
        dim_path = self.output_directory / f"{name}.parquet"
        if dim_path.exists():
            try:
                return pd.read_parquet(dim_path)
            except Exception as e:
                self.logger.warning(f"Could not load existing {name}: {e}")
        return None

    def _save_dataframe(self, df: pd.DataFrame, name: str) -> Path:
        """Save DataFrame to Parquet with Spark-compatible timestamp format.
        
        Uses microsecond precision (us) instead of nanosecond to ensure
        compatibility with Apache Spark in Microsoft Fabric.
        
        Also enforces Int64 (nullable integer) type for surrogate key columns
        to ensure Direct Lake relationship compatibility in Power BI.
        """
        path = self.output_directory / f"{name}.parquet"

        # Enforce Int64 type for surrogate key columns (prevents Double conversion)
        # This is critical for Direct Lake relationships in Power BI/Fabric
        sk_columns = [col for col in df.columns if col.endswith('_sk')]
        for col in sk_columns:
            if col in df.columns:
                # Convert to Int64 (nullable integer) - handles NaN values properly
                df[col] = df[col].astype('Int64')

        # Use microsecond precision for Spark compatibility
        # Spark doesn't support TIMESTAMP(NANOS,true) format
        df.to_parquet(
            path,
            index=False,
            coerce_timestamps='us',  # Microsecond precision (Spark compatible)
            allow_truncated_timestamps=True  # Allow truncation from ns to us
        )
        self.logger.info(f"Saved {name} with {len(df)} records to {path}")
        return path

    def build_complete_schema(
        self,
        activities: List[Dict[str, Any]],
        lineage_data: Optional[List[Dict[str, Any]]] = None,
        incremental: bool = True,
        date_range_days: int = 365
    ) -> Dict[str, Any]:
        """
        Build the complete star schema from raw activity data.
        
        Args:
            activities: List of activity dictionaries from Monitor Hub
            lineage_data: Optional lineage data for enriching items
            incremental: If True, only process new records since last run
            date_range_days: Number of days for date dimension (default 1 year)
        
        Returns:
            Summary of the build operation
        """
        import time
        start_time = time.time()

        self.logger.info("="*60)
        self.logger.info("Starting Star Schema Build")
        self.logger.info(f"Mode: {'Incremental' if incremental else 'Full Refresh'}")
        self.logger.info(f"Input activities: {len(activities)}")
        self.logger.info("="*60)

        # Enrich activities with workspace names from lookup
        self.logger.info("Pre-processing: Enriching activities with workspace names...")
        activities = self._enrich_activities_with_workspace_names(activities)

        results = {
            "status": "success",
            "build_mode": "incremental" if incremental else "full",
            "dimensions_built": {},
            "facts_built": {},
            "output_directory": str(self.output_directory),
            "errors": []
        }

        try:
            # 1. Build Reference Dimensions (Type 0 - always full refresh)
            self.logger.info("Step 1: Building reference dimensions...")

            self.dim_date = self.date_builder.build(
                start_date=datetime.now() - timedelta(days=date_range_days),
                end_date=datetime.now() + timedelta(days=90)
            )
            self._save_dataframe(self.dim_date, "dim_date")
            results["dimensions_built"]["dim_date"] = len(self.dim_date)

            self.dim_time = self.time_builder.build()
            self._save_dataframe(self.dim_time, "dim_time")
            results["dimensions_built"]["dim_time"] = len(self.dim_time)

            self.dim_activity_type = self.activity_type_builder.build()
            self._save_dataframe(self.dim_activity_type, "dim_activity_type")
            results["dimensions_built"]["dim_activity_type"] = len(self.dim_activity_type)

            self.dim_status = self.status_builder.build()
            self._save_dataframe(self.dim_status, "dim_status")
            results["dimensions_built"]["dim_status"] = len(self.dim_status)

            # 2. Build Slowly Changing Dimensions (Type 2)
            self.logger.info("Step 2: Building slowly changing dimensions...")

            existing_workspace = self._load_existing_dimension("dim_workspace") if incremental else None
            self.dim_workspace, new_workspaces = self.workspace_builder.build_from_activities(
                activities, existing_workspace
            )
            self._save_dataframe(self.dim_workspace, "dim_workspace")
            results["dimensions_built"]["dim_workspace"] = len(self.dim_workspace)
            results["dimensions_built"]["dim_workspace_new"] = len(new_workspaces)

            existing_item = self._load_existing_dimension("dim_item") if incremental else None
            self.dim_item, new_items = self.item_builder.build_from_activities(
                activities, lineage_data, existing_item
            )
            self._save_dataframe(self.dim_item, "dim_item")
            results["dimensions_built"]["dim_item"] = len(self.dim_item)
            results["dimensions_built"]["dim_item_new"] = len(new_items)

            existing_user = self._load_existing_dimension("dim_user") if incremental else None
            self.dim_user, new_users = self.user_builder.build_from_activities(
                activities, existing_user
            )
            self._save_dataframe(self.dim_user, "dim_user")
            results["dimensions_built"]["dim_user"] = len(self.dim_user)
            results["dimensions_built"]["dim_user_new"] = len(new_users)

            # 3. Build Fact Table
            self.logger.info("Step 3: Building fact activity table...")

            high_water_mark = self.tracker.get_high_water_mark("fact_activity") if incremental else None

            fact_builder = FactActivityBuilder(
                dim_workspace=self.dim_workspace,
                dim_item=self.dim_item,
                dim_user=self.dim_user,
                dim_activity_type=self.dim_activity_type,
                dim_status=self.dim_status,
                logger=self.logger
            )

            new_facts = fact_builder.build_from_activities(activities, high_water_mark)

            # Append to existing facts if incremental
            existing_facts = self._load_existing_dimension("fact_activity") if incremental else None
            if existing_facts is not None and not existing_facts.empty and not new_facts.empty:
                self.fact_activity = pd.concat([existing_facts, new_facts], ignore_index=True)
            elif not new_facts.empty:
                self.fact_activity = new_facts
            elif existing_facts is not None and not existing_facts.empty:
                self.fact_activity = existing_facts
            else:
                self.fact_activity = pd.DataFrame()

            if not self.fact_activity.empty:
                self._save_dataframe(self.fact_activity, "fact_activity")
                results["facts_built"]["fact_activity"] = len(self.fact_activity)
                results["facts_built"]["fact_activity_new"] = len(new_facts)

                # Update high water mark
                if not new_facts.empty and "extracted_at" in new_facts.columns:
                    new_hwm = new_facts["extracted_at"].max()
                    self.tracker.set_high_water_mark("fact_activity", new_hwm)

            # 4. Build Aggregate Facts
            self.logger.info("Step 4: Building aggregate metrics...")

            if not self.fact_activity.empty:
                daily_builder = DailyMetricsBuilder()
                self.fact_daily_metrics = daily_builder.build_from_fact(self.fact_activity)

                if not self.fact_daily_metrics.empty:
                    self._save_dataframe(self.fact_daily_metrics, "fact_daily_metrics")
                    results["facts_built"]["fact_daily_metrics"] = len(self.fact_daily_metrics)

            # 5. Record load operation
            elapsed = time.time() - start_time
            self.tracker.record_load(
                "star_schema_build",
                records_processed=len(activities),
                duration_seconds=elapsed
            )

            results["duration_seconds"] = round(elapsed, 2)

            self.logger.info("="*60)
            self.logger.info("Star Schema Build Complete")
            self.logger.info(f"Duration: {elapsed:.2f} seconds")
            self.logger.info(f"Output: {self.output_directory}")
            self.logger.info("="*60)

        except Exception as e:
            self.logger.error(f"Star Schema build failed: {e}")
            results["status"] = "error"
            results["errors"].append(str(e))

        return results

    def get_ddl(self, dialect: str = "delta") -> str:
        """
        Get the DDL statements for creating the star schema tables.
        
        Args:
            dialect: SQL dialect (delta, spark, tsql)
        """
        if dialect == "delta":
            return "\n\n".join(ALL_STAR_SCHEMA_DDLS.values())
        # Add other dialects as needed
        return "\n\n".join(ALL_STAR_SCHEMA_DDLS.values())

    def describe_schema(self) -> str:
        """Return a text description of the star schema."""
        desc = """
FABRIC MONITORING STAR SCHEMA
=============================

This star schema is designed for analytical queries on Microsoft Fabric monitoring data.

FACT TABLES:
------------
1. fact_activity (Grain: One row per activity event)
   - Primary fact table with all activity measures
   - Partitioned by activity_date for query performance
   - Contains duration metrics, success/failure flags

2. fact_daily_metrics (Grain: One row per workspace per day)
   - Pre-aggregated daily metrics for dashboards
   - Improves performance for common roll-up queries

DIMENSION TABLES:
-----------------
1. dim_date - Calendar dimension with fiscal year support
2. dim_time - Time-of-day dimension with business hours flag
3. dim_workspace - Workspace details with SCD Type 2
4. dim_item - Fabric items with lineage data
5. dim_user - Users with risk classification
6. dim_activity_type - Activity type reference data
7. dim_status - Status code reference data

KEY RELATIONSHIPS:
------------------
fact_activity[date_sk] → dim_date[date_sk]
fact_activity[time_sk] → dim_time[time_sk]
fact_activity[workspace_sk] → dim_workspace[workspace_sk]
fact_activity[item_sk] → dim_item[item_sk]
fact_activity[user_sk] → dim_user[user_sk]
fact_activity[activity_type_sk] → dim_activity_type[activity_type_sk]
fact_activity[status_sk] → dim_status[status_sk]

SAMPLE QUERIES:
---------------
-- Failure rate by workspace over last 7 days
SELECT 
    w.workspace_name,
    SUM(f.is_failed) as failures,
    SUM(f.record_count) as total,
    ROUND(SUM(f.is_failed) * 100.0 / SUM(f.record_count), 2) as failure_rate
FROM fact_activity f
JOIN dim_workspace w ON f.workspace_sk = w.workspace_sk
JOIN dim_date d ON f.date_sk = d.date_sk
WHERE d.full_date >= DATEADD(day, -7, CURRENT_DATE)
GROUP BY w.workspace_name
ORDER BY failure_rate DESC;

-- Top 10 longest running items
SELECT 
    i.item_name,
    i.item_type,
    w.workspace_name,
    SUM(f.duration_hours) as total_hours,
    AVG(f.duration_minutes) as avg_minutes
FROM fact_activity f
JOIN dim_item i ON f.item_sk = i.item_sk
JOIN dim_workspace w ON f.workspace_sk = w.workspace_sk
GROUP BY i.item_name, i.item_type, w.workspace_name
ORDER BY total_hours DESC
LIMIT 10;

-- Activity by time of day (business vs off-hours)
SELECT 
    t.time_period,
    t.is_business_hours,
    COUNT(*) as activity_count,
    SUM(f.is_failed) as failures
FROM fact_activity f
JOIN dim_time t ON f.time_sk = t.time_sk
GROUP BY t.time_period, t.is_business_hours
ORDER BY activity_count DESC;
"""
        return desc


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def build_star_schema_from_parquet(
    activities_parquet_path: str,
    output_directory: Optional[str] = None,
    incremental: bool = True
) -> Dict[str, Any]:
    """
    Convenience function to build star schema from existing Parquet exports.
    
    Args:
        activities_parquet_path: Path to activities Parquet file
        output_directory: Output directory for star schema tables
        incremental: Whether to perform incremental load
    
    Returns:
        Build results summary
    """
    # Load activities from Parquet
    activities_df = pd.read_parquet(activities_parquet_path)
    activities = activities_df.to_dict(orient="records")

    # Build schema
    builder = StarSchemaBuilder(output_directory=output_directory)
    return builder.build_complete_schema(activities, incremental=incremental)


def build_star_schema_from_pipeline_output(
    pipeline_output_dir: str,
    output_directory: Optional[str] = None,
    incremental: bool = True
) -> Dict[str, Any]:
    """
    Build star schema from Monitor Hub pipeline output directory.
    
    Priority: Loads from parquet/activities_*.parquet (28 columns, full data including
    workspace_name, failure_reason, user details) first, falls back to activities_master CSV.
    
    Args:
        pipeline_output_dir: Directory containing pipeline outputs
        output_directory: Output directory for star schema tables
        incremental: Whether to perform incremental load
    """
    pipeline_path = Path(pipeline_output_dir)

    # Priority 1: Look for parquet files (28 columns - complete data)
    parquet_path = pipeline_path / "parquet"
    parquet_files = sorted(parquet_path.glob("activities_*.parquet"), reverse=True) if parquet_path.exists() else []

    if parquet_files:
        logger.info(f"Loading activities from parquet: {parquet_files[0]}")
        activities_df = pd.read_parquet(parquet_files[0])
        logger.info(f"Loaded {len(activities_df):,} records with {len(activities_df.columns)} columns")
    else:
        # Fallback: CSV file (19 columns - limited data)
        activities_files = sorted(pipeline_path.glob("activities_master_*.csv"), reverse=True)
        if not activities_files:
            raise FileNotFoundError(
                f"No activities data found in {pipeline_path}. "
                f"Expected parquet/activities_*.parquet or activities_master_*.csv"
            )
        logger.warning(f"Parquet not found, falling back to CSV: {activities_files[0]}")
        activities_df = pd.read_csv(activities_files[0])
    activities = activities_df.to_dict(orient="records")

    # Optionally load lineage data
    lineage_path = pipeline_path.parent / "lineage"
    lineage_data = None
    if lineage_path.exists():
        lineage_files = sorted(lineage_path.glob("mirrored_lineage_*.csv"), reverse=True)
        if lineage_files:
            lineage_df = pd.read_csv(lineage_files[0])
            lineage_data = lineage_df.to_dict(orient="records")

    # Build schema
    builder = StarSchemaBuilder(output_directory=output_directory)
    return builder.build_complete_schema(
        activities,
        lineage_data=lineage_data,
        incremental=incremental
    )


# =============================================================================
# CLI ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Build Star Schema for Fabric Monitoring")
    parser.add_argument(
        "--input-dir",
        default="exports/monitor_hub_analysis",
        help="Path to Monitor Hub pipeline output directory"
    )
    parser.add_argument(
        "--output-dir",
        default="exports/star_schema",
        help="Output directory for star schema tables"
    )
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Perform full refresh instead of incremental load"
    )
    parser.add_argument(
        "--ddl-only",
        action="store_true",
        help="Print DDL statements and exit"
    )

    args = parser.parse_args()

    if args.ddl_only:
        builder = StarSchemaBuilder()
        print(builder.get_ddl())
        print("\n" + builder.describe_schema())
    else:
        results = build_star_schema_from_pipeline_output(
            pipeline_output_dir=args.input_dir,
            output_directory=args.output_dir,
            incremental=not args.full_refresh
        )

        print("\nStar Schema Build Results:")
        print(json.dumps(results, indent=2, default=str))
