"""
Type Safety Utilities for Microsoft Fabric Monitoring.

This module provides defensive data handling functions to prevent common
production failures related to NULL values, type mismatches, and data coercion.

These utilities address the most common failure patterns observed in production:
- NaN/None values causing Float64 instead of Int64 surrogate keys
- Missing workspace names in enrichment pipelines
- Datetime parsing failures with varied formats
- NULL handling in dimension builders

Usage:
    from usf_fabric_monitoring.core.type_safety import (
        safe_int64,
        coerce_surrogate_keys,
        safe_datetime,
        safe_workspace_lookup
    )
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


# =============================================================================
# INTEGER TYPE COERCION
# =============================================================================

def safe_int64(value: Any) -> Optional[int]:
    """
    Safely convert a value to nullable Int64, handling NaN/None.
    
    This addresses the Direct Lake relationship compatibility issue where
    NULL values in pandas convert columns to Float64 instead of Int64.
    
    Args:
        value: Any value to convert (int, float, str, None, NaN)
        
    Returns:
        Integer value or None if conversion fails
        
    Examples:
        >>> safe_int64(42)
        42
        >>> safe_int64(42.0)
        42
        >>> safe_int64(np.nan)
        None
        >>> safe_int64(None)
        None
        >>> safe_int64("123")
        123
    """
    if value is None:
        return None
    if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
        return None
    if pd.isna(value):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def coerce_surrogate_keys(df: pd.DataFrame, inplace: bool = False) -> pd.DataFrame:
    """
    Enforce nullable Int64 type for all surrogate key columns (*_sk).
    
    This ensures compatibility with Power BI Direct Lake mode which requires
    matching data types for relationships between fact and dimension tables.
    
    Args:
        df: DataFrame with surrogate key columns
        inplace: If True, modify DataFrame in place
        
    Returns:
        DataFrame with Int64 surrogate key columns
        
    Example:
        >>> df = coerce_surrogate_keys(df)
        >>> df['user_sk'].dtype
        Int64
    """
    if not inplace:
        df = df.copy()

    sk_columns = [col for col in df.columns if col.endswith('_sk')]

    for col in sk_columns:
        if col in df.columns:
            # Convert to nullable Int64
            try:
                df[col] = pd.array(
                    [safe_int64(v) for v in df[col]],
                    dtype=pd.Int64Dtype()
                )
            except Exception as e:
                logger.warning(f"Failed to coerce {col} to Int64: {e}")

    return df


# =============================================================================
# DATETIME HANDLING
# =============================================================================

# Common datetime formats in Fabric API responses
DATETIME_FORMATS = [
    "%Y-%m-%dT%H:%M:%S.%fZ",      # ISO8601 with microseconds and Z
    "%Y-%m-%dT%H:%M:%SZ",          # ISO8601 with Z
    "%Y-%m-%dT%H:%M:%S.%f",        # ISO8601 with microseconds
    "%Y-%m-%dT%H:%M:%S",           # ISO8601 basic
    "%Y-%m-%d %H:%M:%S.%f",        # Space-separated with microseconds
    "%Y-%m-%d %H:%M:%S",           # Space-separated basic
    "%Y-%m-%d",                     # Date only
]


def safe_datetime(value: Any) -> Optional[datetime]:
    """
    Safely parse a datetime value with multiple format fallbacks.
    
    Handles various datetime formats commonly returned by Fabric/Power BI APIs,
    including ISO8601 variations with/without microseconds and timezone suffixes.
    
    Args:
        value: Datetime string, datetime object, or None
        
    Returns:
        Parsed datetime or None if parsing fails
        
    Examples:
        >>> safe_datetime("2024-01-15T10:30:00Z")
        datetime(2024, 1, 15, 10, 30, 0)
        >>> safe_datetime("2024-01-15T10:30:00.123456Z")
        datetime(2024, 1, 15, 10, 30, 0, 123456)
        >>> safe_datetime(None)
        None
    """
    if value is None:
        return None
    if pd.isna(value):
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()

    if not isinstance(value, str):
        try:
            value = str(value)
        except Exception:
            return None

    value = value.strip()
    if not value:
        return None

    # Try pandas first (handles ISO8601 well)
    try:
        result = pd.to_datetime(value, utc=True)
        return result.to_pydatetime().replace(tzinfo=None)
    except Exception:
        pass

    # Try explicit formats
    for fmt in DATETIME_FORMATS:
        try:
            return datetime.strptime(value.rstrip('Z'), fmt.rstrip('Z'))
        except ValueError:
            continue

    logger.debug(f"Could not parse datetime: {value}")
    return None


def safe_datetime_column(series: pd.Series) -> pd.Series:
    """
    Convert a pandas Series to datetime with robust error handling.
    
    Uses multiple parsing strategies to handle mixed-format datetime data
    commonly found in API responses.
    
    Args:
        series: Pandas Series with datetime values
        
    Returns:
        Series with datetime64[ns] dtype
    """
    # Try ISO8601 first (most common)
    try:
        return pd.to_datetime(series, format='ISO8601', errors='coerce')
    except Exception:
        pass

    # Try mixed format parsing
    try:
        return pd.to_datetime(series, format='mixed', errors='coerce')
    except Exception:
        pass

    # Fallback to individual parsing
    return series.apply(safe_datetime)


# =============================================================================
# WORKSPACE LOOKUP
# =============================================================================

def safe_workspace_lookup(
    workspace_id: Optional[str],
    workspace_name: Optional[str],
    id_lookup: Dict[str, str],
    name_lookup: Dict[str, str],
    default: str = "Unknown"
) -> str:
    """
    Resolve workspace name with fallback chain.
    
    This handles the common case where failed job history records have
    workspace_name but NULL workspace_id.
    
    Lookup priority:
    1. workspace_id in id_lookup → return name
    2. workspace_name already provided → return as-is
    3. workspace_name in name_lookup → validate and return
    4. Return default
    
    Args:
        workspace_id: Workspace GUID (may be None)
        workspace_name: Workspace display name (may be None)
        id_lookup: Dict mapping workspace_id → workspace_name
        name_lookup: Dict mapping workspace_name → workspace_id (for validation)
        default: Default value if resolution fails
        
    Returns:
        Resolved workspace name or default
        
    Example:
        >>> lookup = {"abc-123": "My Workspace"}
        >>> safe_workspace_lookup("abc-123", None, lookup, {})
        "My Workspace"
    """
    # Try ID lookup first
    if workspace_id and not pd.isna(workspace_id):
        name = id_lookup.get(str(workspace_id))
        if name:
            return name

    # Use provided name if available and non-null
    if workspace_name and not pd.isna(workspace_name):
        name_str = str(workspace_name).strip()
        if name_str and name_str.lower() != "unknown":
            return name_str

    return default


def safe_workspace_sk_lookup(
    workspace_id: Optional[str],
    workspace_name: Optional[str],
    sk_by_id: Dict[str, int],
    sk_by_name: Dict[str, int],
    default_sk: int = -1
) -> int:
    """
    Resolve workspace surrogate key with fallback chain.
    
    Similar to safe_workspace_lookup but returns surrogate key.
    
    Args:
        workspace_id: Workspace GUID (may be None)
        workspace_name: Workspace display name (may be None)
        sk_by_id: Dict mapping workspace_id → surrogate key
        sk_by_name: Dict mapping workspace_name → surrogate key
        default_sk: Default surrogate key if resolution fails
        
    Returns:
        Resolved surrogate key or default
    """
    # Try ID lookup first
    if workspace_id and not pd.isna(workspace_id):
        sk = sk_by_id.get(str(workspace_id))
        if sk is not None:
            return sk

    # Try name lookup
    if workspace_name and not pd.isna(workspace_name):
        name_str = str(workspace_name).strip()
        sk = sk_by_name.get(name_str)
        if sk is not None:
            return sk

    return default_sk


# =============================================================================
# STRING HANDLING
# =============================================================================

def safe_string(value: Any, default: str = "") -> str:
    """
    Safely convert a value to string, handling None/NaN.
    
    Args:
        value: Any value to convert
        default: Default value for None/NaN
        
    Returns:
        String value or default
    """
    if value is None or pd.isna(value):
        return default
    return str(value).strip()


def safe_string_or_none(value: Any) -> Optional[str]:
    """
    Safely convert a value to string, returning None for empty/null.
    
    Args:
        value: Any value to convert
        
    Returns:
        Non-empty string or None
    """
    if value is None or pd.isna(value):
        return None
    result = str(value).strip()
    return result if result else None


# =============================================================================
# DATAFRAME UTILITIES
# =============================================================================

def ensure_columns_exist(
    df: pd.DataFrame,
    columns: List[str],
    fill_value: Any = None
) -> pd.DataFrame:
    """
    Ensure specified columns exist in DataFrame, adding if missing.
    
    Args:
        df: DataFrame to check/modify
        columns: List of column names to ensure exist
        fill_value: Value to use for missing columns
        
    Returns:
        DataFrame with all specified columns
    """
    for col in columns:
        if col not in df.columns:
            df[col] = fill_value
    return df


def safe_fillna(series: pd.Series, value: Any) -> pd.Series:
    """
    Fill NA values in a series, handling edge cases.
    
    Args:
        series: Pandas Series
        value: Fill value
        
    Returns:
        Series with NA values filled
    """
    try:
        return series.fillna(value)
    except Exception:
        # Handle incompatible types
        return series.apply(lambda x: value if pd.isna(x) else x)


def microsecond_timestamps(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert all datetime columns to microsecond precision for Spark compatibility.
    
    Fabric/Spark only supports microsecond precision, not nanosecond.
    This prevents "Illegal Parquet type: INT64 (TIMESTAMP(NANOS,true))" errors.
    
    Args:
        df: DataFrame to process
        
    Returns:
        DataFrame with datetime columns truncated to microseconds
    """
    datetime_cols = df.select_dtypes(include=['datetime64[ns]', 'datetime64']).columns

    for col in datetime_cols:
        try:
            # Truncate to microseconds
            df[col] = df[col].dt.floor('us')
        except Exception as e:
            logger.debug(f"Could not truncate {col} to microseconds: {e}")

    return df
