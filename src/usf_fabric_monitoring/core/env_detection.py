"""
Environment Detection Utilities for Microsoft Fabric Monitoring.

This module provides environment-aware utilities for detecting and adapting to
different deployment contexts: LOCAL development, FABRIC_NOTEBOOK, FABRIC_PIPELINE,
and AZURE_DEVOPS.

Usage:
    from usf_fabric_monitoring.core.env_detection import (
        detect_environment,
        is_fabric_environment,
        get_default_output_path
    )
    
    env = detect_environment()
    if is_fabric_environment():
        # Use Fabric-specific paths
        output_dir = get_default_output_path()
"""

from __future__ import annotations

import os
import sys
import logging
from enum import Enum
from pathlib import Path

logger = logging.getLogger(__name__)


class Environment(str, Enum):
    """Deployment environment types."""
    LOCAL = "LOCAL"
    FABRIC_NOTEBOOK = "FABRIC_NOTEBOOK"
    FABRIC_PIPELINE = "FABRIC_PIPELINE"
    AZURE_DEVOPS = "AZURE_DEVOPS"
    UNKNOWN = "UNKNOWN"


def detect_environment() -> Environment:
    """
    Detect the current execution environment.
    
    Detection order:
    1. Azure DevOps - Check for TF_BUILD environment variable
    2. Fabric Notebook - Check for notebookutils module availability
    3. Fabric Pipeline - Check for Fabric-specific env vars
    4. Local - Default fallback
    
    Returns:
        Environment enum value
        
    Example:
        >>> env = detect_environment()
        >>> if env == Environment.FABRIC_NOTEBOOK:
        ...     # Use Fabric notebook-specific behavior
    """
    # Check Azure DevOps
    if os.environ.get("TF_BUILD") == "True":
        return Environment.AZURE_DEVOPS

    # Check for Fabric environment indicators
    if _is_fabric_context():
        # Try to distinguish between notebook and pipeline
        if _has_notebookutils():
            return Environment.FABRIC_NOTEBOOK
        if os.environ.get("FABRIC_PIPELINE_ID"):
            return Environment.FABRIC_PIPELINE
        # Generic Fabric environment
        return Environment.FABRIC_NOTEBOOK

    # Default to local
    return Environment.LOCAL


def _is_fabric_context() -> bool:
    """
    Check if running in a Microsoft Fabric context.
    
    Uses multiple indicators to robustly detect Fabric environment.
    """
    # Check common Fabric environment indicators
    fabric_indicators = [
        # Lakehouse mount path exists
        Path("/lakehouse/default").exists(),
        # Fabric workspace ID is set
        bool(os.environ.get("FABRIC_WORKSPACE_ID")),
        # Trident/Synapse indicators
        bool(os.environ.get("TRIDENT_LOG_PATH")),
        # notebookutils is available
        _has_notebookutils(),
        # mssparkutils is available
        _has_mssparkutils(),
    ]

    return any(fabric_indicators)


def _has_notebookutils() -> bool:
    """Check if notebookutils module is available (Fabric notebook)."""
    try:
        import notebookutils  # noqa: F401
        return True
    except ImportError:
        return False


def _has_mssparkutils() -> bool:
    """Check if mssparkutils is available (Synapse/Fabric Spark)."""
    try:
        from pyspark.dbutils import DBUtils  # noqa: F401
        return True
    except ImportError:
        pass

    # Check for mssparkutils global (injected in Fabric notebooks)
    if 'mssparkutils' in dir(__builtins__) if isinstance(__builtins__, dict) else hasattr(__builtins__, 'mssparkutils'):
        return True

    return False


def is_fabric_environment() -> bool:
    """
    Quick check if running in any Fabric environment.
    
    Returns:
        True if running in Fabric (notebook or pipeline)
    """
    env = detect_environment()
    return env in (Environment.FABRIC_NOTEBOOK, Environment.FABRIC_PIPELINE)


def is_local_environment() -> bool:
    """
    Quick check if running locally.
    
    Returns:
        True if running in local development environment
    """
    return detect_environment() == Environment.LOCAL


def get_default_output_path(subdir: str = "exports") -> Path:
    """
    Get the default output path based on environment.
    
    In Fabric: Uses /lakehouse/default/Files/<subdir>
    Locally: Uses project root / <subdir>
    
    Args:
        subdir: Subdirectory name within the output location
        
    Returns:
        Path object for output directory
    """
    if is_fabric_environment():
        # Fabric Lakehouse path
        lakehouse_base = Path("/lakehouse/default/Files")
        if lakehouse_base.exists():
            return lakehouse_base / subdir

    # Local development - find project root
    return _get_project_root() / subdir


def get_config_path() -> Path:
    """
    Get the configuration directory path based on environment.
    
    Returns:
        Path to config directory
    """
    if is_fabric_environment():
        # In Fabric, config might be in Files
        lakehouse_config = Path("/lakehouse/default/Files/config")
        if lakehouse_config.exists():
            return lakehouse_config

    # Local development
    return _get_project_root() / "config"


def _get_project_root() -> Path:
    """Get project root directory."""
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if (parent / "pyproject.toml").exists():
            return parent
        if (parent / "config").is_dir() and (parent / "src").is_dir():
            return parent
    # Fallback
    return current.parent.parent.parent.parent


def convert_to_spark_path(local_path: str) -> str:
    """
    Convert a local mount path to Spark-compatible relative path.
    
    In Fabric, glob.glob returns /lakehouse/default/Files/... paths,
    but Spark needs relative paths like Files/...
    
    Args:
        local_path: Local filesystem path
        
    Returns:
        Spark-compatible path
        
    Example:
        >>> convert_to_spark_path("/lakehouse/default/Files/data/file.csv")
        "Files/data/file.csv"
    """
    patterns = [
        "/lakehouse/default/",
        "/lakehouse/default",
    ]

    result = local_path
    for pattern in patterns:
        if result.startswith(pattern):
            result = result[len(pattern):]
            break

    return result


def get_environment_info() -> dict:
    """
    Get detailed information about the current environment.
    
    Returns:
        Dictionary with environment details
    """
    env = detect_environment()

    return {
        "environment": env.value,
        "is_fabric": is_fabric_environment(),
        "is_local": is_local_environment(),
        "python_version": f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}",
        "has_notebookutils": _has_notebookutils(),
        "has_mssparkutils": _has_mssparkutils(),
        "lakehouse_available": Path("/lakehouse/default").exists(),
        "project_root": str(_get_project_root()),
        "default_output": str(get_default_output_path()),
        "config_path": str(get_config_path()),
    }
