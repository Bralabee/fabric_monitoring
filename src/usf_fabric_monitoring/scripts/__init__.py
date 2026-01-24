"""
USF Fabric Monitoring - CLI Scripts Package

This module contains command-line interface entry points for the monitoring system.
Each script provides a standalone CLI tool that can be invoked via:
- Direct execution: python -m usf_fabric_monitoring.scripts.<script_name>
- Entry points: usf-<command> (after pip install)
"""

from . import (
    monitor_hub_pipeline,
    enforce_workspace_access,
    validate_config,
    build_star_schema,
    extract_lineage,
)

__all__ = [
    "monitor_hub_pipeline",
    "enforce_workspace_access",
    "validate_config",
    "build_star_schema",
    "extract_lineage",
]
