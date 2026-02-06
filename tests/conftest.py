"""
Pytest configuration and fixtures for test suite.

This module provides shared fixtures used across multiple test files.
"""

import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import pytest

# Add the project root and scripts directory to the Python path
_project_root = Path(__file__).parent.parent
sys.path.insert(0, str(_project_root))
sys.path.insert(0, str(_project_root / "scripts"))


# =============================================================================
# SAMPLE DATA FIXTURES
# =============================================================================


@pytest.fixture
def sample_activities() -> list[dict[str, Any]]:
    """Sample activity records for testing Smart Merge and pipelines."""
    base_time = datetime(2024, 1, 15, 10, 0, 0)
    return [
        {
            "activity_id": "act_001",
            "item_id": "item_001",
            "start_time": base_time.isoformat() + "Z",
            "end_time": (base_time + timedelta(minutes=5)).isoformat() + "Z",
            "status": "Succeeded",
            "duration_seconds": 300,
            "submitted_by": "user1@example.com",
            "workspace_id": "ws_001",
            "workspace_name": "Dev Workspace",
            "activity_type": "ReadArtifact",
        },
        {
            "activity_id": "act_002",
            "item_id": "item_002",
            "start_time": (base_time + timedelta(hours=1)).isoformat() + "Z",
            "end_time": (base_time + timedelta(hours=1, minutes=10)).isoformat() + "Z",
            "status": "Succeeded",
            "duration_seconds": 600,
            "submitted_by": "user2@example.com",
            "workspace_id": "ws_002",
            "workspace_name": "Test Workspace",
            "activity_type": "RunArtifact",
        },
        {
            "activity_id": "act_003",
            "item_id": "item_001",
            "start_time": (base_time + timedelta(hours=2)).isoformat() + "Z",
            "end_time": None,  # Missing end time - common edge case
            "status": "Failed",
            "duration_seconds": None,
            "submitted_by": "user1@example.com",
            "workspace_id": "ws_001",
            "workspace_name": "Dev Workspace",
            "activity_type": "Pipeline",
            "failure_reason": "Connection timeout",
        },
    ]


@pytest.fixture
def sample_jobs() -> list[dict[str, Any]]:
    """Sample job history records for testing Smart Merge."""
    base_time = datetime(2024, 1, 15, 10, 0, 30)  # 30 seconds after activities
    return [
        {
            "id": "job_001",
            "itemId": "item_001",
            "startTimeUtc": base_time.isoformat() + "Z",
            "endTimeUtc": (base_time + timedelta(minutes=5)).isoformat() + "Z",
            "status": "Succeeded",
            "failureReason": None,
            "jobType": "Refresh",
        },
        {
            "id": "job_002",
            "itemId": "item_002",
            "startTimeUtc": (base_time + timedelta(hours=1)).isoformat() + "Z",
            "endTimeUtc": (base_time + timedelta(hours=1, minutes=8)).isoformat() + "Z",
            "status": "Failed",
            "failureReason": "Insufficient permissions",
            "jobType": "Pipeline",
        },
    ]


@pytest.fixture
def sample_activities_df(sample_activities) -> pd.DataFrame:
    """Sample activities as a pandas DataFrame."""
    return pd.DataFrame(sample_activities)


@pytest.fixture
def sample_workspaces() -> dict[str, str]:
    """Sample workspace ID to name mapping."""
    return {
        "ws_001": "Dev Workspace",
        "ws_002": "Test Workspace",
        "ws_003": "UAT Workspace",
        "ws_004": "Production Workspace",
    }


# =============================================================================
# TEMPORARY CONFIG FIXTURES
# =============================================================================


@pytest.fixture
def temp_config_dir(tmp_path) -> Path:
    """Create a temporary config directory with valid config files."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()

    # Create valid inference_rules.json
    inference_rules = {
        "domains": {"HR": ["hr", "human"], "Finance": ["finance", "budget"]},
        "locations": {"EMEA": ["emea"], "Americas": ["us", "usa"]},
    }
    (config_dir / "inference_rules.json").write_text(json.dumps(inference_rules))

    # Create valid workspace_access_targets.json
    targets = {
        "description": "Test targets",
        "groups": [
            {"displayName": "Test Admin Group", "objectId": "12345678-1234-1234-1234-123456789012", "role": "Admin"}
        ],
    }
    (config_dir / "workspace_access_targets.json").write_text(json.dumps(targets))

    # Create valid workspace_access_suppressions.json
    suppressions = {
        "description": "Test suppressions",
        "workspaceIds": ["00000000-0000-0000-0000-000000000000"],
        "workspaceNames": ["Test Sandbox"],
    }
    (config_dir / "workspace_access_suppressions.json").write_text(json.dumps(suppressions))

    return config_dir


@pytest.fixture
def invalid_config_file(tmp_path) -> Path:
    """Create an invalid config file for error testing."""
    filepath = tmp_path / "workspace_access_targets.json"
    filepath.write_text(json.dumps({"invalid": "structure"}))
    return filepath


# =============================================================================
# MOCK API RESPONSE FIXTURES
# =============================================================================


@pytest.fixture
def mock_workspace_response() -> dict[str, Any]:
    """Mock response from Power BI Admin /groups endpoint."""
    return {
        "value": [
            {
                "id": "ws_001",
                "name": "Dev Workspace",
                "type": "Workspace",
                "state": "Active",
                "isOnDedicatedCapacity": True,
                "capacityId": "cap_001",
            },
            {
                "id": "ws_002",
                "name": "Test Workspace",
                "type": "Workspace",
                "state": "Active",
                "isOnDedicatedCapacity": True,
                "capacityId": "cap_001",
            },
        ]
    }


@pytest.fixture
def mock_activity_response() -> dict[str, Any]:
    """Mock response from Power BI Admin /activityevents endpoint."""
    return {
        "activityEventEntities": [
            {
                "Id": "evt_001",
                "CreationTime": "2024-01-15T10:00:00Z",
                "Activity": "ReadArtifact",
                "UserId": "user1@example.com",
                "WorkspaceId": "ws_001",
                "ItemId": "item_001",
            }
        ],
        "continuationUri": None,
        "lastResultSet": True,
    }
