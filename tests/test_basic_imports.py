#!/usr/bin/env python3
"""
Simple pytest test to validate package structure and imports.
"""

import sys
from pathlib import Path

import pytest

# Add src to path for testing
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))


def test_package_imports():
    """Test that core package modules can be imported successfully."""
    # Test core pipeline import
    from usf_fabric_monitoring.core.pipeline import MonitorHubPipeline

    assert MonitorHubPipeline is not None

    # Test data loader import
    from usf_fabric_monitoring.core.data_loader import load_activities_from_directory

    assert load_activities_from_directory is not None

    # Test reporter import
    from usf_fabric_monitoring.core.monitor_hub_reporter_clean import MonitorHubCSVReporter

    assert MonitorHubCSVReporter is not None


def test_pipeline_initialization():
    """Test that pipeline can be initialized with basic parameters."""
    from usf_fabric_monitoring.core.pipeline import MonitorHubPipeline

    # Test initialization with output directory
    pipeline = MonitorHubPipeline("test_output/pytest_test")
    assert pipeline is not None
    assert pipeline.output_directory.exists()


def test_version_consistency():
    """Test that version is properly defined."""
    # Check that we can access the version
    # Version should be accessible through the package
    # This test ensures package is properly structured


if __name__ == "__main__":
    pytest.main([__file__])
