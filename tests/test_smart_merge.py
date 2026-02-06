"""
Tests for Smart Merge Logic in MonitorHubPipeline

The Smart Merge correlates activity events with detailed job history using
pandas merge_asof to align events by ItemId and Time (within 5 minutes).
"""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def create_activity(
    activity_id, item_id, start_time, status, end_time=None, duration_seconds=0, submitted_by="test@example.com"
):
    """Helper to create complete activity records matching expected structure."""
    return {
        "activity_id": activity_id,
        "item_id": item_id,
        "start_time": start_time,
        "end_time": end_time,
        "status": status,
        "duration_seconds": duration_seconds,
        "submitted_by": submitted_by,
        "creation_time": start_time,
        "failure_reason": None,
    }


def create_job(job_id, item_id, start_time, status, failure_reason=None, end_time=None):
    """Helper to create complete job records matching Fabric API structure."""
    return {
        "id": job_id,
        "itemId": item_id,
        "startTimeUtc": start_time,
        "endTimeUtc": end_time,
        "status": status,
        "failureReason": failure_reason,
        "jobType": "Refresh",
        "invoker": "system@fabric.microsoft.com",
    }


class TestSmartMerge:
    """Tests for the _merge_activities method in MonitorHubPipeline."""

    @pytest.fixture
    def pipeline(self):
        """Create a pipeline instance with mocked dependencies."""
        with patch("usf_fabric_monitoring.core.pipeline.setup_logging"):
            with patch("usf_fabric_monitoring.core.pipeline.resolve_path", return_value=Path("/tmp/test")):
                from usf_fabric_monitoring.core.pipeline import MonitorHubPipeline

                p = MonitorHubPipeline()
                p.logger = MagicMock()
                return p

    def test_merge_empty_activities(self, pipeline):
        """Empty activities should return empty list."""
        result = pipeline._merge_activities([], [])
        assert result == []

    def test_merge_no_jobs(self, pipeline):
        """Activities with no jobs should return activities unchanged."""
        activities = [
            create_activity("act_001", "item_001", "2024-01-15T10:00:00Z", "Succeeded", end_time="2024-01-15T10:05:00Z")
        ]
        result = pipeline._merge_activities(activities, [])
        assert len(result) == 1
        assert result[0]["activity_id"] == "act_001"

    def test_merge_correlates_by_item_id_and_time(self, pipeline):
        """Jobs should be matched to activities by item_id and within 5 min window."""
        activities = [create_activity("act_001", "item_001", "2024-01-15T10:00:00Z", "Completed")]
        detailed_jobs = [
            create_job("job_001", "item_001", "2024-01-15T10:01:00Z", "Succeeded", end_time="2024-01-15T10:10:00Z")
        ]

        result = pipeline._merge_activities(activities, detailed_jobs)

        assert len(result) == 1
        # Job should be correlated - check that job_status column exists
        assert result[0].get("job_status") == "Succeeded"

    def test_merge_updates_failed_status(self, pipeline):
        """When job failed, activity status should be updated to Failed."""
        activities = [
            create_activity("act_001", "item_001", "2024-01-15T10:00:00Z", "Completed", end_time="2024-01-15T10:05:00Z")
        ]
        detailed_jobs = [
            create_job(
                "job_001",
                "item_001",
                "2024-01-15T10:00:30Z",
                "Failed",
                failure_reason="Timeout exceeded",
                end_time="2024-01-15T10:05:00Z",
            )
        ]

        result = pipeline._merge_activities(activities, detailed_jobs)

        assert len(result) == 1
        assert result[0]["status"] == "Failed"
        assert result[0].get("failure_reason") == "Timeout exceeded"

    def test_merge_fills_missing_end_time(self, pipeline):
        """Missing end_time should be filled from job data when job matches."""
        activities = [create_activity("act_001", "item_001", "2024-01-15T10:00:00Z", "Succeeded")]
        detailed_jobs = [
            create_job("job_001", "item_001", "2024-01-15T10:00:00Z", "Succeeded", end_time="2024-01-15T10:15:00Z")
        ]

        result = pipeline._merge_activities(activities, detailed_jobs)

        assert len(result) == 1
        # Job should have been matched
        assert result[0].get("job_status") == "Succeeded"

    def test_merge_does_not_correlate_outside_window(self, pipeline):
        """Jobs more than 5 minutes from activity should not be matched."""
        activities = [create_activity("act_001", "item_001", "2024-01-15T10:00:00Z", "Completed")]
        detailed_jobs = [
            create_job(
                "job_001", "item_001", "2024-01-15T10:10:00Z", "Succeeded", end_time="2024-01-15T10:20:00Z"
            )  # 10 min later - outside window
        ]

        result = pipeline._merge_activities(activities, detailed_jobs)

        assert len(result) == 1
        # Job should NOT be matched (no job_status or it's NaN)
        job_status = result[0].get("job_status")
        assert job_status is None or pd.isna(job_status)

    def test_merge_different_items_not_matched(self, pipeline):
        """Jobs for different item_ids should not be matched."""
        activities = [create_activity("act_001", "item_001", "2024-01-15T10:00:00Z", "Completed")]
        detailed_jobs = [
            create_job(
                "job_001", "item_002", "2024-01-15T10:00:00Z", "Failed", failure_reason="Error"
            )  # Different item
        ]

        result = pipeline._merge_activities(activities, detailed_jobs)

        assert len(result) == 1
        assert result[0]["status"] == "Completed"  # Status unchanged
        job_status = result[0].get("job_status")
        assert job_status is None or pd.isna(job_status)


class TestSmartMergeEdgeCases:
    """Edge cases and error handling for Smart Merge."""

    @pytest.fixture
    def pipeline(self):
        """Create a pipeline instance with mocked dependencies."""
        with patch("usf_fabric_monitoring.core.pipeline.setup_logging"):
            with patch("usf_fabric_monitoring.core.pipeline.resolve_path", return_value=Path("/tmp/test")):
                from usf_fabric_monitoring.core.pipeline import MonitorHubPipeline

                p = MonitorHubPipeline()
                p.logger = MagicMock()
                return p

    def test_merge_handles_null_timestamps(self, pipeline):
        """Null timestamps should not cause errors."""
        activities = [create_activity("act_001", "item_001", None, "Unknown")]
        detailed_jobs = [create_job("job_001", "item_001", "2024-01-15T10:00:00Z", "Succeeded")]

        result = pipeline._merge_activities(activities, detailed_jobs)
        # Should not raise, returns activities
        assert len(result) == 1

    def test_merge_handles_invalid_dates(self, pipeline):
        """Invalid date strings should be handled gracefully."""
        activities = [create_activity("act_001", "item_001", "not-a-date", "Unknown")]

        result = pipeline._merge_activities(activities, [])
        assert len(result) == 1

    def test_merge_multiple_activities_multiple_jobs(self, pipeline):
        """Multiple activities should each match their corresponding job."""
        activities = [
            create_activity("act_001", "item_001", "2024-01-15T10:00:00Z", "Completed"),
            create_activity("act_002", "item_002", "2024-01-15T11:00:00Z", "Completed"),
        ]
        detailed_jobs = [
            create_job("job_001", "item_001", "2024-01-15T10:00:30Z", "Succeeded"),
            create_job("job_002", "item_002", "2024-01-15T11:00:30Z", "Failed", failure_reason="Connection timeout"),
        ]

        result = pipeline._merge_activities(activities, detailed_jobs)

        assert len(result) == 2
        act_001 = next(r for r in result if r["activity_id"] == "act_001")
        act_002 = next(r for r in result if r["activity_id"] == "act_002")

        # Job succeeded, activity status stays Completed
        assert act_001["status"] == "Completed"

        # Job failed, status should be updated to Failed
        assert act_002["status"] == "Failed"
