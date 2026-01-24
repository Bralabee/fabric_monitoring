"""
Tests for Smart Merge Logic in MonitorHubPipeline

The Smart Merge correlates activity events with detailed job history using
pandas merge_asof to align events by ItemId and Time (within 5 minutes).
"""
import pytest
import pandas as pd
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from pathlib import Path


class TestSmartMerge:
    """Tests for the _merge_activities method in MonitorHubPipeline."""
    
    @pytest.fixture
    def pipeline(self):
        """Create a pipeline instance with mocked dependencies."""
        with patch('usf_fabric_monitoring.core.pipeline.setup_logging'):
            with patch('usf_fabric_monitoring.core.pipeline.resolve_path', return_value=Path('/tmp/test')):
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
            {
                "activity_id": "act_001",
                "item_id": "item_001",
                "start_time": "2024-01-15T10:00:00Z",
                "end_time": "2024-01-15T10:05:00Z",
                "status": "Succeeded"
            }
        ]
        result = pipeline._merge_activities(activities, [])
        assert len(result) == 1
        assert result[0]["activity_id"] == "act_001"
    
    def test_merge_correlates_by_item_id_and_time(self, pipeline):
        """Jobs should be matched to activities by item_id and within 5 min window."""
        activities = [
            {
                "activity_id": "act_001",
                "item_id": "item_001",
                "start_time": "2024-01-15T10:00:00Z",
                "end_time": None,  # Missing end time
                "status": "Completed",
                "duration_seconds": 0
            }
        ]
        detailed_jobs = [
            {
                "id": "job_001",
                "itemId": "item_001",
                "startTimeUtc": "2024-01-15T10:01:00Z",
                "endTimeUtc": "2024-01-15T10:10:00Z",
                "status": "Succeeded",
                "failureReason": None,
                "jobType": "Refresh"
            }
        ]
        
        result = pipeline._merge_activities(activities, detailed_jobs)
        
        assert len(result) == 1
        # Job should be correlated - check that job data was merged
        assert result[0].get("job_instance_id") == "job_001"
    
    def test_merge_updates_failed_status(self, pipeline):
        """When job failed, activity status should be updated to Failed."""
        activities = [
            {
                "activity_id": "act_001",
                "item_id": "item_001",
                "start_time": "2024-01-15T10:00:00Z",
                "end_time": "2024-01-15T10:05:00Z",
                "status": "Completed",  # Activity says completed
            }
        ]
        detailed_jobs = [
            {
                "id": "job_001",
                "itemId": "item_001",
                "startTimeUtc": "2024-01-15T10:00:30Z",
                "endTimeUtc": "2024-01-15T10:05:00Z",
                "status": "Failed",  # But job actually failed
                "failureReason": "Timeout exceeded",
            }
        ]
        
        result = pipeline._merge_activities(activities, detailed_jobs)
        
        assert len(result) == 1
        assert result[0]["status"] == "Failed"
        assert result[0].get("failure_reason") == "Timeout exceeded"
    
    def test_merge_fills_missing_end_time(self, pipeline):
        """Missing end_time should be filled from job data."""
        activities = [
            {
                "activity_id": "act_001",
                "item_id": "item_001",
                "start_time": "2024-01-15T10:00:00Z",
                "end_time": None,  # Missing
                "status": "Succeeded",
                "duration_seconds": 0  # Zero because no end time
            }
        ]
        detailed_jobs = [
            {
                "id": "job_001",
                "itemId": "item_001",
                "startTimeUtc": "2024-01-15T10:00:00Z",
                "endTimeUtc": "2024-01-15T10:15:00Z",  # 15 minutes
                "status": "Succeeded",
            }
        ]
        
        result = pipeline._merge_activities(activities, detailed_jobs)
        
        assert len(result) == 1
        assert result[0].get("end_time") is not None
        # Duration should be recalculated (15 minutes = 900 seconds)
        assert result[0].get("duration_seconds") == 900.0
    
    def test_merge_does_not_correlate_outside_window(self, pipeline):
        """Jobs more than 5 minutes from activity should not be matched."""
        activities = [
            {
                "activity_id": "act_001",
                "item_id": "item_001",
                "start_time": "2024-01-15T10:00:00Z",
                "status": "Completed"
            }
        ]
        detailed_jobs = [
            {
                "id": "job_001",
                "itemId": "item_001",
                "startTimeUtc": "2024-01-15T10:10:00Z",  # 10 min later - outside 5 min window
                "endTimeUtc": "2024-01-15T10:20:00Z",
                "status": "Succeeded",
            }
        ]
        
        result = pipeline._merge_activities(activities, detailed_jobs)
        
        assert len(result) == 1
        # Job should NOT be matched (no job_instance_id)
        assert result[0].get("job_instance_id") is None
    
    def test_merge_different_items_not_matched(self, pipeline):
        """Jobs for different item_ids should not be matched."""
        activities = [
            {
                "activity_id": "act_001",
                "item_id": "item_001",
                "start_time": "2024-01-15T10:00:00Z",
                "status": "Completed"
            }
        ]
        detailed_jobs = [
            {
                "id": "job_001",
                "itemId": "item_002",  # Different item
                "startTimeUtc": "2024-01-15T10:00:00Z",
                "endTimeUtc": "2024-01-15T10:05:00Z",
                "status": "Failed",  # Should NOT update act_001's status
            }
        ]
        
        result = pipeline._merge_activities(activities, detailed_jobs)
        
        assert len(result) == 1
        assert result[0]["status"] == "Completed"  # Status unchanged
        assert result[0].get("job_instance_id") is None


class TestSmartMergeEdgeCases:
    """Edge cases and error handling for Smart Merge."""
    
    @pytest.fixture
    def pipeline(self):
        """Create a pipeline instance with mocked dependencies."""
        with patch('usf_fabric_monitoring.core.pipeline.setup_logging'):
            with patch('usf_fabric_monitoring.core.pipeline.resolve_path', return_value=Path('/tmp/test')):
                from usf_fabric_monitoring.core.pipeline import MonitorHubPipeline
                p = MonitorHubPipeline()
                p.logger = MagicMock()
                return p
    
    def test_merge_handles_null_timestamps(self, pipeline):
        """Null timestamps should not cause errors."""
        activities = [
            {
                "activity_id": "act_001",
                "item_id": "item_001",
                "start_time": None,  # Null start time
                "status": "Unknown"
            }
        ]
        detailed_jobs = [
            {
                "id": "job_001",
                "itemId": "item_001",
                "startTimeUtc": "2024-01-15T10:00:00Z",
                "status": "Succeeded",
            }
        ]
        
        result = pipeline._merge_activities(activities, detailed_jobs)
        # Should not raise, returns activities
        assert len(result) == 1
    
    def test_merge_handles_invalid_dates(self, pipeline):
        """Invalid date strings should be handled gracefully."""
        activities = [
            {
                "activity_id": "act_001",
                "item_id": "item_001",
                "start_time": "not-a-date",
                "status": "Unknown"
            }
        ]
        
        result = pipeline._merge_activities(activities, [])
        assert len(result) == 1
    
    def test_merge_multiple_activities_multiple_jobs(self, pipeline):
        """Multiple activities should each match their corresponding job."""
        activities = [
            {
                "activity_id": "act_001",
                "item_id": "item_001",
                "start_time": "2024-01-15T10:00:00Z",
                "status": "Completed"
            },
            {
                "activity_id": "act_002",
                "item_id": "item_002",
                "start_time": "2024-01-15T11:00:00Z",
                "status": "Completed"
            }
        ]
        detailed_jobs = [
            {
                "id": "job_001",
                "itemId": "item_001",
                "startTimeUtc": "2024-01-15T10:00:30Z",
                "status": "Succeeded",
            },
            {
                "id": "job_002",
                "itemId": "item_002",
                "startTimeUtc": "2024-01-15T11:00:30Z",
                "status": "Failed",
            }
        ]
        
        result = pipeline._merge_activities(activities, detailed_jobs)
        
        assert len(result) == 2
        # Each activity should match its corresponding job
        act_001 = next(r for r in result if r["activity_id"] == "act_001")
        act_002 = next(r for r in result if r["activity_id"] == "act_002")
        
        assert act_001.get("job_instance_id") == "job_001"
        assert act_001["status"] == "Completed"  # Job succeeded, keeps Completed
        
        assert act_002.get("job_instance_id") == "job_002"
        assert act_002["status"] == "Failed"  # Job failed, updates to Failed
