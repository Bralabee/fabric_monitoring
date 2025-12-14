import sys
from pathlib import Path

import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from usf_fabric_monitoring.core.monitor_hub_reporter_clean import MonitorHubCSVReporter


def test_compute_report(tmp_path: Path):
    reporter = MonitorHubCSVReporter(str(tmp_path))
    
    activities = [
        {
            "activity_id": "1",
            "item_type": "SparkJob",
            "item_name": "Daily Processing",
            "submitted_by": "user@example.com",
            "status": "Succeeded",
            "duration_seconds": 120,
            "start_time": "2025-11-20T10:00:00Z"
        },
        {
            "activity_id": "2",
            "item_type": "SparkJob",
            "item_name": "Daily Processing",
            "submitted_by": "user@example.com",
            "status": "Failed",
            "duration_seconds": 30,
            "start_time": "2025-11-20T11:00:00Z"
        },
        {
            "activity_id": "3",
            "item_type": "Notebook",
            "item_name": "Analysis NB",
            "submitted_by": "analyst@example.com",
            "status": "Succeeded",
            "duration_seconds": 300,
            "start_time": "2025-11-20T12:00:00Z"
        },
        {
            "activity_id": "4",
            "item_type": "Report", # Should be ignored
            "item_name": "Sales Report",
            "submitted_by": "viewer@example.com",
            "status": "Succeeded",
            "duration_seconds": 10,
            "start_time": "2025-11-20T13:00:00Z"
        }
    ]
    
    filepath = reporter._generate_compute_analysis_report(activities)
    assert filepath
    df = pd.read_csv(filepath)

    row1 = df[df["Item Name"] == "Daily Processing"].iloc[0]
    assert row1["Total Runs"] == 2
    assert row1["Failed Runs"] == 1
    assert row1["Failure Rate %"] == 50.0

    row2 = df[df["Item Name"] == "Analysis NB"].iloc[0]
    assert row2["Total Runs"] == 1
    assert row2["Failed Runs"] == 0
