import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from usf_fabric_monitoring.core.enrichment import _parse_datetime, compute_duration_seconds


def test_parse_datetime_supports_z_suffix():
    dt = _parse_datetime("2025-11-24T12:00:00Z")
    assert dt is not None
    assert dt.isoformat().startswith("2025-11-24T12:00:00")


def test_compute_duration_seconds_standard_z():
    activity = {
        "StartTime": "2025-11-24T12:00:00Z",
        "EndTime": "2025-11-24T12:00:10Z",
    }
    assert compute_duration_seconds(activity) == 10.0


def test_compute_duration_seconds_fractional_z():
    activity = {
        "StartTime": "2025-11-24T12:00:00.123Z",
        "EndTime": "2025-11-24T12:00:10.123Z",
    }
    assert compute_duration_seconds(activity) == 10.0


def test_compute_duration_seconds_without_z():
    activity = {
        "StartTime": "2025-11-24T12:00:00",
        "EndTime": "2025-11-24T12:00:10",
    }
    assert compute_duration_seconds(activity) == 10.0


def test_compute_duration_seconds_missing_end_returns_none():
    activity = {"StartTime": "2025-11-24T12:00:00Z"}
    assert compute_duration_seconds(activity) is None
