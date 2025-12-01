
import sys
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from usf_fabric_monitoring.core.enrichment import compute_duration_seconds, _parse_datetime

def test_duration_parsing():
    # Test case 1: Standard ISO with Z
    act1 = {
        "StartTime": "2025-11-24T12:00:00Z",
        "EndTime": "2025-11-24T12:00:10Z"
    }
    dur1 = compute_duration_seconds(act1)
    print(f"Test 1 (Standard Z): {dur1} (Expected 10.0)")

    # Test case 2: ISO with fractional seconds and Z
    act2 = {
        "StartTime": "2025-11-24T12:00:00.123Z",
        "EndTime": "2025-11-24T12:00:10.123Z"
    }
    dur2 = compute_duration_seconds(act2)
    print(f"Test 2 (Fractional Z): {dur2} (Expected 10.0)")
    
    # Test case 3: ISO without Z (common in some APIs)
    act3 = {
        "StartTime": "2025-11-24T12:00:00",
        "EndTime": "2025-11-24T12:00:10"
    }
    dur3 = compute_duration_seconds(act3)
    print(f"Test 3 (No Z): {dur3} (Expected 10.0)")

    # Test case 4: Missing EndTime (Should fail gracefully or return None)
    act4 = {
        "StartTime": "2025-11-24T12:00:00Z"
    }
    dur4 = compute_duration_seconds(act4)
    print(f"Test 4 (Missing End): {dur4} (Expected None)")

if __name__ == "__main__":
    test_duration_parsing()
