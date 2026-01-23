
import sys
import os
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parents[2]))

from usf_fabric_monitoring.core.data_loader import load_activities_from_directory
from usf_fabric_monitoring.core.monitor_hub_reporter_clean import MonitorHubCSVReporter

def main():
    # Find the latest extraction directory
    base_extract_dir = Path("exports/monitor_hub_analysis/extracted")
    if not base_extract_dir.exists():
        print(f"Error: Extraction directory {base_extract_dir} does not exist.")
        return

    # Get all subdirectories and sort by name (timestamp) descending
    extract_dirs = sorted([d for d in base_extract_dir.iterdir() if d.is_dir()], reverse=True)
    
    if not extract_dirs:
        print("Error: No extraction directories found.")
        return
        
    extraction_dir = str(extract_dirs[0])
    output_dir = "exports/monitor_hub_analysis"
    
    print(f"Loading activities from latest extraction: {extraction_dir}...")
    activities = load_activities_from_directory(extraction_dir)
    print(f"Loaded {len(activities)} activities")
    
    if not activities:
        print("No activities found!")
        return
        
    # Build historical dataset structure
    # We'll just use dummy dates for the period as we are generating from partial data
    start_date = datetime.now() - timedelta(days=3)
    end_date = datetime.now()
    
    historical_data = {
        "analysis_period": {
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "days": 3,
            "description": "Partial analysis from interrupted run"
        },
        "workspaces": [], # We don't have easy access to workspaces list here without re-parsing, but reporter might not need it strictly
        "items": [],
        "activities": activities
    }
    
    print("Generating reports...")
    reporter = MonitorHubCSVReporter(output_dir)
    files = reporter.generate_comprehensive_reports(historical_data)
    
    print("Generated files:")
    for name, path in files.items():
        print(f"- {name}: {path}")

if __name__ == "__main__":
    main()
