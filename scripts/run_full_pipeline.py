#!/usr/bin/env python
"""
Run the full Monitor Hub pipeline with 28 days of extraction.
This script loads environment variables and runs the complete analysis.
"""
import sys
import os

# Add src to path
sys.path.insert(0, "src")

# Load environment variables from .env
from dotenv import load_dotenv
load_dotenv()

print("="*60)
print("MONITOR HUB PIPELINE - FULL EXTRACTION")
print("="*60)
print(f"Working directory: {os.getcwd()}")
print(f"Tenant ID: {os.getenv('AZURE_TENANT_ID', 'NOT SET')[:8]}...")
print(f"Client ID: {os.getenv('AZURE_CLIENT_ID', 'NOT SET')[:8]}...")
print("="*60)

from usf_fabric_monitoring.core.pipeline import MonitorHubPipeline

# Create pipeline
pipeline = MonitorHubPipeline(output_directory="exports/monitor_hub_analysis")

# Run the complete analysis
print("\nStarting pipeline extraction (28 days)...")
try:
    result = pipeline.run_complete_analysis(days=28, tenant_wide=True)
    print("\n" + "="*60)
    print("PIPELINE COMPLETED SUCCESSFULLY!")
    print("="*60)
    print(f"Result keys: {list(result.keys()) if result else 'None'}")
    
    # Check for generated files
    import glob
    parquet_files = glob.glob("exports/monitor_hub_analysis/parquet/*.parquet")
    print(f"\nGenerated parquet files: {len(parquet_files)}")
    for f in parquet_files:
        size = os.path.getsize(f)
        print(f"  - {os.path.basename(f)}: {size:,} bytes")
        
except Exception as e:
    print(f"\nPIPELINE FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
