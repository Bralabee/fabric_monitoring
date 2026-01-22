#!/usr/bin/env python
"""
Quick launcher for the Lineage Explorer.
"""
import sys
from pathlib import Path

# Add src to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / "src"))

# Find the CSV
csv_path = project_root / "exports" / "lineage" / "mirrored_lineage_20260122_121529.csv"
if not csv_path.exists():
    print(f"CSV not found at: {csv_path}")
    sys.exit(1)

print(f"Loading: {csv_path}")

from usf_fabric_monitoring.lineage_explorer import run_server
run_server(csv_path=str(csv_path), host="127.0.0.1", port=8000)
