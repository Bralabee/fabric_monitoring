#!/usr/bin/env python
"""
Quick launcher for the Lineage Explorer.
Supports both JSON (preferred) and CSV (legacy) lineage files.
"""
import sys
from pathlib import Path

# Add project root and lineage_explorer to path
project_root = Path(__file__).parent.parent  # scripts/ -> root
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

# Find lineage file (prefer JSON, fallback to CSV)
lineage_dir = project_root / "exports" / "lineage"
data_file = None

if lineage_dir.exists():
    # Prefer JSON (native format)
    json_files = sorted(lineage_dir.glob("lineage_*.json"), reverse=True)
    if json_files:
        data_file = json_files[0]
    else:
        # Fallback to CSV (legacy)
        csv_files = sorted(lineage_dir.glob("mirrored_lineage_*.csv"), reverse=True)
        if csv_files:
            data_file = csv_files[0]

if not data_file:
    print(f"No lineage file found in: {lineage_dir}")
    print("Run 'make extract-lineage' first to generate lineage data.")
    sys.exit(1)

print(f"Loading: {data_file}")

from lineage_explorer import run_server
run_server(csv_path=str(data_file), host="127.0.0.1", port=8000)
