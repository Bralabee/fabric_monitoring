"""
CLI entry point for Lineage Explorer.

Usage:
    python -m lineage_explorer [OPTIONS]
    
Options:
    --file PATH     Path to lineage JSON or CSV file
    --port PORT     Server port (default: 8000)
    --host HOST     Server host (default: 127.0.0.1)
"""

import argparse
import sys
from pathlib import Path

# Find the default lineage file if exists
def find_default_file():
    """Look for the most recent lineage JSON or CSV (prefers JSON)."""
    # Get the module directory and navigate up to project root
    module_dir = Path(__file__).parent
    # Go up: lineage_explorer -> usf_fabric_monitoring (root)
    project_root = module_dir.parent
    
    lineage_dir = project_root / "exports" / "lineage"
    
    # Also check relative to current working directory
    cwd_lineage = Path.cwd() / "exports" / "lineage"
    
    for search_dir in [lineage_dir, cwd_lineage]:
        if search_dir.exists():
            # Prefer JSON (native format)
            json_files = sorted(search_dir.glob("lineage_*.json"), reverse=True)
            if json_files:
                return str(json_files[0])
            
            # Fallback to CSV (legacy)
            csv_files = sorted(search_dir.glob("mirrored_lineage_*.csv"), reverse=True)
            if csv_files:
                return str(csv_files[0])
    
    return None


def main():
    parser = argparse.ArgumentParser(
        description="Fabric Lineage Explorer - Interactive lineage visualization"
    )
    parser.add_argument(
        "--file",
        type=str,
        default=find_default_file(),
        help="Path to lineage JSON or CSV file (auto-detects if not specified)"
    )
    # Keep --csv as alias for backwards compatibility
    parser.add_argument(
        "--csv",
        type=str,
        default=None,
        help="(Deprecated) Alias for --file"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8000,
        help="Server port (default: 8000)"
    )
    parser.add_argument(
        "--host",
        type=str,
        default="127.0.0.1",
        help="Server host (default: 127.0.0.1)"
    )
    
    args = parser.parse_args()
    
    # --csv is deprecated alias for --file
    file_path = args.csv if args.csv else args.file
    
    if not file_path:
        print("Error: No lineage file found.")
        print("Please specify with --file PATH")
        sys.exit(1)
    
    data_path = Path(file_path)
    if not data_path.exists():
        print(f"Error: File not found: {data_path}")
        sys.exit(1)
    
    print(f"🔗 Loading lineage data from: {data_path}")
    print(f"🚀 Starting server at http://{args.host}:{args.port}")
    
    from .server import run_server
    run_server(csv_path=str(data_path), host=args.host, port=args.port)


if __name__ == "__main__":
    main()
