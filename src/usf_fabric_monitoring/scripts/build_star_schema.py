#!/usr/bin/env python3
"""
Build Star Schema for Fabric Monitoring Analytics

This script transforms raw Monitor Hub data into a dimensional star schema
suitable for SQL queries and semantic model construction.

Usage:
    # Build from latest pipeline output (incremental)
    python build_star_schema.py
    
    # Full refresh
    python build_star_schema.py --full-refresh
    
    # Custom directories
    python build_star_schema.py --input-dir exports/monitor_hub_analysis --output-dir exports/star_schema
    
    # Print DDL only
    python build_star_schema.py --ddl-only

The star schema includes:
- Fact Tables: fact_activity, fact_daily_metrics, fact_user_metrics
- Dimensions: dim_date, dim_time, dim_workspace, dim_item, dim_user, dim_activity_type, dim_status

Incremental loading uses high-water mark pattern to only process new records.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from datetime import datetime

from dotenv import load_dotenv

# Import from parent package (now inside usf_fabric_monitoring.scripts)
from usf_fabric_monitoring.core.star_schema_builder import (
    StarSchemaBuilder,
    build_star_schema_from_pipeline_output,
    ALL_STAR_SCHEMA_DDLS
)
from usf_fabric_monitoring.core.logger import setup_logging


def main():
    """Main entry point for star schema building."""
    load_dotenv()
    
    parser = argparse.ArgumentParser(
        description="Build Star Schema for Fabric Monitoring Analytics",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Build from default pipeline output (incremental):
    python build_star_schema.py

  Full refresh (rebuild all tables):
    python build_star_schema.py --full-refresh

  Custom input/output directories:
    python build_star_schema.py --input-dir /path/to/data --output-dir /path/to/output

  Print DDL statements:
    python build_star_schema.py --ddl-only

  Print schema description:
    python build_star_schema.py --describe
        """
    )
    
    parser.add_argument(
        "--input-dir",
        default=os.getenv("EXPORT_DIRECTORY", "exports/monitor_hub_analysis"),
        help="Path to Monitor Hub pipeline output directory (default: exports/monitor_hub_analysis)"
    )
    
    parser.add_argument(
        "--output-dir",
        default=os.getenv("STAR_SCHEMA_OUTPUT_DIR", "exports/star_schema"),
        help="Output directory for star schema tables (default: exports/star_schema)"
    )
    
    parser.add_argument(
        "--full-refresh",
        action="store_true",
        help="Perform full refresh instead of incremental load"
    )
    
    parser.add_argument(
        "--ddl-only",
        action="store_true",
        help="Print DDL statements and exit"
    )
    
    parser.add_argument(
        "--describe",
        action="store_true",
        help="Print schema description and exit"
    )
    
    parser.add_argument(
        "--date-range-days",
        type=int,
        default=365,
        help="Number of days for date dimension (default: 365)"
    )
    
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    logger = setup_logging(name="build_star_schema", log_file="logs/star_schema_build.log")
    
    # Handle DDL-only mode
    if args.ddl_only:
        print("=" * 80)
        print("STAR SCHEMA DDL STATEMENTS")
        print("=" * 80)
        print()
        for table_name, ddl in ALL_STAR_SCHEMA_DDLS.items():
            print(f"-- {table_name.upper()}")
            print(ddl)
            print()
        return 0
    
    # Handle describe mode
    if args.describe:
        builder = StarSchemaBuilder()
        print(builder.describe_schema())
        return 0
    
    # Build star schema
    print("\n" + "=" * 80)
    print("FABRIC MONITORING - STAR SCHEMA BUILDER")
    print("=" * 80)
    print(f"Input Directory:  {args.input_dir}")
    print(f"Output Directory: {args.output_dir}")
    print(f"Mode:             {'Full Refresh' if args.full_refresh else 'Incremental'}")
    print(f"Date Range:       {args.date_range_days} days")
    print("=" * 80 + "\n")
    
    try:
        results = build_star_schema_from_pipeline_output(
            pipeline_output_dir=args.input_dir,
            output_directory=args.output_dir,
            incremental=not args.full_refresh
        )
        
        print("\n" + "=" * 80)
        print("BUILD RESULTS")
        print("=" * 80)
        
        if results["status"] == "success":
            print(f"\n✅ Star Schema build completed successfully!")
            print(f"   Duration: {results.get('duration_seconds', 0):.2f} seconds")
            
            print("\n📊 Dimensions Built:")
            for dim_name, count in results.get("dimensions_built", {}).items():
                if not dim_name.endswith("_new"):
                    new_key = f"{dim_name}_new"
                    new_count = results.get("dimensions_built", {}).get(new_key, "")
                    new_suffix = f" (+{new_count} new)" if new_count else ""
                    print(f"   • {dim_name}: {count:,} records{new_suffix}")
            
            print("\n📈 Fact Tables Built:")
            for fact_name, count in results.get("facts_built", {}).items():
                if not fact_name.endswith("_new"):
                    new_key = f"{fact_name}_new"
                    new_count = results.get("facts_built", {}).get(new_key, "")
                    new_suffix = f" (+{new_count} new)" if new_count else ""
                    print(f"   • {fact_name}: {count:,} records{new_suffix}")
            
            print(f"\n📁 Output Directory: {results['output_directory']}")
            
            # List output files
            output_path = Path(results['output_directory'])
            if output_path.exists():
                parquet_files = list(output_path.glob("*.parquet"))
                if parquet_files:
                    print("\n📄 Output Files:")
                    for f in sorted(parquet_files):
                        size_mb = f.stat().st_size / (1024 * 1024)
                        print(f"   • {f.name} ({size_mb:.2f} MB)")
            
        else:
            print(f"\n❌ Star Schema build failed!")
            for error in results.get("errors", []):
                print(f"   Error: {error}")
            return 1
        
        print("\n" + "=" * 80)
        
        # Save results summary
        summary_path = Path(args.output_dir) / f"build_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_path, "w") as f:
            json.dump(results, f, indent=2, default=str)
        print(f"\nBuild summary saved to: {summary_path}")
        
        return 0
        
    except FileNotFoundError as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure to run the Monitor Hub pipeline first:")
        print("  make monitor-hub")
        return 1
        
    except Exception as e:
        logger.exception(f"Star Schema build failed: {e}")
        print(f"\n❌ Unexpected error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
