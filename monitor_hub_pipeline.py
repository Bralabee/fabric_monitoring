"""
Microsoft Fabric Monitor Hub Analysis Pipeline

This script provides complete end-to-end analysis of Microsoft Fabric Monitor Hub data,
including 90-day historical extraction, analysis, and CSV report generation as specified
in the statement of work.

Key Features:
- 90-day historical data extraction 
- Key measurables calculation (Activities, Failed Activity, Success %, Total Duration)
- Multi-dimensional analysis (Time/Date, Location, Domain, User, Item Type, Status)
- Comprehensive CSV reporting for performance optimization
- Identification of constant failures and excess activity per User/Location/Domain

Usage:
    python monitor_hub_pipeline.py [--days 90] [--output-dir exports/]
"""

import os
import sys
import argparse
from pathlib import Path

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from usf_fabric_monitoring.core.pipeline import MonitorHubPipeline


def main():
    """Main function for command line execution"""
    max_days = int(os.getenv('MAX_HISTORICAL_DAYS', '28'))
    
    parser = argparse.ArgumentParser(
        description="Microsoft Fabric Monitor Hub Analysis Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=\"\"\"
Examples:
  python monitor_hub_pipeline.py                    # Run with defaults (tenant-wide, 90 days)
  python monitor_hub_pipeline.py --days 30         # Analyze last 30 days (all tenant workspaces)
  python monitor_hub_pipeline.py --member-only     # Monitor only member workspaces (~139)
  python monitor_hub_pipeline.py --output exports/ # Custom output directory
        \"\"\"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        default=None,
        help=f"Number of days of historical data to analyze (default: from env, max: {max_days} due to API limits)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="exports/monitor_hub_analysis",
        help="Output directory for reports (default: exports/monitor_hub_analysis)"
    )
    
    parser.add_argument(
        "--member-only",
        action="store_true",
        help="Monitor only member workspaces (~139) instead of all tenant workspaces (~2187)"
    )
    
    args = parser.parse_args()
    
    # Print banner immediately
    print("🚀 Starting Microsoft Fabric Monitor Hub Analysis Pipeline", flush=True)
    print(f"   • Analysis Period: {args.days} days", flush=True)
    print(f"   • Output Directory: {args.output_dir}", flush=True)
    print(f"   • Monitoring Scope: {'Member workspaces only (~139)' if args.member_only else 'All tenant workspaces (~2187)'}", flush=True)
    print(f"   • API Strategy: {'Per-workspace loop' if args.member_only else 'Tenant-wide Power BI Admin API'}", flush=True)

    # Initialize and run pipeline
    pipeline = MonitorHubPipeline(args.output_dir)
    
    results = pipeline.run_complete_analysis(
        days=args.days
    )
    
    pipeline.print_results_summary(results)
    
    # Exit with appropriate code
    if results["status"] == "success":
        print("\n✅ Analysis completed successfully!")
        return 0
    else:
        print(f"\n❌ Analysis failed: {results.get('message', 'Unknown error')}")
        return 1


if __name__ == "__main__":
    exit(main())
