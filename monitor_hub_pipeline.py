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
import logging
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import json

# Load environment variables
from dotenv import load_dotenv
load_dotenv()

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

from extract_historical_data import extract_historical_data as run_historical_extraction
from core.data_loader import load_activities_from_directory
from core.monitor_hub_reporter_clean import MonitorHubCSVReporter


class MonitorHubPipeline:
    """Complete Monitor Hub analysis pipeline"""
    
    def __init__(self, output_directory: str = None):
        """Initialize the pipeline"""
        self.logger = self._setup_logging()
        
        # Use environment configuration for output directory
        if output_directory is None:
            output_directory = os.getenv('EXPORT_DIRECTORY', 'exports/monitor_hub_analysis')
        
        self.output_directory = Path(output_directory)
        self.output_directory.mkdir(parents=True, exist_ok=True)
        
        self.reporter = None
        self.max_days = int(os.getenv('MAX_HISTORICAL_DAYS', '28'))
        
        self.logger.info("Monitor Hub Pipeline initialized")
    
    def run_complete_analysis(self, days: int = 90) -> Dict[str, Any]:
        """
        Run complete Monitor Hub analysis pipeline
        
        Args:
            days: Number of days of historical data to analyze
            
        Returns:
            Dictionary with analysis results and report file paths
        """
        resolved_days = self._resolve_days(days)
        self.logger.info(f"Starting Monitor Hub analysis for {resolved_days} days (API max {self.max_days})")

        try:
            start_date, end_date = self._calculate_date_range(resolved_days)
            extraction_dir = self._prepare_extraction_directory()

            self.logger.info("Step 1: Extracting historical activities from Fabric APIs")
            extraction_result = run_historical_extraction(
                start_date=start_date,
                end_date=end_date,
                output_dir=str(extraction_dir)
            )

            if extraction_result.get("status") != "success":
                message = extraction_result.get("message", "Historical extraction failed")
                self.logger.warning(message)
                return {"status": "error", "message": message, "report_files": {}}

            self.logger.info("Step 2: Loading enriched activity exports")
            activities = load_activities_from_directory(str(extraction_dir))

            if not activities:
                self.logger.warning("No activities loaded from exports")
                return {"status": "no_data", "message": "No activities data available for analysis"}

            historical_data = self._build_historical_dataset(activities, start_date, end_date, resolved_days)
            self.logger.info(f"✅ Loaded {len(activities)} activities for analysis")

            self.logger.info("Step 3: Generating comprehensive CSV reports")
            self.reporter = MonitorHubCSVReporter(str(self.output_directory))
            report_files = self.reporter.generate_comprehensive_reports(historical_data)
            self.logger.info(f"✅ Generated {len(report_files)} comprehensive reports")

            pipeline_summary = self._create_pipeline_summary(historical_data, report_files, resolved_days)

            return {
                "status": "success",
                "summary": pipeline_summary,
                "report_files": report_files,
                "raw_data_summary": {
                    "total_activities": len(historical_data["activities"]),
                    "analysis_period_days": resolved_days,
                    "workspaces_analyzed": len(historical_data.get("workspaces", [])),
                    "items_analyzed": len(historical_data.get("items", []))
                }
            }

        except Exception as e:  # pragma: no cover - defensive
            self.logger.error(f"Pipeline failed: {str(e)}")
            return {
                "status": "error",
                "message": str(e),
                "report_files": {}
            }
    
    def _create_pipeline_summary(self, historical_data: Dict[str, Any], 
                                report_files: Dict[str, str], days: int) -> Dict[str, Any]:
        """Create summary of pipeline execution"""
        
        # Calculate basic statistics
        activities = historical_data.get("activities", [])
        total_activities = len(activities)
        
        if total_activities > 0:
            failed_activities = sum(1 for a in activities if a.get("status") == "Failed")
            success_rate = ((total_activities - failed_activities) / total_activities) * 100
            
            # Calculate duration statistics
            durations = [a.get("duration_seconds", 0) for a in activities]
            total_duration_seconds = sum(durations)
            avg_duration_seconds = total_duration_seconds / total_activities if total_activities > 0 else 0
        else:
            failed_activities = 0
            success_rate = 0
            total_duration_seconds = 0
            avg_duration_seconds = 0
        
        # Get unique counts
        workspaces = set(a.get("workspace_id") for a in activities if a.get("workspace_id"))
        users = set(a.get("submitted_by") for a in activities if a.get("submitted_by"))
        domains = set(a.get("domain") for a in activities if a.get("domain"))
        item_types = set(a.get("item_type") for a in activities if a.get("item_type"))
        
        summary = {
            "pipeline_execution": {
                "executed_at": datetime.now().isoformat(),
                "analysis_period_days": days,
                "total_reports_generated": len(report_files)
            },
            "key_measurables": {
                "total_activities": total_activities,
                "failed_activities": failed_activities,
                "success_rate_percent": round(success_rate, 2),
                "total_duration_hours": round(total_duration_seconds / 3600, 2)
            },
            "analysis_scope": {
                "unique_workspaces": len(workspaces),
                "unique_users": len(users),
                "unique_domains": len(domains),
                "unique_item_types": len(item_types)
            },
            "report_files": report_files
        }
        
        # Save summary to file
        summary_file = self.output_directory / f"pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, default=str)
        
        self.logger.info(f"Pipeline summary saved to: {summary_file}")
        
        return summary

    def _resolve_days(self, requested_days: Optional[int]) -> int:
        default_days = int(os.getenv('DEFAULT_ANALYSIS_DAYS', '7'))
        if not requested_days:
            requested_days = default_days
        return min(max(1, requested_days), self.max_days)

    def _calculate_date_range(self, days: int) -> Tuple[datetime, datetime]:
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        start_date = end_date - timedelta(days=days - 1)
        return start_date, end_date

    def _prepare_extraction_directory(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        extraction_dir = self.output_directory / "extracted" / timestamp
        extraction_dir.mkdir(parents=True, exist_ok=True)
        return extraction_dir

    def _build_historical_dataset(self, activities: List[Dict[str, Any]], start_date: datetime, end_date: datetime, days: int) -> Dict[str, Any]:
        workspace_lookup: Dict[str, Dict[str, Any]] = {}
        item_lookup: Dict[str, Dict[str, Any]] = {}

        for activity in activities:
            workspace_id = activity.get("workspace_id")
            if workspace_id and workspace_id not in workspace_lookup:
                workspace_lookup[workspace_id] = {
                    "id": workspace_id,
                    "displayName": activity.get("workspace_name") or "Unknown",
                }

            item_id = activity.get("item_id")
            if item_id and item_id not in item_lookup:
                item_lookup[item_id] = {
                    "id": item_id,
                    "displayName": activity.get("item_name") or "Unknown",
                    "type": activity.get("item_type") or "Unknown"
                }

        return {
            "analysis_period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "days": days,
                "description": f"{days}-day analysis window"
            },
            "workspaces": list(workspace_lookup.values()),
            "items": list(item_lookup.values()),
            "activities": activities
        }
    
    def _setup_logging(self) -> logging.Logger:
        """Setup logging configuration"""
        # Ensure stdout is line-buffered for immediate output
        if hasattr(sys.stdout, 'reconfigure'):
            sys.stdout.reconfigure(line_buffering=True)

        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('monitor_hub_pipeline.log')
            ]
        )
        return logging.getLogger(__name__)
    
    def print_results_summary(self, results: Dict[str, Any]) -> None:
        """Print formatted results summary"""
        print("\n" + "="*80)
        print("MICROSOFT FABRIC MONITOR HUB ANALYSIS RESULTS")
        print("="*80)
        
        if results["status"] == "success":
            summary = results["summary"]
            
            print(f"\n📊 KEY MEASURABLES (Statement of Work Requirements):")
            measurables = summary["key_measurables"]
            print(f"   • Activities: {measurables['total_activities']:,}")
            print(f"   • Failed Activity: {measurables['failed_activities']:,}")
            print(f"   • Success %: {measurables['success_rate_percent']}%")
            print(f"   • Total Duration: {measurables['total_duration_hours']} hours")
            
            print(f"\n🔍 ANALYSIS SCOPE:")
            scope = summary["analysis_scope"]
            print(f"   • Period: {summary['pipeline_execution']['analysis_period_days']} days")
            print(f"   • Workspaces: {scope['unique_workspaces']:,}")
            print(f"   • Users: {scope['unique_users']:,}")
            print(f"   • Domains: {scope['unique_domains']:,}")
            print(f"   • Item Types: {scope['unique_item_types']:,}")
            
            print(f"\n📋 REPORTS GENERATED ({len(results['report_files'])} files):")
            for report_name, file_path in results["report_files"].items():
                file_name = Path(file_path).name
                print(f"   • {report_name}: {file_name}")
            
            print(f"\n📁 Export Directory: {self.output_directory}")
            
        elif results["status"] == "no_data":
            print(f"\n⚠️  {results['message']}")
            
        else:
            print(f"\n❌ Analysis failed: {results.get('message', 'Unknown error')}")
        
        print("\n" + "="*80)


def main():
    """Main function for command line execution"""
    max_days = int(os.getenv('MAX_HISTORICAL_DAYS', '28'))
    
    parser = argparse.ArgumentParser(
        description="Microsoft Fabric Monitor Hub Analysis Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python monitor_hub_pipeline.py                    # Run with defaults (tenant-wide, 90 days)
  python monitor_hub_pipeline.py --days 30         # Analyze last 30 days (all tenant workspaces)
  python monitor_hub_pipeline.py --member-only     # Monitor only member workspaces (~139)
  python monitor_hub_pipeline.py --output exports/ # Custom output directory
        """
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
    
    # Initialize and run pipeline
    pipeline = MonitorHubPipeline(args.output_dir)
    
    print("🚀 Starting Microsoft Fabric Monitor Hub Analysis Pipeline")
    print(f"   • Analysis Period: {args.days} days")
    print(f"   • Output Directory: {args.output_dir}")
    print(f"   • Monitoring Scope: {'Member workspaces only (~139)' if args.member_only else 'All tenant workspaces (~2187)'}")
    print(f"   • API Strategy: {'Per-workspace loop' if args.member_only else 'Tenant-wide Power BI Admin API'}")
    
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