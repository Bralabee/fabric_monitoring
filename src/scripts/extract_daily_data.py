#!/usr/bin/env python3
"""
Microsoft Fabric Daily Data Extractor - REAL API VERSION

Extracts REAL daily activity data from Microsoft Fabric APIs and exports to CSV files.
Uses actual API calls to get genuine activity data within 28-day API limits.
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[1]))

from dotenv import load_dotenv
from core.auth import create_authenticator_from_env
from core.extractor import FabricDataExtractor
from core.csv_exporter import CSVExporter


def setup_logging():
    """Setup basic logging configuration."""
    # Ensure stdout is line-buffered for immediate output
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(line_buffering=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler('logs/daily_extraction.log')
        ]
    )
    
    # Create logs directory
    Path("logs").mkdir(exist_ok=True)


def extract_real_daily_data(target_date, output_dir, workspace_ids=None, activity_types=None):
    """
    Extract REAL daily activity data from Microsoft Fabric APIs.
    
    Args:
        target_date: Date to extract data for
        output_dir: Directory to save CSV files
        workspace_ids: Optional list of workspace IDs to filter
        activity_types: Optional list of activity types to filter
        
    Returns:
        Dictionary with extraction results and file paths
    """
    logger = logging.getLogger(__name__)
    
    try:
        # Initialize authentication
        logger.info("🔐 Authenticating with Microsoft Fabric...")
        auth = create_authenticator_from_env()
        
        # Initialize data extractor
        logger.info("📡 Initializing Fabric data extractor...")
        extractor = FabricDataExtractor(auth)
        
        # Test connectivity
        logger.info("🧪 Testing API connectivity...")
        connectivity = extractor.test_api_connectivity()
        
        if not all(connectivity.values()):
            logger.warning("⚠️  Some API connectivity tests failed")
            for test, result in connectivity.items():
                logger.info(f"   {test}: {'✅ PASS' if result else '❌ FAIL'}")
        
        # Extract daily activities using REAL API calls
        logger.info(f"📥 Extracting activities for {target_date.strftime('%Y-%m-%d')}...")
        activities = extractor.get_daily_activities(
            date=target_date,
            workspace_ids=workspace_ids,
            activity_types=activity_types,
            tenant_wide=True  # Use tenant-wide Power BI Admin API
        )
        
        if not activities:
            logger.warning(f"No activities found for {target_date.strftime('%Y-%m-%d')}")
            return {
                "status": "no_data",
                "date": target_date.strftime('%Y-%m-%d'),
                "message": f"No activities found for {target_date.strftime('%Y-%m-%d')}",
                "files_created": []
            }
        
        logger.info(f"✅ Retrieved {len(activities)} REAL activities from Fabric APIs")
        
        # Initialize CSV exporter
        csv_exporter = CSVExporter(output_dir)
        
        # Export activities to CSV
        logger.info("📤 Exporting to CSV files...")
        activities_file = csv_exporter.export_daily_activities(activities, target_date)
        
        # Export summary statistics
        summary_file = csv_exporter.export_activity_summary(activities, target_date)
        
        files_created = []
        if activities_file:
            files_created.append(activities_file)
        if summary_file:
            files_created.append(summary_file)
        
        # Calculate summary statistics
        total_activities = len(activities)
        failed_activities = sum(1 for a in activities if a.get("status") == "Failed" or a.get("Status") == "Failure")
        success_rate = ((total_activities - failed_activities) / total_activities * 100) if total_activities > 0 else 0
        
        return {
            "status": "success",
            "date": target_date.strftime('%Y-%m-%d'),
            "total_activities": total_activities,
            "failed_activities": failed_activities,
            "success_rate": round(success_rate, 2),
            "files_created": files_created,
            "source": "Microsoft Fabric APIs",
            "is_real_data": True
        }
        
    except Exception as e:
        logger.error(f"❌ Real data extraction failed: {str(e)}")
        return {
            "status": "error",
            "date": target_date.strftime('%Y-%m-%d'),
            "message": str(e),
            "files_created": []
        }


def parse_list_argument(arg_str):
    """Parse comma-separated string into list."""
    if arg_str:
        return [item.strip() for item in arg_str.split(",") if item.strip()]
    return None


def main():
    """Main function - extracts REAL data from Fabric APIs."""
    parser = argparse.ArgumentParser(
        description="Extract REAL daily Fabric activity data from APIs",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python extract_daily_data.py                                    # Extract yesterday's REAL data
  python extract_daily_data.py --date 2025-11-14                 # Extract specific date
  python extract_daily_data.py --workspaces "ws1,ws2,ws3"        # Filter specific workspaces
  python extract_daily_data.py --activities "Refresh,ViewReport" # Filter activity types
  python extract_daily_data.py --output-dir /custom/path         # Custom output location
        """
    )
    
    parser.add_argument(
        "--date",
        type=str,
        help="Date to extract data for (YYYY-MM-DD). Defaults to yesterday."
    )
    
    parser.add_argument(
        "--workspaces",
        type=str,
        help="Comma-separated list of workspace IDs to filter"
    )
    
    parser.add_argument(
        "--activities",
        type=str,
        help="Comma-separated list of activity types to filter"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        help="Output directory for CSV files"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Load environment
    load_dotenv()
    
    # Determine target date
    if args.date:
        try:
            target_date = datetime.strptime(args.date, "%Y-%m-%d")
        except ValueError:
            print(f"❌ Invalid date format: {args.date}. Use YYYY-MM-DD.")
            return 1
    else:
        # Default to yesterday
        target_date = datetime.now() - timedelta(days=1)
    
    # Validate we don't exceed API limits (max 28 days back)
    days_back = (datetime.now() - target_date).days
    max_days = int(os.getenv('MAX_HISTORICAL_DAYS', '28'))
    
    if days_back > max_days:
        print(f"❌ Date {target_date.strftime('%Y-%m-%d')} is {days_back} days ago.")
        print(f"   API limit is {max_days} days. Please use a more recent date.")
        return 1
    
    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    else:
        output_dir = os.getenv('DAILY_EXPORT_DIRECTORY', 'exports/daily')
    
    # Parse filter arguments
    workspace_ids = parse_list_argument(args.workspaces)
    activity_types = parse_list_argument(args.activities)
    
    print("🚀 Microsoft Fabric Daily Data Extractor - REAL API VERSION")
    print(f"📅 Target Date: {target_date.strftime('%Y-%m-%d')} ({days_back} days ago)")
    print(f"📁 Output Directory: {output_dir}")
    print(f"🔒 API Compliant: Max {max_days} days back")
    print(f"🏢 Workspace Filter: {len(workspace_ids) if workspace_ids else 'All accessible'}")
    print(f"🔍 Activity Filter: {len(activity_types) if activity_types else 'All types'}")
    print(f"📡 Data Source: Microsoft Fabric APIs (REAL DATA)")
    
    try:
        # Extract REAL data from APIs
        print(f"\n📥 Extracting REAL data from Microsoft Fabric APIs...")
        result = extract_real_daily_data(
            target_date=target_date,
            output_dir=output_dir,
            workspace_ids=workspace_ids,
            activity_types=activity_types
        )
        
        # Display results
        print(f"\n📋 Extraction Summary:")
        print(f"   • Date: {result['date']}")
        print(f"   • Status: {result['status']}")
        
        if result["status"] == "success":
            print(f"   • Total Activities: {result['total_activities']}")
            print(f"   • Failed Activities: {result['failed_activities']}")
            print(f"   • Success Rate: {result['success_rate']}%")
            print(f"   • Data Source: {result['source']}")
            print(f"   • Real Data: {result['is_real_data']}")
            print(f"   • Files Created: {len(result['files_created'])}")
            
            print(f"\n📄 Generated Files:")
            for file_path in result["files_created"]:
                file_size = Path(file_path).stat().st_size / 1024  # KB
                file_name = Path(file_path).name
                print(f"   • {file_name} ({file_size:.1f} KB)")
            
            print(f"\n✅ REAL data extraction completed successfully!")
            print(f"   Data saved to: {Path(output_dir).absolute()}")
            
        elif result["status"] == "no_data":
            print(f"   • Message: {result['message']}")
            print(f"\n⚠️  No activities found for the specified date.")
            
        else:
            print(f"   • Error: {result['message']}")
            print(f"\n❌ Extraction failed.")
            return 1
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Extraction failed: {str(e)}")
        logger.error(f"Main execution failed: {str(e)}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())