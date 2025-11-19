#!/usr/bin/env python3
"""
Microsoft Fabric Historical Data Extractor

Extracts activity data for multiple days (up to 28 days) from Microsoft Fabric APIs.
Respects API limits and exports consolidated CSV files.
"""

import os
import sys
import argparse
import logging
from datetime import datetime, timedelta
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from dotenv import load_dotenv
from core.auth import create_authenticator_from_env
from core.extractor import FabricDataExtractor
from core.csv_exporter import CSVExporter


def setup_logging():
    """Setup logging configuration."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('logs/historical_extraction.log')
        ]
    )
    Path("logs").mkdir(exist_ok=True)


def extract_historical_data(start_date, end_date, output_dir, workspace_ids=None, activity_types=None):
    """
    Extract activity data for a date range from Microsoft Fabric APIs.
    
    Args:
        start_date: Start date (inclusive)
        end_date: End date (inclusive)
        output_dir: Directory to save CSV files
        workspace_ids: Optional list of workspace IDs to filter
        activity_types: Optional list of activity types to filter
        
    Returns:
        Dictionary with extraction results
    """
    logger = logging.getLogger(__name__)
    
    # Validate date range
    days_span = (end_date - start_date).days + 1
    max_days = int(os.getenv('MAX_HISTORICAL_DAYS', '28'))
    
    if days_span > max_days:
        raise ValueError(f"Date range ({days_span} days) exceeds API limit ({max_days} days)")
    
    if days_span <= 0:
        raise ValueError("End date must be on or after start date")
    
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
        
        # Extract activities day by day, organizing by date
        activities_by_date = {}
        failed_days = []
        total_activities = 0
        
        logger.info(f"📥 Extracting activities for {days_span} days...")
        
        current_date = start_date
        while current_date <= end_date:
            try:
                date_str = current_date.strftime('%Y-%m-%d')
                logger.info(f"  Processing {date_str}...")
                
                daily_activities = extractor.get_daily_activities(
                    date=current_date,
                    workspace_ids=workspace_ids,
                    activity_types=activity_types,
                    tenant_wide=True  # Use tenant-wide Power BI Admin API
                )
                
                if daily_activities:
                    activities_by_date[date_str] = daily_activities
                    total_activities += len(daily_activities)
                    logger.info(f"  ✓ {date_str}: {len(daily_activities)} activities")
                else:
                    logger.info(f"  ⊘ {date_str}: No activities")
                    
            except Exception as e:
                logger.error(f"  ✗ {date_str}: Failed - {str(e)}")
                failed_days.append(date_str)
            
            current_date += timedelta(days=1)
        
        if not activities_by_date:
            return {
                "status": "no_data",
                "message": f"No activities found for date range {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}",
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
                "days_span": days_span,
                "total_activities": 0,
                "failed_days": failed_days,
                "files_created": []
            }
        
        # Export each day to separate CSV files
        logger.info(f"✅ Retrieved {total_activities} REAL activities across {days_span} days")
        logger.info("📤 Exporting to CSV files (one per day)...")
        
        exporter = CSVExporter(export_base_path=output_dir)
        
        files_created = []
        
        # Export each day separately
        for date_str, activities in activities_by_date.items():
            if activities:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                activities_file = exporter.export_daily_activities(activities, date_obj)
                summary_file = exporter.export_activity_summary(activities, date_obj)
                files_created.extend([activities_file, summary_file])
                logger.info(f"  ✓ Exported {date_str}: {len(activities)} activities")
        
        return {
            "status": "success",
            "start_date": start_date.strftime('%Y-%m-%d'),
            "end_date": end_date.strftime('%Y-%m-%d'),
            "days_span": days_span,
            "total_activities": total_activities,
            "failed_days": failed_days,
            "source": "Microsoft Fabric APIs",
            "is_real_data": True,
            "files_created": files_created
        }
        
    except Exception as e:
        logger.error(f"Historical extraction failed: {str(e)}")
        raise


def main():
    """Main entry point."""
    load_dotenv()
    setup_logging()
    
    # Parse date range
    max_days = int(os.getenv('MAX_HISTORICAL_DAYS', '28'))
    
    parser = argparse.ArgumentParser(
        description=f"Extract historical Microsoft Fabric activity data (up to {max_days} days)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
Examples:
  # Extract last 7 days
  python extract_historical_data.py --days 7
  
  # Extract last {max_days} days (maximum)
  python extract_historical_data.py --days {max_days}
  
  # Extract specific date range
  python extract_historical_data.py --start-date 2025-11-01 --end-date 2025-11-14
  
  # Extract with workspace filter
  python extract_historical_data.py --days 7 --workspace-ids ws1,ws2,ws3
        """
    )
    
    parser.add_argument(
        "--days",
        type=int,
        help=f"Number of days back from today to extract (max {max_days})"
    )
    
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD format)"
    )
    
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD format)"
    )
    
    parser.add_argument(
        "--output-dir",
        type=str,
        default="exports/historical",
        help="Output directory for CSV files (default: exports/historical)"
    )
    
    parser.add_argument(
        "--workspace-ids",
        type=str,
        help="Comma-separated list of workspace IDs to filter"
    )
    
    parser.add_argument(
        "--activity-types",
        type=str,
        help="Comma-separated list of activity types to filter"
    )
    
    args = parser.parse_args()
    
    if args.start_date and args.end_date:
        # Use explicit date range
        try:
            start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
            end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError:
            print("❌ Invalid date format. Use YYYY-MM-DD format.")
            return 1
            
    elif args.days:
        # Use days back from today
        if args.days < 1 or args.days > max_days:
            print(f"❌ Days must be between 1 and {max_days}")
            return 1
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        start_date = end_date - timedelta(days=args.days - 1)
        
    else:
        # Default: last 7 days
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        start_date = end_date - timedelta(days=6)
    
    # Parse filters
    workspace_ids = args.workspace_ids.split(',') if args.workspace_ids else None
    activity_types = args.activity_types.split(',') if args.activity_types else None
    
    # Display configuration
    days_span = (end_date - start_date).days + 1
    
    print("🚀 Microsoft Fabric Historical Data Extractor")
    print(f"📅 Date Range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')} ({days_span} days)")
    print(f"📁 Output Directory: {args.output_dir}")
    print(f"🔒 API Limit: Max {max_days} days")
    print(f"🏢 Workspace Filter: {len(workspace_ids) if workspace_ids else 'All accessible'}")
    print(f"🔍 Activity Filter: {len(activity_types) if activity_types else 'All types'}")
    print(f"📡 Data Source: Microsoft Fabric APIs (REAL DATA)")
    
    try:
        # Extract data
        print(f"\n📥 Extracting REAL data from Microsoft Fabric APIs...")
        result = extract_historical_data(
            start_date=start_date,
            end_date=end_date,
            output_dir=args.output_dir,
            workspace_ids=workspace_ids,
            activity_types=activity_types
        )
        
        # Display results
        print(f"\n📋 Extraction Summary:")
        print(f"   • Date Range: {result['start_date']} to {result['end_date']}")
        print(f"   • Days Span: {result['days_span']}")
        print(f"   • Status: {result['status']}")
        
        if result["status"] == "success":
            print(f"   • Total Activities: {result['total_activities']:,}")
            print(f"   • Average per Day: {result['total_activities'] // result['days_span']:,}")
            if result['failed_days']:
                print(f"   • Failed Days: {len(result['failed_days'])}")
                for day in result['failed_days']:
                    print(f"      - {day}")
            else:
                print(f"   • Failed Days: 0")
            print(f"   • Data Source: {result['source']}")
            print(f"   • Real Data: {result['is_real_data']}")
            print(f"   • Files Created: {len(result['files_created'])}")
            
            print(f"\n📄 Generated Files:")
            for file_path in result["files_created"]:
                file_size = Path(file_path).stat().st_size / 1024  # KB
                file_name = Path(file_path).name
                print(f"   • {file_name} ({file_size:.1f} KB)")
            
            print(f"\n✅ Historical data extraction completed successfully!")
            print(f"   Data saved to: {Path(args.output_dir).absolute()}")
            
        else:
            print(f"   • Message: {result['message']}")
            print(f"\n⚠️  No activities found for the specified date range.")
        
        return 0
        
    except Exception as e:
        print(f"\n❌ Extraction failed: {str(e)}")
        logging.error(f"Extraction error: {str(e)}", exc_info=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
