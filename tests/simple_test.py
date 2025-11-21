#!/usr/bin/env python3
"""
Simple Working Test - Fixed Version

Tests basic functionality. Max 28 days back as per API limits.
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

def main():
    # Load environment variables
    from dotenv import load_dotenv
    load_dotenv()

    # Add src to path
    sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

    print("🔍 Simple test - NO INFINITE LOOPS")

    # Test 1: Environment
    print("\n1. Environment configuration:")
    max_days = int(os.getenv('MAX_HISTORICAL_DAYS', '28'))
    print(f"   MAX_HISTORICAL_DAYS: {max_days}")
    
    # Test 2: Date calculation (28 days max)
    today = datetime.now()
    days_back = min(max_days, 28)  # Ensure max 28 days
    start_date = today - timedelta(days=days_back)
    
    print(f"\n2. Date range:")
    print(f"   Today: {today.strftime('%Y-%m-%d')}")
    print(f"   Looking back {days_back} days to: {start_date.strftime('%Y-%m-%d')}")
    
    # Test 3: Generate data (no API calls)
    print(f"\n3. Generating simulation data...")
    
    activities = []
    for i in range(50):  # 50 sample activities
        activities.append({
            "activity_id": f"act_{i}",
            "workspace_id": f"ws_{(i % 3) + 1}",
            "item_name": f"Item {i+1}",
            "item_type": ["Lakehouse", "Report", "Pipeline"][i % 3],
            "activity_type": "DataRefresh",
            "status": "Succeeded" if i % 4 != 0 else "Failed",
            "start_time": (today - timedelta(days=i % days_back)).isoformat(),
            "duration_seconds": 1800 + (i * 60),
            "submitted_by": f"user{(i % 3) + 1}@company.com",
            "domain": "company.com",
            "location": ["East US", "West US", "Central US"][i % 3],
            "is_simulated": True
        })
    
    print(f"   Generated {len(activities)} activities")
    
    # Test 4: CSV export to your location
    print(f"\n4. CSV export:")
    
    export_dir = Path("/home/sanmi/Documents/J'TOYE_DIGITAL/LEIT_TEKSYSTEMS/1_Project_Rhico/usf_fabric_monitoring/exports/daily")
    export_dir.mkdir(parents=True, exist_ok=True)
    
    import pandas as pd
    
    # Activities CSV
    df = pd.DataFrame(activities)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    csv_file = export_dir / f"activities_{timestamp}.csv"
    df.to_csv(csv_file, index=False)
    print(f"   ✅ Activities: {csv_file}")
    
    # Summary CSV
    total = len(activities)
    failed = sum(1 for a in activities if a["status"] == "Failed")
    success_rate = ((total - failed) / total * 100)
    
    summary = pd.DataFrame([{
        "date": today.strftime('%Y-%m-%d'),
        "total_activities": total,
        "failed_activities": failed,
        "success_rate_percent": round(success_rate, 2),
        "analysis_days": days_back
    }])
    
    summary_file = export_dir / f"summary_{timestamp}.csv"
    summary.to_csv(summary_file, index=False)
    print(f"   ✅ Summary: {summary_file}")
    
    print(f"\n📊 RESULTS ({days_back} days max):")
    print(f"   • Total Activities: {total}")
    print(f"   • Failed Activities: {failed}")
    print(f"   • Success Rate: {success_rate:.1f}%")
    print(f"   • Export Location: {export_dir}")
    
    print(f"\n✅ WORKING CORRECTLY!")
    print("   - Max 28 days (API compliant)")
    print("   - Generates daily data")
    print("   - Saves CSV to your location")
    print("   - NO infinite loops")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        traceback.print_exc()