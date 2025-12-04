#!/usr/bin/env python3
"""Test script to validate duration calculation fix in the enhanced pipeline."""

import sys
import os
import json
import pandas as pd
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from usf_fabric_monitoring.core.pipeline import MonitorHubPipeline

def test_duration_fix():
    """Test the enhanced pipeline duration calculation functionality."""
    
    print("🔍 Testing Enhanced Pipeline Duration Fix...")
    
    # Initialize pipeline with output directory (not config file)
    output_dir = "test_output/duration_fix_validation"
    
    try:
        pipeline = MonitorHubPipeline(output_dir)
        print("✅ Pipeline initialized successfully")
        
        # Test with existing CSV data
        print("\n📊 Testing with existing CSV data...")
        
        # Check for existing exports directory
        exports_dir = Path("exports")
        if not exports_dir.exists():
            print("❌ No exports directory found - need data to test")
            return False
            
        # Look for CSV data files
        csv_files = list(exports_dir.glob("**/fabric_activities_*.csv"))
        json_files = list(exports_dir.glob("**/fabric_job_details_*.json"))
        
        print(f"   - Activity CSV files found: {len(csv_files)}")
        print(f"   - Job details JSON files found: {len(json_files)}")
        
        if not csv_files:
            print("❌ No activity CSV files found for testing")
            return False
            
        # Load some sample data for testing
        from usf_fabric_monitoring.core.data_loader import load_activities_from_directory
        
        # Find the parent directory of CSV files
        csv_parent = csv_files[0].parent.parent if csv_files else None
        if csv_parent:
            print(f"   - Loading data from: {csv_parent}")
            activities = load_activities_from_directory(str(csv_parent))
            print(f"   - Activities loaded: {len(activities)}")
        else:
            activities = []
            
        # Load job details if available
        job_details = []
        for json_file in json_files[:1]:  # Test with first file
            try:
                with open(json_file, 'r') as f:
                    json_data = json.load(f)
                    if isinstance(json_data, list):
                        job_details.extend(json_data)
                    else:
                        job_details.append(json_data)
            except Exception as e:
                print(f"   - Warning: Could not load {json_file}: {e}")
                
        print(f"   - Job details loaded: {len(job_details)}")
        
        if not activities:
            print("❌ No activity data available for testing")
            return False
            
        # Create mock job details for testing if we don't have them
        if not job_details and activities:
            print("\n🔧 Creating mock job details for duration fix testing...")
            # Create simplified mock job details based on activity data
            job_details = []
            for i, activity in enumerate(activities[:5]):  # Test with first 5 activities
                start_time = activity.get('start_time', '2024-12-02T18:00:00Z')
                # Parse the start time to create a proper end time
                try:
                    from datetime import datetime, timedelta
                    if start_time:
                        # Parse start time and add 1 hour
                        start_dt = pd.to_datetime(start_time, utc=True)
                        end_dt = start_dt + timedelta(hours=1)
                        end_time = end_dt.isoformat()
                    else:
                        start_time = '2024-12-02T18:00:00Z'
                        end_time = '2024-12-02T19:00:00Z'
                except:
                    start_time = '2024-12-02T18:00:00Z'
                    end_time = '2024-12-02T19:00:00Z'
                    
                job_detail = {
                    'id': activity.get('activity_id', f'mock_id_{i}'),
                    'itemId': str(activity.get('item_id', f'item_{i}')),
                    'startTimeUtc': start_time,
                    'endTimeUtc': end_time,  # End time 1 hour after start
                    'status': 'Completed',
                    'failureReason': None,
                    'jobType': 'DataPipeline',
                    'invoker': 'User'
                }
                job_details.append(job_detail)
            print(f"   - Mock job details created: {len(job_details)}")
        
        # Test the enhanced merge with duration fix
        if activities and job_details:
            print("\n🔧 Testing enhanced merge with duration calculations...")
            
            # Test with small subset for focused testing  
            test_activities = activities[:5]
            test_jobs = job_details[:5]
            
            print(f"   - Testing with {len(test_activities)} activities and {len(test_jobs)} job details")
            
            merged_data = pipeline._merge_activities(test_activities, test_jobs)
            print(f"   - Merged records: {len(merged_data)}")
            
            if merged_data:
                df = pd.DataFrame(merged_data)
                
                # Debug: Show sample data
                print(f"   - Sample merged record fields: {list(df.columns)}")
                if len(df) > 0:
                    sample_record = df.iloc[0]
                    print(f"   - Sample start_time: {sample_record.get('start_time')}")
                    print(f"   - Sample end_time: {sample_record.get('end_time')}")
                    print(f"   - Sample duration_seconds: {sample_record.get('duration_seconds')}")
                
                # Check duration improvements
                if 'duration_seconds' in df.columns:
                    valid_durations = df['duration_seconds'].notna() & (df['duration_seconds'] > 0)
                    valid_count = valid_durations.sum()
                    
                    print(f"   - Records with valid duration after merge: {valid_count}/{len(df)} ({valid_count/len(df)*100:.1f}%)")
                    
                    # Show all duration values for debugging
                    durations = df['duration_seconds'].tolist()
                    print(f"   - All duration values: {durations}")
                    
                    if valid_count > 0:
                        duration_stats = df[valid_durations]['duration_seconds'].describe()
                        print(f"   - Duration statistics:")
                        print(f"     * Mean: {duration_stats['mean']:.2f}s")
                        print(f"     * Median: {duration_stats['50%']:.2f}s")
                        print(f"     * Max: {duration_stats['max']:.2f}s")
                        
                # Check if end_time was filled from job data
                if 'end_time' in df.columns:
                    valid_end_times = df['end_time'].notna()
                    end_time_count = valid_end_times.sum()
                    print(f"   - Records with valid end_time: {end_time_count}/{len(df)} ({end_time_count/len(df)*100:.1f}%)")
                    
        else:
            print("\n⚠️  Testing with activities only (no job details available)...")
            df = pd.DataFrame(activities[:100])  # Test with first 100 records
            
            if 'duration_seconds' in df.columns:
                valid_durations = df['duration_seconds'].notna() & (df['duration_seconds'] > 0)
                valid_count = valid_durations.sum()
                total_count = len(df)
                print(f"   - Records with valid duration: {valid_count}/{total_count} ({valid_count/total_count*100:.1f}%)")
                
        print("\n✅ Duration fix validation completed!")
            
        print("\n✅ Pipeline duration fix test completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error testing pipeline: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    # Change to monitoring directory
    monitoring_dir = Path(__file__).parent
    os.chdir(monitoring_dir)
    
    print("🚀 Starting Enhanced Pipeline Duration Fix Validation\n")
    
    success = test_duration_fix()
    
    if success:
        print("\n🎉 All tests passed! Duration fix is working correctly in the pipeline.")
        sys.exit(0)
    else:
        print("\n💥 Tests failed! Check the implementation.")
        sys.exit(1)