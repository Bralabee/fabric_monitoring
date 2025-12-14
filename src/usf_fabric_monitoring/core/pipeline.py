import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
import json
import pandas as pd

# Add src to path for imports if needed, but assuming package structure
from usf_fabric_monitoring.scripts.extract_historical_data import extract_historical_data as run_historical_extraction
from usf_fabric_monitoring.scripts.extract_fabric_item_details import run_item_details_extraction
from usf_fabric_monitoring.core.data_loader import load_activities_from_directory
from usf_fabric_monitoring.core.monitor_hub_reporter_clean import MonitorHubCSVReporter
from usf_fabric_monitoring.core.logger import setup_logging
from usf_fabric_monitoring.core.utils import resolve_path

class MonitorHubPipeline:
    """Complete Monitor Hub analysis pipeline"""
    
    def __init__(self, output_directory: str = None):
        """Initialize the pipeline"""
        # Configure logging for the entire package so scripts inherit it
        self.logger = setup_logging(
            name="usf_fabric_monitoring",
            log_file='logs/monitor_hub_pipeline.log'
        )
        
        # Use environment configuration for output directory
        if output_directory is None:
            # Default to exports/monitor_hub_analysis, resolved to Lakehouse if in Fabric
            self.output_directory = resolve_path(os.getenv('EXPORT_DIRECTORY', 'exports/monitor_hub_analysis'))
        else:
            # Resolve user provided path to ensure it lands in Lakehouse if relative
            self.output_directory = resolve_path(output_directory)
        
        self.output_directory.mkdir(parents=True, exist_ok=True)
        
        self.reporter = None
        self.max_days = int(os.getenv('MAX_HISTORICAL_DAYS', '28'))
        
        self.logger.info("Monitor Hub Pipeline initialized")
    
    def run_complete_analysis(self, days: int = 90, *, tenant_wide: bool = True) -> Dict[str, Any]:
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
                output_dir=str(extraction_dir),
                tenant_wide=tenant_wide,
            )

            if extraction_result.get("status") != "success":
                message = extraction_result.get("message", "Historical extraction failed")
                self.logger.warning(message)
                return {"status": "error", "message": message, "report_files": {}}

            if extraction_result.get("failed_days"):
                self.logger.warning(f"⚠️  Failed to extract data for {len(extraction_result['failed_days'])} days: {extraction_result['failed_days']}")

            self.logger.info("Step 1b: Extracting detailed job history (for accurate failure tracking)")
            # We use a separate directory for item details, but we can pass it if needed.
            # The default is exports/fabric_item_details, which matches what we load later.
            details_output_dir = self.output_directory / "fabric_item_details"
            
            # Check for recent detailed job extraction (8-hour cache logic)
            details_cache_valid = self._check_recent_job_details_extraction(details_output_dir)
            
            if details_cache_valid:
                self.logger.info("✅ Using cached detailed job data (within 8 hours)")
                details_result = {"status": "success", "cached": True, "jobs_count": "cached"}
            else:
                details_result = run_item_details_extraction(
                    output_dir=str(details_output_dir)
                )
            
            if details_result.get("status") != "success":
                self.logger.warning(f"Detailed job extraction failed: {details_result.get('message')}")
                # We proceed, but warn that failure data might be incomplete
            else:
                if details_result.get("cached"):
                    self.logger.info("✅ Using cached detailed job records")
                else:
                    self.logger.info(f"✅ Extracted {details_result.get('jobs_count', 0)} new job records")

            if os.getenv("USF_FABRIC_MONITORING_SHOW_PROGRESS", "1") == "1":
                print("\n" + "=" * 40)
                print("✅ Extraction Complete. Starting Analysis...")
                print("=" * 40 + "\n")

            self.logger.info("Step 2: Loading enriched activity exports")
            activities = load_activities_from_directory(str(extraction_dir))

            self.logger.info("Step 2b: Loading detailed job history (for accurate status)")
            detailed_jobs = self._load_detailed_jobs()
            activities = self._merge_activities(activities, detailed_jobs)

            if not activities:
                self.logger.warning("No activities loaded from exports")
                return {"status": "no_data", "message": "No activities data available for analysis"}

            historical_data = self._build_historical_dataset(activities, start_date, end_date, resolved_days)
            self.logger.info(f"✅ Loaded {len(activities)} activities for analysis")

            self.logger.info("Step 2c: Persisting merged data to Parquet (Source of Truth)")
            self._save_to_parquet(historical_data)

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

        except Exception as e:
            self.logger.error(f"Pipeline failed: {str(e)}")
            return {
                "status": "error",
                "message": str(e),
                "report_files": {}
            }
    
    def _save_to_parquet(self, historical_data: Dict[str, Any]) -> None:
        """
        Save merged data to Parquet files for Delta Table ingestion.
        
        Args:
            historical_data: The dictionary containing merged activities, workspaces, and items.
        """
        parquet_dir = self.output_directory / "parquet"
        parquet_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Save Activities
        activities = historical_data.get("activities", [])
        if activities:
            try:
                df_activities = pd.DataFrame(activities)
                # Ensure datetime columns are properly typed for Parquet
                for col in ['start_time', 'end_time', 'creation_time']:
                    if col in df_activities.columns:
                        df_activities[col] = pd.to_datetime(df_activities[col], errors='coerce')
                
                activities_path = parquet_dir / f"activities_{timestamp}.parquet"
                df_activities.to_parquet(activities_path, index=False)
                self.logger.info(f"Saved {len(activities)} activities to {activities_path}")
            except Exception as e:
                self.logger.error(f"Failed to save activities to parquet: {e}")
        
        # 2. Save Workspaces
        workspaces = historical_data.get("workspaces", [])
        if workspaces:
            try:
                df_workspaces = pd.DataFrame(workspaces)
                workspaces_path = parquet_dir / f"workspaces_{timestamp}.parquet"
                df_workspaces.to_parquet(workspaces_path, index=False)
                self.logger.info(f"Saved {len(workspaces)} workspaces to {workspaces_path}")
            except Exception as e:
                self.logger.error(f"Failed to save workspaces to parquet: {e}")
                
        # 3. Save Items
        items = historical_data.get("items", [])
        if items:
            try:
                df_items = pd.DataFrame(items)
                items_path = parquet_dir / f"items_{timestamp}.parquet"
                df_items.to_parquet(items_path, index=False)
                self.logger.info(f"Saved {len(items)} items to {items_path}")
            except Exception as e:
                self.logger.error(f"Failed to save items to parquet: {e}")

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
        # Use a persistent directory for raw data to enable caching/incremental updates
        extraction_dir = self.output_directory / "raw_data"
        extraction_dir.mkdir(parents=True, exist_ok=True)
        return extraction_dir

    def _check_recent_job_details_extraction(self, details_output_dir: Path, hours_threshold: int = 8) -> bool:
        """Check if we have recent detailed job data within the threshold."""
        try:
            if not details_output_dir.exists():
                return False
            
            # Find all jobs_*.json files
            job_files = list(details_output_dir.glob("jobs_*.json"))
            if not job_files:
                return False
            
            # Get the most recent file by modification time - using sorted to avoid max() key issues
            job_files_with_time = [(f, f.stat().st_mtime) for f in job_files]
            job_files_with_time.sort(key=lambda x: x[1], reverse=True)  # Sort by time, newest first
            most_recent_file, _ = job_files_with_time[0]
            
            # Check if it's within the threshold
            file_time = datetime.fromtimestamp(most_recent_file.stat().st_mtime)
            current_time = datetime.now()
            hours_old = (current_time - file_time).total_seconds() / 3600
            
            if hours_old <= hours_threshold:
                self.logger.info(f"Found recent job details file: {most_recent_file.name} ({hours_old:.1f} hours old)")
                return True
            else:
                self.logger.info(f"Most recent job details file is {hours_old:.1f} hours old (threshold: {hours_threshold}h)")
                return False
        
        except Exception as e:
            self.logger.warning(f"Error checking job details cache: {e}")
            return False

    def _load_detailed_jobs(self) -> List[Dict[str, Any]]:
        """Load all detailed jobs exports"""
        export_dir = self.output_directory / "fabric_item_details"
        if not export_dir.exists():
            self.logger.warning(f"Detailed jobs directory not found: {export_dir}")
            return []
            
        # Find all jobs_*.json files
        job_files = list(export_dir.glob("jobs_*.json"))
        if not job_files:
            self.logger.warning("No detailed job files found")
            return []
            
        all_jobs = []
        for job_file in job_files:
            self.logger.info(f"Loading detailed jobs from {job_file}")
            try:
                with open(job_file, 'r') as f:
                    jobs = json.load(f)
                    all_jobs.extend(jobs)
            except Exception as e:
                self.logger.error(f"Failed to load detailed jobs from {job_file}: {e}")
                
        self.logger.info(f"Total detailed jobs loaded: {len(all_jobs)}")
        return all_jobs

    def _merge_activities(self, activities: List[Dict[str, Any]], detailed_jobs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Smart Merge: Correlate Activity Events with Detailed Job History.
        Uses pandas merge_asof to align events by ItemId and Time (within 5 mins).
        """
        if not activities:
            return []
        if not detailed_jobs:
            return activities

        self.logger.info("Starting Smart Merge of Activities and Detailed Jobs...")

        # 1. Prepare Activities DataFrame
        df_activities = pd.DataFrame(activities)
        
        # Ensure timestamps are datetime
        time_cols = ['start_time', 'end_time', 'creation_time']
        for col in time_cols:
            if col in df_activities.columns:
                df_activities[col] = pd.to_datetime(df_activities[col], utc=True, errors='coerce')

        # Ensure item_id is string and handle NaNs
        if 'item_id' in df_activities.columns:
            df_activities['item_id'] = df_activities['item_id'].astype(str)
        
        # Sort for merge_asof
        df_activities = df_activities.sort_values('start_time')

        # 2. Prepare Jobs DataFrame
        df_jobs = pd.DataFrame(detailed_jobs)
        
        # Map Job columns to friendly names
        job_rename_map = {
            'id': 'job_instance_id',
            'startTimeUtc': 'job_start_time',
            'endTimeUtc': 'job_end_time',
            'status': 'job_status',
            'failureReason': 'job_failure_reason',
            'itemId': 'item_id',
            'jobType': 'job_type',
            'invoker': 'job_invoker'
        }
        df_jobs = df_jobs.rename(columns=job_rename_map)
        
        # Ensure timestamps are datetime
        job_time_cols = ['job_start_time', 'job_end_time']
        for col in job_time_cols:
            if col in df_jobs.columns:
                df_jobs[col] = pd.to_datetime(df_jobs[col], utc=True, errors='coerce')
        
        # Ensure item_id is string
        if 'item_id' in df_jobs.columns:
            df_jobs['item_id'] = df_jobs['item_id'].astype(str)

        # Drop rows without start time or item_id
        df_jobs = df_jobs.dropna(subset=['job_start_time', 'item_id'])
        
        # Sort for merge_asof
        df_jobs = df_jobs.sort_values('job_start_time')

        # 3. Perform Merge
        # We want to keep all activities, and find the nearest job execution
        try:
            merged_df = pd.merge_asof(
                df_activities,
                df_jobs,
                left_on='start_time',
                right_on='job_start_time',
                by='item_id',
                tolerance=pd.Timedelta('5min'),
                direction='nearest',
                suffixes=('', '_job')
            )
            
            # 4. Enrich Data
            
            # Update Status: If Job Failed, mark Activity as Failed
            # (Activity logs often say "Completed" even if the internal job failed)
            mask_job_failed = merged_df['job_status'] == 'Failed'
            merged_df.loc[mask_job_failed, 'status'] = 'Failed'
            
            # Enrich Failure Reason
            # If activity doesn't have a failure reason, take it from the job
            if 'failure_reason' not in merged_df.columns:
                merged_df['failure_reason'] = None
            
            merged_df['failure_reason'] = merged_df['failure_reason'].fillna(merged_df['job_failure_reason'])
            
            # Create explicit error_message column if not exists
            if 'error_message' not in merged_df.columns:
                merged_df['error_message'] = merged_df['failure_reason']
            else:
                merged_df['error_message'] = merged_df['error_message'].fillna(merged_df['failure_reason'])

            # Enrich User (if job has invoker info)
            # Note: Job history often doesn't have user info, but sometimes 'invoker' field exists
            if 'job_invoker' in merged_df.columns:
                merged_df['submitted_by'] = merged_df['submitted_by'].fillna(merged_df['job_invoker'])

            # Fix Duration Calculation: Use job end times when activity end times are missing
            self.logger.info("Applying duration calculation fix...")
            original_missing_end_time = merged_df['end_time'].isna().sum()
            
            if original_missing_end_time > 0 and 'job_end_time' in merged_df.columns:
                # Fill missing end_time with job_end_time
                merged_df['end_time'] = merged_df['end_time'].fillna(merged_df['job_end_time'])
                
                after_fix_missing_end_time = merged_df['end_time'].isna().sum()
                fixed_count = original_missing_end_time - after_fix_missing_end_time
                self.logger.info(f"Fixed {fixed_count} missing end times using job data")
                
                # Recalculate duration_seconds
                def calculate_duration(start_time, end_time):
                    if pd.isna(start_time) or pd.isna(end_time):
                        return 0.0
                    try:
                        duration = (end_time - start_time).total_seconds()
                        return max(0.0, duration)  # Ensure non-negative duration
                    except:
                        return 0.0

                merged_df['duration_seconds'] = merged_df.apply(
                    lambda row: calculate_duration(row['start_time'], row['end_time']), 
                    axis=1
                )
                
                # Log improvement statistics
                original_zero_duration = (pd.DataFrame(activities).get('duration_seconds', pd.Series([0.0] * len(activities))) == 0.0).sum()
                fixed_zero_duration = (merged_df['duration_seconds'] == 0.0).sum()
                duration_improvement = original_zero_duration - fixed_zero_duration
                if duration_improvement > 0:
                    self.logger.info(f"Duration fix: restored duration data for {duration_improvement} records")

            # Fill remaining NaNs in key columns to avoid serialization issues
            merged_df['status'] = merged_df['status'].fillna('Unknown')
            
            self.logger.info(f"Smart Merge Complete. Enriched {len(merged_df)} activities.")
            
            # Convert back to list of dicts
            # Handle datetime serialization by converting to ISO format strings
            for col in ['start_time', 'end_time', 'creation_time', 'job_start_time', 'job_end_time']:
                if col in merged_df.columns:
                    merged_df[col] = merged_df[col].apply(lambda x: x.isoformat() if pd.notnull(x) else None)

            return merged_df.to_dict(orient='records')

        except Exception as e:
            self.logger.error(f"Smart Merge failed: {e}")
            self.logger.warning("Falling back to original activities list")
            return activities

    def _build_historical_dataset(self, activities: List[Dict[str, Any]], start_date: datetime, end_date: datetime, days: int) -> Dict[str, Any]:
        workspace_lookup: Dict[str, Dict[str, Any]] = {}
        item_lookup: Dict[str, Dict[str, Any]] = {}
        
        # Filter for Fabric-only activities
        fabric_activities = [a for a in activities if self._is_fabric_activity(a)]
        self.logger.info(f"Filtered {len(activities)} total activities to {len(fabric_activities)} Fabric activities")

        for activity in fabric_activities:
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
            "activities": fabric_activities
        }

    def _is_fabric_activity(self, activity: Dict[str, Any]) -> bool:
        """Check if activity is related to a Fabric item type."""
        # If it comes from JobHistory, it is definitely a Fabric activity
        if activity.get("source") == "JobHistory":
            return True

        # List of known Fabric item types
        fabric_types = {
            "DataPipeline", "Notebook", "SparkJobDefinition", 
            "Lakehouse", "Warehouse", "KQLDatabase", "KQLQueryset", 
            "Eventstream", "Reflex", "Environment", "MLModel",
            "Dataflow", "Datamart", "GraphQLApi", "MirroredDatabase"
        }
        
        # List of known Fabric activity operations
        fabric_ops = {
            "RunArtifact", "ExecuteNotebook", "ExecutePipeline", 
            "ExecuteSparkJob", "ExecuteDataflow", "ReadLakehouse",
            "WriteLakehouse"
        }
        
        item_type = activity.get("item_type")
        activity_type = activity.get("activity_type")
        
        if item_type and item_type in fabric_types:
            return True
            
        if activity_type and activity_type in fabric_ops:
            return True
            
        return False
    
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
