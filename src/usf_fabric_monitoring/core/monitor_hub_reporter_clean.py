"""
Microsoft Fabric Monitor Hub CSV Reporter

This module generates comprehensive CSV reports for historical analysis of Microsoft Fabric
Monitor Hub data. It produces the reports specified in the statement of work to identify:
- Constant failures
- Excess activity per User/Location/Domain  
- Historical performance trends

Export formats:
- Historical Activities Report
- User Performance Analysis
- Domain Performance Analysis  
- Failure Analysis Report
- Daily/Weekly Trend Reports
"""

import os
import logging
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
from pathlib import Path
import json

from .historical_analyzer import HistoricalAnalysisEngine


class MonitorHubCSVReporter:
    """Generates comprehensive CSV reports for Monitor Hub analysis"""
    
    def __init__(self, export_directory: str = "exports/monitor_hub_reports"):
        """
        Initialize the CSV reporter
        
        Args:
            export_directory: Directory to save CSV reports
        """
        self.logger = logging.getLogger(__name__)
        self.export_directory = Path(export_directory)
        self.export_directory.mkdir(parents=True, exist_ok=True)
        
        # Analysis engine
        self.analyzer = HistoricalAnalysisEngine()
        
        # Report timestamp for consistent naming
        self.report_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def generate_comprehensive_reports(self, historical_data: Dict[str, Any]) -> Dict[str, str]:
        """
        Generate all comprehensive CSV reports
        
        Args:
            historical_data: Raw historical data from MonitorHubExtractor
            
        Returns:
            Dictionary mapping report names to file paths
        """
        self.logger.info("Generating comprehensive Monitor Hub CSV reports")
        
        # Perform analysis
        analysis_results = self.analyzer.perform_comprehensive_analysis(historical_data)
        
        # Generate individual reports
        report_files = {}
        
        # 1. Master Activities Report
        activities_file = self._generate_activities_report(historical_data["activities"])
        report_files["activities_report"] = activities_file
        
        # 2. Key Measurables Summary
        summary_file = self._generate_summary_report(analysis_results)
        report_files["summary_report"] = summary_file
        
        # 3. User Performance Analysis
        user_file = self._generate_user_performance_report(analysis_results)
        report_files["user_performance"] = user_file
        
        # 4. Domain Performance Analysis
        domain_file = self._generate_domain_performance_report(analysis_results)
        report_files["domain_performance"] = domain_file
        
        # 5. Failure Analysis Report
        failure_file = self._generate_failure_analysis_report(analysis_results)
        report_files["failure_analysis"] = failure_file
        
        # 6. Daily Trends Report
        daily_file = self._generate_daily_trends_report(analysis_results)
        report_files["daily_trends"] = daily_file
        
        # 7. Compute & Failure Analysis Report
        compute_file = self._generate_compute_analysis_report(historical_data["activities"])
        report_files["compute_analysis"] = compute_file
        
        self.logger.info(f"Generated {len(report_files)} comprehensive reports")
        return report_files
    
    def _generate_activities_report(self, activities: List[Dict[str, Any]]) -> str:
        """Generate master activities report with all activity details"""
        if not activities:
            return self._create_empty_report("activities_master")
        
        df = pd.DataFrame(activities)
        
        # Add derived columns for analysis
        df["date"] = pd.to_datetime(df["start_time"], format='mixed', errors='coerce', utc=True).dt.date
        df["hour"] = pd.to_datetime(df["start_time"], format='mixed', errors='coerce', utc=True).dt.hour
        df["duration_minutes"] = df["duration_seconds"] / 60
        df["is_failed"] = df["status"] == "Failed"
        df["is_success"] = df["status"] != "Failed"
        
        # Reorder columns for better readability
        column_order = [
            "activity_id", "workspace_id", "item_id", "item_name", "item_type", 
            "activity_type", "status", "start_time", "end_time", "date", "hour",
            "duration_seconds", "duration_minutes", "submitted_by", "created_by", 
            "last_updated_by", "domain", "location", "object_url", "is_simulated"
        ]
        
        # Keep only columns that exist
        available_columns = [col for col in column_order if col in df.columns]
        df_ordered = df[available_columns]
        
        # Sort by start time
        df_ordered = df_ordered.sort_values("start_time", ascending=False)
        
        filename = f"activities_master_{self.report_timestamp}.csv"
        filepath = self.export_directory / filename
        df_ordered.to_csv(filepath, index=False)
        
        self.logger.info(f"Generated activities master report: {filename}")
        return str(filepath)
    
    def _generate_summary_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate key measurables summary report"""
        key_measurables = analysis_results["key_measurables"]
        metadata = analysis_results["analysis_metadata"]
        
        summary_data = [
            {
                "metric": "Total Activities",
                "value": key_measurables["total_activities"],
                "unit": "count",
                "period": f"{metadata['period']['days']} days"
            },
            {
                "metric": "Failed Activities", 
                "value": key_measurables["failed_activities"],
                "unit": "count",
                "period": f"{metadata['period']['days']} days"
            },
            {
                "metric": "Success Rate",
                "value": key_measurables["success_rate_percent"],
                "unit": "percentage",
                "period": f"{metadata['period']['days']} days"
            },
            {
                "metric": "Total Duration",
                "value": key_measurables["total_duration_hours"],
                "unit": "hours",
                "period": f"{metadata['period']['days']} days"
            }
        ]
        
        df = pd.DataFrame(summary_data)
        
        filename = f"key_measurables_summary_{self.report_timestamp}.csv"
        filepath = self.export_directory / filename
        df.to_csv(filepath, index=False)
        
        self.logger.info(f"Generated summary report: {filename}")
        return str(filepath)
    
    def _generate_user_performance_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate user performance analysis report"""
        user_analysis = analysis_results.get("user_activity_analysis", {})
        top_users = user_analysis.get("top_users_by_activity", {})
        
        if not top_users:
            return self._create_empty_report("user_performance")
        
        user_data = []
        for user, stats in top_users.items():
            user_data.append({
                "user": user,
                "total_activities": stats["total_activities"],
                "failed_activities": stats["failed_activities"],
                "success_rate_percent": stats["success_rate"],
                "total_duration_seconds": stats["total_duration"],
                "average_duration_seconds": stats["avg_duration"],
                "daily_average_activities": round(stats["total_activities"] / 90, 2),
                "risk_level": self._assess_user_risk_level(stats)
            })
        
        df = pd.DataFrame(user_data)
        df = df.sort_values("total_activities", ascending=False)
        
        filename = f"user_performance_analysis_{self.report_timestamp}.csv"
        filepath = self.export_directory / filename
        df.to_csv(filepath, index=False)
        
        self.logger.info(f"Generated user performance report: {filename}")
        return str(filepath)
    
    def _generate_domain_performance_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate domain performance analysis report"""
        domain_analysis = analysis_results.get("domain_analysis", {})
        domain_summary = domain_analysis.get("domain_summary", {})
        
        if not domain_summary:
            return self._create_empty_report("domain_performance")
        
        domain_data = []
        for domain, stats in domain_summary.items():
            domain_data.append({
                "domain": domain,
                "total_activities": stats["total_activities"],
                "failed_activities": stats["failed_activities"], 
                "success_rate_percent": stats["success_rate"],
                "total_duration_seconds": stats["total_duration"],
                "average_duration_seconds": stats["avg_duration"],
                "percentage_of_total_activities": stats["percentage_of_total"]
            })
        
        df = pd.DataFrame(domain_data)
        df = df.sort_values("total_activities", ascending=False)
        
        filename = f"domain_performance_analysis_{self.report_timestamp}.csv"
        filepath = self.export_directory / filename
        df.to_csv(filepath, index=False)
        
        self.logger.info(f"Generated domain performance report: {filename}")
        return str(filepath)
    
    def _generate_failure_analysis_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate detailed failure analysis report"""
        failure_analysis = analysis_results.get("failure_analysis", {})
        
        if failure_analysis.get("total_failures", 0) == 0:
            return self._create_empty_report("failure_analysis")
        
        # Top failing items
        top_failing = failure_analysis.get("top_failing_items", [])
        failure_data = []
        
        for item in top_failing:
            failure_data.append({
                "item_id": item["item_id"],
                "item_name": item["item_name"],
                "item_type": item["item_type"],
                "failure_count": item["failure_count"],
                "severity": "High" if item["failure_count"] > 10 else "Medium" if item["failure_count"] > 5 else "Low"
            })
        
        if failure_data:
            df = pd.DataFrame(failure_data)
            df = df.sort_values("failure_count", ascending=False)
            
            filename = f"failure_analysis_{self.report_timestamp}.csv"
            filepath = self.export_directory / filename
            df.to_csv(filepath, index=False)
            
            self.logger.info(f"Generated failure analysis report: {filename}")
            return str(filepath)
        else:
            return self._create_empty_report("failure_analysis")
    
    def _generate_daily_trends_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate daily trends analysis report"""
        trend_analysis = analysis_results.get("trend_analysis", {})
        daily_trends = trend_analysis.get("daily_trends", {})
        
        if not daily_trends:
            return self._create_empty_report("daily_trends")
        
        trend_data = []
        for date_str, stats in daily_trends.items():
            trend_data.append({
                "date": date_str,
                "total_activities": stats["total_activities"],
                "failed_activities": stats["failed_activities"],
                "success_rate_percent": stats["success_rate"],
                "total_duration_seconds": stats["total_duration"],
                "average_duration_seconds": stats["avg_duration"]
            })
        
        df = pd.DataFrame(trend_data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date", ascending=False)
        
        filename = f"daily_trends_analysis_{self.report_timestamp}.csv"
        filepath = self.export_directory / filename
        df.to_csv(filepath, index=False)
        
        self.logger.info(f"Generated daily trends report: {filename}")
        return str(filepath)
    
    def _create_empty_report(self, report_type: str) -> str:
        """Create empty report when no data available"""
        empty_data = [{"message": f"No data available for {report_type} analysis"}]
        df = pd.DataFrame(empty_data)
        
        filename = f"{report_type}_empty_{self.report_timestamp}.csv"
        filepath = self.export_directory / filename
        df.to_csv(filepath, index=False)
        
        self.logger.warning(f"Generated empty report: {filename}")
        return str(filepath)
    
    def _assess_user_risk_level(self, stats: Dict[str, Any]) -> str:
        """Assess user risk level based on activity patterns"""
        success_rate = stats["success_rate"]
        total_activities = stats["total_activities"]
        
        if success_rate < 70 or total_activities > 100:
            return "High"
        elif success_rate < 85 or total_activities > 50:
            return "Medium"
        else:
            return "Low"
            
    def _generate_compute_analysis_report(self, activities: List[Dict[str, Any]]) -> str:
        """
        Generate detailed compute analysis report for Spark, Notebooks, and Pipelines.
        Focuses on who ran what, success/failure rates, and duration.
        """
        if not activities:
            return self._create_empty_report("compute_analysis")
            
        # Filter for compute-intensive items
        compute_types = {'SparkJob', 'Notebook', 'DataPipeline', 'SparkJobDefinition', 'Lakehouse'}
        compute_activities = [a for a in activities if a.get("item_type") in compute_types]
        
        if not compute_activities:
            return self._create_empty_report("compute_analysis")
            
        df = pd.DataFrame(compute_activities)
        
        # Ensure required columns exist
        required_cols = ["submitted_by", "item_name", "item_type", "status", "duration_seconds", "start_time"]
        for col in required_cols:
            if col not in df.columns:
                df[col] = None
                
        # Group by User, Item Name, Item Type, and Status
        # We want to see: User X ran Notebook Y -> Success (5 times), Failed (2 times)
        
        # First, fill missing values
        df["submitted_by"] = df["submitted_by"].fillna("Unknown")
        df["item_name"] = df["item_name"].fillna("Unknown")
        df["status"] = df["status"].fillna("Unknown")
        # Do NOT fill duration with 0, as it skews averages. Keep as NaN.
        
        # Aggregate
        summary = df.groupby(["submitted_by", "item_name", "item_type", "status"]).agg(
            count=("activity_id", "count"),
            avg_duration=("duration_seconds", "mean"),
            total_duration=("duration_seconds", "sum"),
            last_run=("start_time", "max")
        ).reset_index()
        
        # Pivot to have Success/Failed counts in columns if possible, or just keep as list
        # For a cleaner report, let's re-aggregate to have one row per User+Item
        
        final_data = []
        
        # Group by User + Item to calculate rates
        grouped = df.groupby(["submitted_by", "item_name", "item_type"])
        
        for (user, item, type_), group in grouped:
            total_runs = len(group)
            failed_runs = len(group[group["status"] == "Failed"])
            success_runs = len(group[group["status"] == "Succeeded"])
            
            # Calculate duration stats only on completed runs (where duration is not null)
            completed_runs = group[group["duration_seconds"].notna()]
            avg_duration = completed_runs["duration_seconds"].mean() if not completed_runs.empty else 0
            total_duration = completed_runs["duration_seconds"].sum()
            
            # Identify runs with unknown duration (likely In Progress or Crashed)
            unknown_duration_runs = len(group[group["duration_seconds"].isna()])
            
            last_run = group["start_time"].max()
            
            # Identify most common error if any failures
            error_msg = ""
            if failed_runs > 0:
                # If we had error messages in the logs, we'd extract them here.
                # For now, we just note it failed.
                pass
                
            final_data.append({
                "User": user,
                "Item Name": item,
                "Item Type": type_,
                "Total Runs": total_runs,
                "Successful Runs": success_runs,
                "Failed Runs": failed_runs,
                "Unknown/In Progress Runs": unknown_duration_runs,
                "Failure Rate %": round((failed_runs / total_runs) * 100, 1),
                "Avg Duration (s)": round(avg_duration, 1),
                "Total Duration (s)": round(total_duration, 1),
                "Last Run Time": last_run
            })
            
        results_df = pd.DataFrame(final_data)
        
        # Sort by Failure Rate (desc) then Total Runs (desc) to highlight issues
        results_df = results_df.sort_values(["Failure Rate %", "Total Runs"], ascending=[False, False])
        
        filename = f"compute_analysis_{self.report_timestamp}.csv"
        filepath = self.export_directory / filename
        results_df.to_csv(filepath, index=False)
        
        self.logger.info(f"Generated compute analysis report: {filename}")
        return str(filepath)