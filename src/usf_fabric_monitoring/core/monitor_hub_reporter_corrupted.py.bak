"""
Microsoft Fabric Monitor Hub CSV Reporter



































































































































































































































































































    exit(main())if __name__ == "__main__":        return 1        print(f"\n❌ Analysis failed: {results.get('message', 'Unknown error')}")    else:        return 0        print("\n✅ Analysis completed successfully!")    if results["status"] == "success":    # Exit with appropriate code        pipeline.print_results_summary(results)        )        use_simulation=not args.no_simulation        days=args.days,    results = pipeline.run_complete_analysis(        print(f"   • Simulation Mode: {'Disabled' if args.no_simulation else 'Enabled'}")    print(f"   • Output Directory: {args.output_dir}")    print(f"   • Analysis Period: {args.days} days")    print("🚀 Starting Microsoft Fabric Monitor Hub Analysis Pipeline")        pipeline = MonitorHubPipeline(args.output_dir)    # Initialize and run pipeline        args = parser.parse_args()        )        help="Disable simulation mode (use real data only)"        action="store_true",        "--no-simulation",    parser.add_argument(        )        help="Output directory for reports (default: exports/monitor_hub_analysis)"        default="exports/monitor_hub_analysis",        type=str,        "--output-dir",    parser.add_argument(        )        help="Number of days of historical data to analyze (default: 90)"        default=90,        type=int,        "--days",    parser.add_argument(        )        """  python monitor_hub_pipeline.py --no-simulation   # Use real data only  python monitor_hub_pipeline.py --output exports/ # Custom output directory  python monitor_hub_pipeline.py --days 30         # Analyze last 30 days  python monitor_hub_pipeline.py                    # Run with defaults (90 days)Examples:        epilog="""        formatter_class=argparse.RawDescriptionHelpFormatter,        description="Microsoft Fabric Monitor Hub Analysis Pipeline",    parser = argparse.ArgumentParser(    """Main function for command line execution"""def main():        print("\n" + "="*80)                    print(f"\n❌ Analysis failed: {results.get('message', 'Unknown error')}")        else:                        print(f"\n⚠️  {results['message']}")        elif results["status"] == "no_data":                        print(f"\n📁 Export Directory: {self.output_directory}")                            print(f"   • {report_name}: {file_name}")                file_name = Path(file_path).name            for report_name, file_path in results["report_files"].items():            print(f"\n📋 REPORTS GENERATED ({len(results['report_files'])} files):")                        print(f"   • Item Types: {scope['unique_item_types']:,}")            print(f"   • Domains: {scope['unique_domains']:,}")            print(f"   • Users: {scope['unique_users']:,}")            print(f"   • Workspaces: {scope['unique_workspaces']:,}")            print(f"   • Period: {summary['pipeline_execution']['analysis_period_days']} days")            scope = summary["analysis_scope"]            print(f"\n🔍 ANALYSIS SCOPE:")                        print(f"   • Total Duration: {measurables['total_duration_hours']} hours")            print(f"   • Success %: {measurables['success_rate_percent']}%")            print(f"   • Failed Activity: {measurables['failed_activities']:,}")            print(f"   • Activities: {measurables['total_activities']:,}")            measurables = summary["key_measurables"]            print(f"\n📊 KEY MEASURABLES (Statement of Work Requirements):")                        summary = results["summary"]        if results["status"] == "success":                print("="*80)        print("MICROSOFT FABRIC MONITOR HUB ANALYSIS RESULTS")        print("\n" + "="*80)        """Print formatted results summary"""    def print_results_summary(self, results: Dict[str, Any]) -> None:            return logging.getLogger(__name__)        )            ]                logging.FileHandler('monitor_hub_pipeline.log')                logging.StreamHandler(),            handlers=[            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',            level=logging.INFO,        logging.basicConfig(        """Setup logging configuration"""    def _setup_logging(self) -> logging.Logger:            return summary                self.logger.info(f"Pipeline summary saved to: {summary_file}")                    json.dump(summary, f, indent=2, default=str)        with open(summary_file, 'w', encoding='utf-8') as f:        summary_file = self.output_directory / f"pipeline_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"        # Save summary to file                }            "report_files": report_files            },                "unique_item_types": len(item_types)                "unique_domains": len(domains),                "unique_users": len(users),                "unique_workspaces": len(workspaces),            "analysis_scope": {            },                "total_duration_hours": round(total_duration_seconds / 3600, 2)                "success_rate_percent": round(success_rate, 2),                "failed_activities": failed_activities,                "total_activities": total_activities,            "key_measurables": {            },                "total_reports_generated": len(report_files)                "analysis_period_days": days,                "executed_at": datetime.now().isoformat(),            "pipeline_execution": {        summary = {                item_types = set(a.get("item_type") for a in activities if a.get("item_type"))        domains = set(a.get("domain") for a in activities if a.get("domain"))        users = set(a.get("submitted_by") for a in activities if a.get("submitted_by"))        workspaces = set(a.get("workspace_id") for a in activities if a.get("workspace_id"))        # Get unique counts                    avg_duration_seconds = 0            total_duration_seconds = 0            success_rate = 0            failed_activities = 0        else:            avg_duration_seconds = total_duration_seconds / total_activities if total_activities > 0 else 0            total_duration_seconds = sum(durations)            durations = [a.get("duration_seconds", 0) for a in activities]            # Calculate duration statistics                        success_rate = ((total_activities - failed_activities) / total_activities) * 100            failed_activities = sum(1 for a in activities if a.get("status") == "Failed")        if total_activities > 0:                total_activities = len(activities)        activities = historical_data.get("activities", [])        # Calculate basic statistics                """Create summary of pipeline execution"""                                report_files: Dict[str, str], days: int) -> Dict[str, Any]:    def _create_pipeline_summary(self, historical_data: Dict[str, Any],                 }                "report_files": {}                "message": str(e),                "status": "error",            return {            self.logger.error(f"Pipeline failed: {str(e)}")        except Exception as e:                        }                }                    "items_analyzed": len(historical_data.get("items", []))                    "workspaces_analyzed": len(historical_data.get("workspaces", [])),                    "analysis_period_days": days,                    "total_activities": len(historical_data["activities"]),                "raw_data_summary": {                "report_files": report_files,                "summary": pipeline_summary,                "status": "success",            return {                        )                historical_data, report_files, days            pipeline_summary = self._create_pipeline_summary(            # Step 4: Create pipeline summary                        self.logger.info(f"✅ Generated {len(report_files)} comprehensive reports")                        report_files = self.reporter.generate_comprehensive_reports(historical_data)                        self.reporter = MonitorHubCSVReporter(str(self.output_directory))            self.logger.info("Step 3: Generating comprehensive CSV reports")            # Step 3: Generate comprehensive reports                        self.logger.info(f"✅ Extracted {len(historical_data['activities'])} activities")                            return {"status": "no_data", "message": "No activities data available for analysis"}                self.logger.warning("No activities data extracted")            if not historical_data or not historical_data.get("activities"):                        )                use_simulation=use_simulation                days=days,            historical_data = self.extractor.extract_historical_data(                        self.extractor = MonitorHubExtractor(self.authenticator)            self.logger.info("Step 2: Extracting historical Monitor Hub data")            # Step 2: Extract historical data                        self.logger.info("✅ Authentication successful")                            raise Exception("Authentication failed")            if not self.authenticator.authenticate():                        self.authenticator = FabricAuthenticator()            self.logger.info("Step 1: Authenticating with Microsoft Fabric")            # Step 1: Authenticate        try:                self.logger.info(f"Starting complete Monitor Hub analysis for {days} days")        """            Dictionary with analysis results and report file paths        Returns:                        use_simulation: Whether to use simulation for demonstration            days: Number of days of historical data to analyze        Args:                Run complete Monitor Hub analysis pipeline        """    def run_complete_analysis(self, days: int = 90, use_simulation: bool = True) -> Dict[str, Any]:            self.logger.info("Monitor Hub Pipeline initialized")                self.reporter = None        self.extractor = None        self.authenticator = None        # Initialize components                self.output_directory.mkdir(parents=True, exist_ok=True)        self.output_directory = Path(output_directory)        self.logger = self._setup_logging()        """Initialize the pipeline"""    def __init__(self, output_directory: str = "exports/monitor_hub_analysis"):        """Complete Monitor Hub analysis pipeline"""class MonitorHubPipeline:from usf_fabric_monitoring.core.monitor_hub_reporter import MonitorHubCSVReporterfrom usf_fabric_monitoring.core.monitor_hub_extractor import MonitorHubExtractorfrom auth.fabric_authenticator import FabricAuthenticatorsys.path.insert(0, str(Path(__file__).parent / "src"))# Add src to path for importsimport jsonfrom typing import Dict, Any, Optionalfrom pathlib import Pathfrom datetime import datetimeimport argparseimport loggingimport sys"""    python monitor_hub_pipeline.py [--days 90] [--output-dir exports/]Usage:- Identification of constant failures and excess activity per User/Location/Domain- Comprehensive CSV reporting for performance optimization- Multi-dimensional analysis (Time/Date, Location, Domain, User, Item Type, Status)- Key measurables calculation (Activities, Failed Activity, Success %, Total Duration)- 90-day historical data extraction Key Features:in the statement of work.including 90-day historical extraction, analysis, and CSV report generation as specifiedThis script provides complete end-to-end analysis of Microsoft Fabric Monitor Hub data,
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
        
        # 7. Dimensional Analysis Report
        dimensional_file = self._generate_dimensional_analysis_report(analysis_results)
        report_files["dimensional_analysis"] = dimensional_file
        
        # 8. Recommendations Report
        recommendations_file = self._generate_recommendations_report(analysis_results)
        report_files["recommendations"] = recommendations_file
        
        # 9. Executive Summary (JSON for detailed insights)
        executive_file = self._generate_executive_summary(analysis_results)
        report_files["executive_summary"] = executive_file
        
        self.logger.info(f"Generated {len(report_files)} comprehensive reports")
        return report_files
    
    def _generate_activities_report(self, activities: List[Dict[str, Any]]) -> str:
        """Generate master activities report with all activity details"""
        if not activities:
            return self._create_empty_report("activities_master")
        
        df = pd.DataFrame(activities)
        
        # Add derived columns for analysis
        df["date"] = pd.to_datetime(df["start_time"]).dt.date
        df["hour"] = pd.to_datetime(df["start_time"]).dt.hour
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
            },
            {
                "metric": "Average Duration",
                "value": key_measurables["average_duration_seconds"],
                "unit": "seconds",
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
                "percentage_of_total_activities": stats["percentage_of_total"],
                "performance_rating": self._assess_domain_performance(stats)
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
        
        if failure_analysis["total_failures"] == 0:
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
        
        # Failure by type summary
        failure_by_type = failure_analysis.get("failure_by_type", {})
        for item_type, count in failure_by_type.items():
            failure_data.append({
                "item_id": f"SUMMARY_{item_type}",
                "item_name": f"All {item_type} Items",
                "item_type": item_type,
                "failure_count": count,
                "severity": "Summary"
            })
        
        df = pd.DataFrame(failure_data)
        df = df.sort_values("failure_count", ascending=False)
        
        filename = f"failure_analysis_{self.report_timestamp}.csv"
        filepath = self.export_directory / filename
        df.to_csv(filepath, index=False)
        
        self.logger.info(f"Generated failure analysis report: {filename}")
        return str(filepath)
    
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
                "average_duration_seconds": stats["avg_duration"],
                "activity_level": self._classify_activity_level(stats["total_activities"])
            })
        
        df = pd.DataFrame(trend_data)
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date", ascending=False)
        
        filename = f"daily_trends_analysis_{self.report_timestamp}.csv"
        filepath = self.export_directory / filename
        df.to_csv(filepath, index=False)
        
        self.logger.info(f"Generated daily trends report: {filename}")
        return str(filepath)
    
    def _generate_dimensional_analysis_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate comprehensive dimensional analysis report"""
        dimensional_analysis = analysis_results.get("dimensional_analysis", {})
        
        if not dimensional_analysis:
            return self._create_empty_report("dimensional_analysis")
        
        all_dimensional_data = []
        
        # Process each dimension
        for dimension_name, dimension_data in dimensional_analysis.items():
            details = dimension_data.get("details", {})
            
            for category, stats in details.items():
                all_dimensional_data.append({
                    "dimension": dimension_name,
                    "category": str(category),
                    "total_activities": stats["total_activities"],
                    "failed_activities": stats["failed_activities"],
                    "success_rate_percent": stats["success_rate_percent"],
                    "average_duration_seconds": stats["average_duration_seconds"],
                    "percentage_of_total": stats["percentage_of_total"],
                    "risk_assessment": self._assess_dimensional_risk(stats)
                })
        
        df = pd.DataFrame(all_dimensional_data)
        df = df.sort_values(["dimension", "total_activities"], ascending=[True, False])
        
        filename = f"dimensional_analysis_{self.report_timestamp}.csv"
        filepath = self.export_directory / filename
        df.to_csv(filepath, index=False)
        
        self.logger.info(f"Generated dimensional analysis report: {filename}")
        return str(filepath)
    
    def _generate_recommendations_report(self, analysis_results: Dict[str, Any]) -> str:
        """Generate actionable recommendations report"""
        recommendations = analysis_results.get("recommendations", [])
        
        if not recommendations:
            return self._create_empty_report("recommendations")
        
        rec_data = []
        for i, rec in enumerate(recommendations):
            rec_data.append({
                "recommendation_id": f"REC_{i+1:03d}",
                "category": rec["category"],
                "priority": rec["priority"],
                "title": rec["title"],
                "description": rec["description"],
                "action": rec["action"],
                "impact": rec["impact"],
                "estimated_effort": self._estimate_effort(rec["category"], rec["priority"])
            })
        
        df = pd.DataFrame(rec_data)
        
        # Sort by priority
        priority_order = {"high": 1, "medium": 2, "low": 3}
        df["priority_order"] = df["priority"].map(priority_order)
        df = df.sort_values("priority_order").drop("priority_order", axis=1)
        
        filename = f"recommendations_{self.report_timestamp}.csv"
        filepath = self.export_directory / filename
        df.to_csv(filepath, index=False)
        
        self.logger.info(f"Generated recommendations report: {filename}")
        return str(filepath)
    
    def _generate_executive_summary(self, analysis_results: Dict[str, Any]) -> str:
        """Generate executive summary in JSON format for detailed insights"""
        # Create executive summary with key insights
        executive_summary = {
            "report_metadata": {
                "generated_at": datetime.now().isoformat(),
                "report_period": analysis_results["analysis_metadata"]["period"],
                "total_activities_analyzed": analysis_results["analysis_metadata"]["total_activities_analyzed"]
            },
            "key_findings": {
                "overall_performance": analysis_results["key_measurables"],
                "critical_issues": len([r for r in analysis_results.get("recommendations", []) if r["priority"] == "high"]),
                "top_performing_domain": self._get_top_performing_domain(analysis_results),
                "most_active_user": self._get_most_active_user(analysis_results)
            },
            "trend_insights": analysis_results.get("trend_analysis", {}).get("trend_summary", {}),
            "recommendations_summary": {
                "total_recommendations": len(analysis_results.get("recommendations", [])),
                "high_priority": len([r for r in analysis_results.get("recommendations", []) if r["priority"] == "high"]),
                "medium_priority": len([r for r in analysis_results.get("recommendations", []) if r["priority"] == "medium"]),
                "low_priority": len([r for r in analysis_results.get("recommendations", []) if r["priority"] == "low"])
            }
        }
        
        filename = f"executive_summary_{self.report_timestamp}.json"
        filepath = self.export_directory / filename
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(executive_summary, f, indent=2, default=str)
        
        self.logger.info(f"Generated executive summary: {filename}")
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
    
    def _assess_domain_performance(self, stats: Dict[str, Any]) -> str:
        """Assess domain performance rating"""
        success_rate = stats["success_rate"]
        
        if success_rate >= 95:
            return "Excellent"
        elif success_rate >= 85:
            return "Good"
        elif success_rate >= 70:
            return "Fair"
        else:
            return "Poor"
    
    def _classify_activity_level(self, activity_count: int) -> str:
        """Classify daily activity level"""
        if activity_count > 50:
            return "High"
        elif activity_count > 20:
            return "Medium"
        elif activity_count > 0:
            return "Low"
        else:
            return "None"
    
    def _assess_dimensional_risk(self, stats: Dict[str, Any]) -> str:
        """Assess risk level for dimensional analysis"""
        success_rate = stats["success_rate_percent"]
        
        if success_rate < 70:
            return "High Risk"
        elif success_rate < 85:
            return "Medium Risk"
        else:
            return "Low Risk"
    
    def _estimate_effort(self, category: str, priority: str) -> str:
        """Estimate implementation effort"""
        effort_matrix = {
            ("reliability", "high"): "Medium",
            ("performance", "high"): "High", 
            ("quality", "high"): "Medium",
            ("capacity", "medium"): "Low",
            ("performance", "medium"): "Medium"
        }
        
        return effort_matrix.get((category, priority), "Medium")
    
    def _get_top_performing_domain(self, analysis_results: Dict[str, Any]) -> Optional[str]:
        """Get the top performing domain"""
        domain_analysis = analysis_results.get("domain_analysis", {})
        domain_comparison = domain_analysis.get("domain_comparison", {})
        best_performing = domain_comparison.get("best_performing", {})
        
        return best_performing.get("domain") if best_performing else None
    
    def _get_most_active_user(self, analysis_results: Dict[str, Any]) -> Optional[str]:
        """Get the most active user"""
        user_analysis = analysis_results.get("user_activity_analysis", {})
        top_users = user_analysis.get("top_users_by_activity", {})
        
        if top_users:
            # Get the user with highest activity
            most_active = max(top_users.items(), key=lambda x: x[1]["total_activities"])
            return most_active[0]
        
        return None