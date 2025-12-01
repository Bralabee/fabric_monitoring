"""
Microsoft Fabric Historical Analysis Engine

This module provides comprehensive historical analysis capabilities for Microsoft Fabric
Monitor Hub data, generating insights across multiple dimensions as specified in the
statement of work:

Key Measurables:
- Activities
- Failed Activity  
- Success %
- Total Duration

Available Dimensions:
- Time/Date
- Location
- Domain 
- Submitted By (User)
- Created By/Last Updated By
- Item Type
- Status

Key Use Case: Analyze historical performance to identify constant failures,
excess activity per User/Location/Domain
"""

import logging
from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime, timedelta
import json
import pandas as pd
import numpy as np
from collections import defaultdict, Counter


class HistoricalAnalysisEngine:
    """Performs comprehensive historical analysis of Fabric Monitor Hub data"""
    
    def __init__(self):
        """Initialize the analysis engine"""
        self.logger = logging.getLogger(__name__)
        
        # Analysis thresholds (configurable)
        self.failure_threshold_percent = 20.0  # Consider >20% failure rate as problematic
        self.excess_activity_threshold = 50   # Activities per day threshold
        self.performance_degradation_threshold = 0.3  # 30% increase in duration
    
    def perform_comprehensive_analysis(self, historical_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform comprehensive historical analysis
        
        Args:
            historical_data: Data from MonitorHubExtractor
            
        Returns:
            Comprehensive analysis results
        """
        self.logger.info("Starting comprehensive historical analysis")
        
        activities_df = pd.DataFrame(historical_data["activities"])
        
        analysis_results = {
            "analysis_metadata": {
                "analysis_timestamp": datetime.now().isoformat(),
                "period": historical_data["analysis_period"],
                "total_activities_analyzed": len(activities_df)
            },
            "key_measurables": self._calculate_key_measurables(activities_df),
            "dimensional_analysis": self._perform_dimensional_analysis(activities_df),
            "trend_analysis": self._perform_trend_analysis(activities_df),
            "performance_insights": self._identify_performance_insights(activities_df),
            "failure_analysis": self._analyze_failures(activities_df),
            "user_activity_analysis": self._analyze_user_activity(activities_df),
            "domain_analysis": self._analyze_domain_performance(activities_df),
            "recommendations": []
        }
        
        # Generate actionable recommendations
        analysis_results["recommendations"] = self._generate_recommendations(analysis_results)
        
        self.logger.info("Historical analysis completed")
        return analysis_results
    
    def _calculate_key_measurables(self, activities_df: pd.DataFrame) -> Dict[str, Any]:
        """Calculate the core key measurables"""
        if activities_df.empty:
            return {
                "total_activities": 0,
                "failed_activities": 0,
                "success_rate_percent": 0.0,
                "total_duration_hours": 0.0,
                "average_duration_seconds": 0.0
            }
        
        total_activities = len(activities_df)
        failed_activities = len(activities_df[activities_df["status"] == "Failed"])
        success_rate = ((total_activities - failed_activities) / total_activities) * 100 if total_activities > 0 else 0.0
        
        total_duration_seconds = activities_df["duration_seconds"].sum()
        total_duration_hours = total_duration_seconds / 3600
        average_duration_seconds = activities_df["duration_seconds"].mean()
        
        return {
            "total_activities": total_activities,
            "failed_activities": failed_activities,
            "success_rate_percent": round(success_rate, 2),
            "total_duration_hours": round(total_duration_hours, 2),
            "average_duration_seconds": round(average_duration_seconds, 2)
        }
    
    def _perform_dimensional_analysis(self, activities_df: pd.DataFrame) -> Dict[str, Any]:
        """Perform analysis across all specified dimensions"""
        dimensional_analysis = {}
        
        # Define dimensions to analyze
        dimensions = {
            "time_date": "start_time",
            "location": "location", 
            "domain": "domain",
            "submitted_by": "submitted_by",
            "created_by": "created_by",
            "last_updated_by": "last_updated_by",
            "item_type": "item_type",
            "status": "status"
        }
        
        for dimension_name, column_name in dimensions.items():
            if column_name in activities_df.columns:
                dimension_analysis = self._analyze_dimension(activities_df, column_name, dimension_name)
                dimensional_analysis[dimension_name] = dimension_analysis
        
        return dimensional_analysis
    
    def _analyze_dimension(self, activities_df: pd.DataFrame, column_name: str, dimension_name: str) -> Dict[str, Any]:
        """Analyze activities across a specific dimension"""
        if column_name == "start_time":
            # Special handling for time dimension
            activities_df["date"] = pd.to_datetime(activities_df["start_time"]).dt.date
            grouped = activities_df.groupby("date")
        else:
            grouped = activities_df.groupby(column_name)
        
        dimension_stats = {}
        
        for group_key, group_data in grouped:
            total_activities = len(group_data)
            failed_activities = len(group_data[group_data["status"] == "Failed"])
            success_rate = ((total_activities - failed_activities) / total_activities) * 100 if total_activities > 0 else 0.0
            total_duration = group_data["duration_seconds"].sum()
            avg_duration = group_data["duration_seconds"].mean()
            
            dimension_stats[str(group_key)] = {
                "total_activities": total_activities,
                "failed_activities": failed_activities, 
                "success_rate_percent": round(success_rate, 2),
                "total_duration_seconds": round(total_duration, 2),
                "average_duration_seconds": round(avg_duration, 2),
                "percentage_of_total": round((total_activities / len(activities_df)) * 100, 2)
            }
        
        # Sort by total activities descending
        sorted_stats = dict(sorted(dimension_stats.items(), 
                                 key=lambda x: x[1]["total_activities"], 
                                 reverse=True))
        
        return {
            "summary": {
                "total_groups": len(sorted_stats),
                "most_active": max(sorted_stats.items(), key=lambda x: x[1]["total_activities"])[0] if sorted_stats else "None",
                "least_reliable": min(sorted_stats.items(), key=lambda x: x[1]["success_rate_percent"])[0] if sorted_stats else "None"
            },
            "details": sorted_stats
        }
    
    def _perform_trend_analysis(self, activities_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze trends over time"""
        if activities_df.empty:
            return {"daily_trends": {}, "weekly_trends": {}}
        
        # Convert start_time to datetime
        activities_df["datetime"] = pd.to_datetime(activities_df["start_time"])
        activities_df["date"] = activities_df["datetime"].dt.date
        activities_df["week"] = activities_df["datetime"].dt.isocalendar().week
        
        # Daily trends
        daily_stats = activities_df.groupby("date").agg({
            "activity_id": "count",
            "status": lambda x: (x == "Failed").sum(),
            "duration_seconds": ["sum", "mean"]
        }).round(2)
        
        daily_stats.columns = ["total_activities", "failed_activities", "total_duration", "avg_duration"]
        daily_stats["success_rate"] = ((daily_stats["total_activities"] - daily_stats["failed_activities"]) / daily_stats["total_activities"] * 100).round(2)
        
        # Weekly trends
        weekly_stats = activities_df.groupby("week").agg({
            "activity_id": "count",
            "status": lambda x: (x == "Failed").sum(), 
            "duration_seconds": ["sum", "mean"]
        }).round(2)
        
        weekly_stats.columns = ["total_activities", "failed_activities", "total_duration", "avg_duration"]
        weekly_stats["success_rate"] = ((weekly_stats["total_activities"] - weekly_stats["failed_activities"]) / weekly_stats["total_activities"] * 100).round(2)
        
        return {
            "daily_trends": daily_stats.to_dict(orient="index"),
            "weekly_trends": weekly_stats.to_dict(orient="index"),
            "trend_summary": {
                "most_active_day": daily_stats["total_activities"].idxmax() if not daily_stats.empty else None,
                "least_reliable_day": daily_stats["success_rate"].idxmin() if not daily_stats.empty else None,
                "average_daily_activities": round(daily_stats["total_activities"].mean(), 2),
                "average_daily_success_rate": round(daily_stats["success_rate"].mean(), 2)
            }
        }
    
    def _identify_performance_insights(self, activities_df: pd.DataFrame) -> Dict[str, Any]:
        """Identify key performance insights and anomalies"""
        insights = {
            "performance_issues": [],
            "positive_trends": [],
            "anomalies": [],
            "capacity_insights": []
        }
        
        if activities_df.empty:
            return insights
        
        # Identify items with high failure rates
        item_failure_rates = activities_df.groupby("item_id").agg({
            "status": ["count", lambda x: (x == "Failed").sum()]
        })
        item_failure_rates.columns = ["total", "failed"]
        item_failure_rates["failure_rate"] = (item_failure_rates["failed"] / item_failure_rates["total"] * 100).round(2)
        
        high_failure_items = item_failure_rates[item_failure_rates["failure_rate"] > self.failure_threshold_percent]
        
        for item_id, stats in high_failure_items.iterrows():
            item_info = activities_df[activities_df["item_id"] == item_id].iloc[0]
            insights["performance_issues"].append({
                "type": "high_failure_rate",
                "item_id": item_id,
                "item_name": item_info["item_name"],
                "item_type": item_info["item_type"],
                "failure_rate_percent": stats["failure_rate"],
                "total_attempts": stats["total"],
                "recommendation": "Investigate and fix recurring issues"
            })
        
        # Identify users with excessive activity
        user_activity = activities_df.groupby("submitted_by")["activity_id"].count()
        high_activity_users = user_activity[user_activity > self.excess_activity_threshold]
        
        for user, activity_count in high_activity_users.items():
            insights["capacity_insights"].append({
                "type": "high_user_activity",
                "user": user,
                "total_activities": activity_count,
                "daily_average": round(activity_count / 90, 2),
                "recommendation": "Review user workload and optimize processes"
            })
        
        # Identify domains with performance issues
        domain_performance = activities_df.groupby("domain").agg({
            "status": ["count", lambda x: (x == "Failed").sum()],
            "duration_seconds": "mean"
        })
        domain_performance.columns = ["total", "failed", "avg_duration"]
        domain_performance["failure_rate"] = (domain_performance["failed"] / domain_performance["total"] * 100).round(2)
        
        problematic_domains = domain_performance[domain_performance["failure_rate"] > self.failure_threshold_percent]
        
        for domain, stats in problematic_domains.iterrows():
            insights["performance_issues"].append({
                "type": "domain_performance_issue",
                "domain": domain,
                "failure_rate_percent": stats["failure_rate"],
                "total_activities": stats["total"],
                "average_duration": round(stats["avg_duration"], 2),
                "recommendation": f"Review {domain} domain processes and data quality"
            })
        
        return insights
    
    def _analyze_failures(self, activities_df: pd.DataFrame) -> Dict[str, Any]:
        """Detailed analysis of failures"""
        failure_analysis = {
            "total_failures": 0,
            "failure_patterns": {},
            "failure_trends": {},
            "top_failing_items": [],
            "failure_by_type": {}
        }
        
        if activities_df.empty:
            return failure_analysis
        
        failed_activities = activities_df[activities_df["status"] == "Failed"]
        failure_analysis["total_failures"] = len(failed_activities)
        
        if failed_activities.empty:
            return failure_analysis
        
        # Failure patterns by item type
        failure_by_type = failed_activities["item_type"].value_counts().to_dict()
        failure_analysis["failure_by_type"] = failure_by_type
        
        # Top failing items
        failing_items = failed_activities.groupby(["item_id", "item_name", "item_type"]).size().reset_index(name="failure_count")
        failing_items = failing_items.sort_values("failure_count", ascending=False).head(10)
        
        failure_analysis["top_failing_items"] = failing_items.to_dict(orient="records")
        
        # Failure trends over time
        failed_activities["date"] = pd.to_datetime(failed_activities["start_time"]).dt.date
        daily_failures = failed_activities.groupby("date").size().to_dict()
        failure_analysis["failure_trends"] = {str(k): v for k, v in daily_failures.items()}
        
        return failure_analysis
    
    def _analyze_user_activity(self, activities_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze activity patterns by user"""
        user_analysis = {
            "top_users_by_activity": {},
            "user_success_rates": {},
            "user_performance_summary": {}
        }
        
        if activities_df.empty:
            return user_analysis
        
        # User activity volume
        user_activity_volume = activities_df.groupby("submitted_by").agg({
            "activity_id": "count",
            "status": lambda x: (x == "Failed").sum(),
            "duration_seconds": ["sum", "mean"]
        }).round(2)
        
        user_activity_volume.columns = ["total_activities", "failed_activities", "total_duration", "avg_duration"]
        user_activity_volume["success_rate"] = ((user_activity_volume["total_activities"] - user_activity_volume["failed_activities"]) / user_activity_volume["total_activities"] * 100).round(2)
        
        # Sort by activity volume
        top_users = user_activity_volume.sort_values("total_activities", ascending=False).head(10)
        user_analysis["top_users_by_activity"] = top_users.to_dict(orient="index")
        
        # Users with low success rates
        low_success_users = user_activity_volume[user_activity_volume["success_rate"] < 80].sort_values("success_rate")
        user_analysis["user_success_rates"] = low_success_users.to_dict(orient="index")
        
        return user_analysis
    
    def _analyze_domain_performance(self, activities_df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze performance across different domains"""
        domain_analysis = {
            "domain_summary": {},
            "domain_comparison": {},
            "domain_trends": {}
        }
        
        if activities_df.empty:
            return domain_analysis
        
        # Domain performance summary
        domain_performance = activities_df.groupby("domain").agg({
            "activity_id": "count",
            "status": lambda x: (x == "Failed").sum(),
            "duration_seconds": ["sum", "mean", "std"]
        }).round(2)
        
        domain_performance.columns = ["total_activities", "failed_activities", "total_duration", "avg_duration", "duration_std"]
        domain_performance["success_rate"] = ((domain_performance["total_activities"] - domain_performance["failed_activities"]) / domain_performance["total_activities"] * 100).round(2)
        domain_performance["percentage_of_total"] = (domain_performance["total_activities"] / len(activities_df) * 100).round(2)
        
        domain_analysis["domain_summary"] = domain_performance.to_dict(orient="index")
        
        # Domain comparison metrics
        best_performing_domain = domain_performance.loc[domain_performance["success_rate"].idxmax()] if not domain_performance.empty else None
        worst_performing_domain = domain_performance.loc[domain_performance["success_rate"].idxmin()] if not domain_performance.empty else None
        
        if best_performing_domain is not None:
            domain_analysis["domain_comparison"] = {
                "best_performing": {
                    "domain": best_performing_domain.name,
                    "success_rate": best_performing_domain["success_rate"],
                    "total_activities": best_performing_domain["total_activities"]
                },
                "worst_performing": {
                    "domain": worst_performing_domain.name,
                    "success_rate": worst_performing_domain["success_rate"],
                    "total_activities": worst_performing_domain["total_activities"]
                }
            }
        
        return domain_analysis
    
    def _generate_recommendations(self, analysis_results: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Generate actionable recommendations based on analysis"""
        recommendations = []
        
        # Analyze key measurables for recommendations
        key_measurables = analysis_results["key_measurables"]
        success_rate = key_measurables["success_rate_percent"]
        
        if success_rate < 85:
            recommendations.append({
                "category": "reliability",
                "priority": "high",
                "title": "Low Overall Success Rate",
                "description": f"System success rate is {success_rate}%, below recommended 85%",
                "action": "Investigate top failing items and implement fixes",
                "impact": "Improve system reliability and user experience"
            })
        
        # Performance insights recommendations
        performance_issues = analysis_results["performance_insights"]["performance_issues"]
        if performance_issues:
            recommendations.append({
                "category": "performance",
                "priority": "medium",
                "title": "Performance Issues Detected",
                "description": f"Found {len(performance_issues)} items/domains with performance issues",
                "action": "Review and optimize identified problematic items",
                "impact": "Reduce failures and improve execution times"
            })
        
        # User activity recommendations
        user_analysis = analysis_results.get("user_activity_analysis", {})
        top_users = user_analysis.get("top_users_by_activity", {})
        
        if top_users:
            high_activity_users = [user for user, stats in top_users.items() if stats["total_activities"] > self.excess_activity_threshold]
            if high_activity_users:
                recommendations.append({
                    "category": "capacity",
                    "priority": "medium", 
                    "title": "High User Activity Detected",
                    "description": f"Users {high_activity_users[:3]} show excessive activity patterns",
                    "action": "Review user workloads and identify automation opportunities",
                    "impact": "Optimize resource utilization and reduce manual overhead"
                })
        
        # Domain-specific recommendations
        domain_analysis = analysis_results.get("domain_analysis", {})
        domain_summary = domain_analysis.get("domain_summary", {})
        
        problematic_domains = [domain for domain, stats in domain_summary.items() if stats["success_rate"] < 80]
        if problematic_domains:
            recommendations.append({
                "category": "quality",
                "priority": "high",
                "title": "Domain Quality Issues",
                "description": f"Domains {problematic_domains} have low success rates",
                "action": "Implement domain-specific quality controls and monitoring",
                "impact": "Improve data quality and process reliability"
            })
        
        return recommendations