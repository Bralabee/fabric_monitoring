"""
Microsoft Fabric Monitor Hub Historical Analysis Extractor

⚠️ DEPRECATED: This module is deprecated and kept for backward compatibility only.
    Use FabricDataExtractor from core.extractor instead for new code.

This module mimics the Monitor Hub functionality by extracting and analyzing
historical data from Microsoft Fabric workspaces to produce the key measurables:
- Activities
- Failed Activity  
- Success %
- Total Duration

Dimensions for analysis:
- Time/Date
- Location
- Domain
- Submitted By (User)
- Created By/Last Updated By
- Item Type
- Status

Migration Guide:
    Old: from core.monitor_hub_extractor import MonitorHubExtractor
         extractor = MonitorHubExtractor(auth)
         data = extractor.extract_historical_data(days=7)
    
    New: from core.extractor import FabricDataExtractor
         extractor = FabricDataExtractor(auth)
         # Use extract_historical_data.py script or monitor_hub_pipeline.py
"""

import os
import logging
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

import pandas as pd
from requests.exceptions import RequestException
import random

from .auth import FabricAuthenticator


class MonitorHubExtractor:
    """Extracts and analyzes Fabric data to provide Monitor Hub-like insights"""
    
    def __init__(self, authenticator: FabricAuthenticator):
        """Initialize the Monitor Hub extractor"""
        self.auth = authenticator
        self.logger = logging.getLogger(__name__)
        
        # API configuration
        self.fabric_base_url = os.getenv("FABRIC_API_BASE_URL", "https://api.fabric.microsoft.com")
        self.powerbi_base_url = os.getenv("POWERBI_API_BASE_URL", "https://api.powerbi.com")
        self.api_version = "v1"
        
        # Analysis configuration
        self.max_workers = int(os.getenv("MAX_CONCURRENT_WORKERS", "10"))
        self.analysis_batch_size = int(os.getenv("ANALYSIS_BATCH_SIZE", "50"))
        
        # Activity simulation for items without direct activity APIs
        self.simulate_activities = os.getenv("SIMULATE_ACTIVITIES", "true").lower() == "true"
    
    def extract_historical_data(self, days: int = None, target_workspaces: Optional[List[str]] = None, use_simulation: bool = None) -> Dict[str, Any]:
        """
        Extract historical analysis data within API limits
        
        Args:
            days: Number of days to analyze (max 28 due to API limits)
            target_workspaces: Optional list of workspace IDs to analyze. If None, analyzes all accessible workspaces.
            use_simulation: Whether to use simulation mode (from env if not specified)
            
        Returns:
            Dictionary containing historical analysis data
        """
        # Load configuration from environment
        max_days = int(os.getenv('MAX_HISTORICAL_DAYS', '28'))
        default_days = int(os.getenv('DEFAULT_ANALYSIS_DAYS', '7'))
        
        # Validate and set analysis period
        if days is None:
            days = default_days
        
        if days > max_days:
            self.logger.warning(f"Requested {days} days exceeds API limit of {max_days} days. Adjusting to {max_days} days.")
            days = max_days
        
        # Set simulation mode from environment if not specified
        if use_simulation is None:
            use_simulation = os.getenv('ENABLE_SIMULATION', 'true').lower() == 'true'
        
        start_date = datetime.now(timezone.utc) - timedelta(days=days)
        end_date = datetime.now(timezone.utc)
        
        self.logger.info(f"Starting {days}-day historical analysis from {start_date} to {end_date} (simulation: {use_simulation})")
        
        # Get workspace inventory
        workspaces = self._get_workspace_inventory(target_workspaces)
        self.logger.info(f"Analyzing {len(workspaces)} workspaces")
        
        # Extract comprehensive workspace data
        historical_data = {
            "analysis_period": {
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "days": days,
                "description": f"{days}-day historical analysis period (API compliant)"
            },
            "workspaces": [],
            "items": [],
            "activities": [],
            "summary": {
                "total_workspaces": 0,
                "total_items": 0,
                "total_activities": 0,
                "failed_activities": 0,
                "success_rate_percent": 0.0,
                "total_duration_seconds": 0
            },
            "dimensions": {
                "by_date": {},
                "by_location": {},
                "by_domain": {},
                "by_user": {},
                "by_item_type": {},
                "by_status": {}
            }
        }
        
        # Process workspaces in parallel for performance
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_workspace = {
                executor.submit(self._analyze_workspace_comprehensive, ws): ws 
                for ws in workspaces
            }
            
            for future in as_completed(future_to_workspace):
                workspace = future_to_workspace[future]
                try:
                    workspace_data = future.result()
                    if workspace_data:
                        historical_data["workspaces"].append(workspace_data["workspace"])
                        historical_data["items"].extend(workspace_data["items"])
                        historical_data["activities"].extend(workspace_data["activities"])
                        
                except Exception as e:
                    self.logger.error(f"Failed to analyze workspace {workspace.get('displayName', 'Unknown')}: {e}")
        
        # Calculate summary metrics
        self._calculate_summary_metrics(historical_data)
        
        # Generate dimensional analysis
        self._generate_dimensional_analysis(historical_data)
        
        self.logger.info("90-day historical analysis completed")
        return historical_data
    
    def _get_workspace_inventory(self, target_workspaces: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Get inventory of workspaces to analyze"""
        try:
            token = self.auth.get_fabric_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            # Get all workspaces
            url = f"{self.fabric_base_url}/{self.api_version}/workspaces"
            response = self.auth._make_authenticated_request("GET", url, headers=headers)
            
            if response.status_code == 200:
                all_workspaces = response.json().get("value", [])
                
                # Filter workspaces if target list provided
                if target_workspaces:
                    workspaces = [ws for ws in all_workspaces if ws["id"] in target_workspaces]
                else:
                    workspaces = all_workspaces
                
                self.logger.info(f"Retrieved {len(workspaces)} workspaces for analysis")
                return workspaces
            else:
                self.logger.error(f"Failed to get workspaces: {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting workspace inventory: {e}")
            return []
    
    def _analyze_workspace_comprehensive(self, workspace: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Perform comprehensive analysis of a single workspace
        
        Returns:
            Dictionary containing workspace analysis data
        """
        workspace_id = workspace["id"]
        workspace_name = workspace["displayName"]
        
        self.logger.debug(f"Analyzing workspace: {workspace_name}")
        
        try:
            # Get workspace items
            items = self._get_workspace_items(workspace_id)
            
            # Enrich items with additional metadata
            enriched_items = []
            activities = []
            
            for item in items:
                enriched_item = self._enrich_item_metadata(workspace_id, item)
                enriched_items.append(enriched_item)
                
                # Extract/simulate activity data for this item
                item_activities = self._extract_item_activities(workspace_id, item, enriched_item)
                activities.extend(item_activities)
            
            # Add workspace-level metadata
            workspace_data = {
                **workspace,
                "capacity_id": workspace.get("capacityId"),
                "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
                "total_items": len(enriched_items),
                "total_activities": len(activities),
                "workspace_url": f"https://app.powerbi.com/groups/{workspace_id}",
                "domain": self._extract_domain_from_name(workspace_name)
            }
            
            return {
                "workspace": workspace_data,
                "items": enriched_items,
                "activities": activities
            }
            
        except Exception as e:
            self.logger.error(f"Failed to analyze workspace {workspace_name}: {e}")
            return None
    
    def _get_workspace_items(self, workspace_id: str) -> List[Dict[str, Any]]:
        """Get all items in a workspace"""
        try:
            token = self.auth.get_fabric_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            url = f"{self.fabric_base_url}/{self.api_version}/workspaces/{workspace_id}/items"
            response = self.auth._make_authenticated_request("GET", url, headers=headers)
            
            if response.status_code == 200:
                return response.json().get("value", [])
            else:
                self.logger.warning(f"Failed to get items for workspace {workspace_id}: {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.error(f"Error getting workspace items: {e}")
            return []
    
    def _enrich_item_metadata(self, workspace_id: str, item: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich item with additional metadata and URLs"""
        item_type = item.get("type", "Unknown")
        item_id = item.get("id")
        item_name = item.get("displayName", "Unknown")
        
        # Generate object URL based on item type
        object_url = self._generate_object_url(workspace_id, item_id, item_type)
        
        # Extract user information (if available)
        created_by = self._extract_user_info(item.get("createdBy", {}))
        modified_by = self._extract_user_info(item.get("modifiedBy", {}))
        
        # Calculate item domain
        domain = self._extract_domain_from_name(item_name)
        
        return {
            **item,
            "workspace_id": workspace_id,
            "object_url": object_url,
            "created_by_user": created_by,
            "modified_by_user": modified_by,
            "domain": domain,
            "analysis_timestamp": datetime.now(timezone.utc).isoformat(),
            "item_category": self._categorize_item_type(item_type)
        }
    
    def _extract_item_activities(self, workspace_id: str, item: Dict[str, Any], enriched_item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Extract or simulate activity data for an item
        
        Since we don't have direct access to activity logs, we'll:
        1. Try to get job instances for monitorable items
        2. Simulate activities based on item metadata for analysis framework
        """
        activities = []
        item_id = item.get("id")
        item_type = item.get("type")
        
        # Try to get real job instances for items that support monitoring
        real_activities = self._get_item_job_instances(workspace_id, item_id, item_type)
        if real_activities:
            activities.extend(real_activities)
        
        # If simulation is enabled and no real activities found, create simulated data
        if self.simulate_activities and not real_activities:
            simulated_activities = self._simulate_item_activities(enriched_item)
            activities.extend(simulated_activities)
        
        return activities
    
    def _get_item_job_instances(self, workspace_id: str, item_id: str, item_type: str) -> List[Dict[str, Any]]:
        """Try to get actual job instances for monitorable items"""
        # Items that typically have job monitoring capabilities
        monitorable_types = [
            "DataPipeline", "Notebook", "Lakehouse", "Warehouse", 
            "Dataflow", "MLExperiment", "SparkJobDefinition"
        ]
        
        if item_type not in monitorable_types:
            return []
        
        try:
            token = self.auth.get_fabric_token()
            headers = {
                "Authorization": f"Bearer {token}",
                "Content-Type": "application/json"
            }
            
            # Try to get job instances
            url = f"{self.fabric_base_url}/{self.api_version}/workspaces/{workspace_id}/items/{item_id}/jobs/instances"
            response = self.auth._make_authenticated_request("GET", url, headers=headers)
            
            if response.status_code == 200:
                job_instances = response.json().get("value", [])
                
                # Convert job instances to activity format
                activities = []
                for job in job_instances:
                    activity = self._convert_job_to_activity(workspace_id, item_id, item_type, job)
                    activities.append(activity)
                
                return activities
            else:
                self.logger.debug(f"No job instances for item {item_id}: {response.status_code}")
                return []
                
        except Exception as e:
            self.logger.debug(f"Could not get job instances for item {item_id}: {e}")
            return []
    
    def _simulate_item_activities(self, item: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Simulate activities for items to demonstrate the analysis framework
        
        This creates realistic-looking activity data based on item metadata
        for testing and framework demonstration purposes.
        """
        if not self.simulate_activities:
            return []
        
        activities = []
        item_id = item.get("id")
        item_type = item.get("type")
        item_name = item.get("displayName", "Unknown")
        workspace_id = item.get("workspace_id")
        
        # Generate different numbers of activities based on item type
        activity_counts = {
            "Report": 15,
            "Dashboard": 10,
            "SemanticModel": 8,
            "DataPipeline": 25,
            "Notebook": 20,
            "Lakehouse": 12,
            "Warehouse": 10,
            "Dataflow": 15,
            "MLExperiment": 5
        }
        
        num_activities = activity_counts.get(item_type, 5)
        
        # Generate activities over the past 90 days
        end_date = datetime.now(timezone.utc)
        
        for i in range(num_activities):
            # Distribute activities over 90 days
            days_ago = (i / num_activities) * 90
            activity_date = end_date - timedelta(days=days_ago)
            
            # Simulate success/failure (90% success rate)
            success = i < num_activities * 0.9
            status = "Success" if success else "Failed"
            
            # Simulate duration based on item type
            duration_ranges = {
                "DataPipeline": (30, 300),
                "Notebook": (10, 120),
                "Lakehouse": (5, 60),
                "SemanticModel": (15, 180),
                "Report": (2, 30),
                "Dashboard": (1, 15)
            }
            
            min_dur, max_dur = duration_ranges.get(item_type, (5, 60))
            duration = min_dur + (i % (max_dur - min_dur))
            
            activity = {
                "activity_id": f"sim_{item_id}_{i}",
                "workspace_id": workspace_id,
                "item_id": item_id,
                "item_name": item_name,
                "item_type": item_type,
                "activity_type": f"{item_type}Execution",
                "status": status,
                "start_time": activity_date.isoformat(),
                "end_time": (activity_date + timedelta(seconds=duration)).isoformat(),
                "duration_seconds": duration,
                "submitted_by": item.get("created_by_user", "system@fabric.com"),
                "created_by": item.get("created_by_user", "system@fabric.com"),
                "last_updated_by": item.get("modified_by_user", "system@fabric.com"),
                "domain": item.get("domain", "default"),
                "object_url": item.get("object_url", ""),
                "location": self._extract_location_from_workspace(workspace_id),
                "is_simulated": True
            }
            
            activities.append(activity)
        
        return activities
    
    def _convert_job_to_activity(self, workspace_id: str, item_id: str, item_type: str, job: Dict[str, Any]) -> Dict[str, Any]:
        """Convert a job instance to activity format"""
        start_time = job.get("startTimeUtc", datetime.now(timezone.utc).isoformat())
        end_time = job.get("endTimeUtc", datetime.now(timezone.utc).isoformat())
        
        # Calculate duration
        if start_time and end_time:
            try:
                start_dt = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                end_dt = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                duration = (end_dt - start_dt).total_seconds()
            except:
                duration = 0
        else:
            duration = 0
        
        return {
            "activity_id": job.get("id", f"job_{item_id}"),
            "workspace_id": workspace_id,
            "item_id": item_id,
            "item_name": job.get("itemDisplayName", "Unknown"),
            "item_type": item_type,
            "activity_type": f"{item_type}Job",
            "status": job.get("status", "Unknown"),
            "start_time": start_time,
            "end_time": end_time,
            "duration_seconds": duration,
            "submitted_by": job.get("invokeType", "system"),
            "created_by": "system",
            "last_updated_by": "system",
            "domain": "default",
            "object_url": "",
            "location": self._extract_location_from_workspace(workspace_id),
            "is_simulated": False
        }
    
    def _generate_object_url(self, workspace_id: str, item_id: str, item_type: str) -> str:
        """Generate the appropriate URL for the object based on its type"""
        base_url = "https://app.powerbi.com"
        
        url_patterns = {
            "Report": f"{base_url}/groups/{workspace_id}/reports/{item_id}",
            "Dashboard": f"{base_url}/groups/{workspace_id}/dashboards/{item_id}",
            "SemanticModel": f"{base_url}/groups/{workspace_id}/datasets/{item_id}",
            "Lakehouse": f"{base_url}/groups/{workspace_id}/lakehouses/{item_id}",
            "Warehouse": f"{base_url}/groups/{workspace_id}/warehouses/{item_id}",
            "DataPipeline": f"{base_url}/groups/{workspace_id}/datapipelines/{item_id}",
            "Notebook": f"{base_url}/groups/{workspace_id}/notebooks/{item_id}",
            "Dataflow": f"{base_url}/groups/{workspace_id}/dataflows/{item_id}"
        }
        
        return url_patterns.get(item_type, f"{base_url}/groups/{workspace_id}/items/{item_id}")
    
    def _extract_user_info(self, user_obj: Dict[str, Any]) -> str:
        """Extract user name from user object"""
        if not user_obj:
            return "Unknown"
        
        # Try different possible fields
        user_name = user_obj.get("displayName") or user_obj.get("userPrincipalName") or user_obj.get("email")
        
        if user_name and "@" in user_name:
            # Extract just the username part from email
            return user_name.split("@")[0]
        
        return user_name or "Unknown"
    
    def _extract_domain_from_name(self, name: str) -> str:
        """Extract domain/category from item or workspace name"""
        name_lower = name.lower()
        
        # Common domain patterns
        if any(x in name_lower for x in ["hr", "human", "resource"]):
            return "Human Resources"
        elif any(x in name_lower for x in ["finance", "financial", "budget"]):
            return "Finance"
        elif any(x in name_lower for x in ["sales", "crm", "customer"]):
            return "Sales"
        elif any(x in name_lower for x in ["ops", "operation", "admin"]):
            return "Operations"
        elif any(x in name_lower for x in ["it", "tech", "system"]):
            return "IT"
        elif any(x in name_lower for x in ["analytics", "bi", "data"]):
            return "Analytics"
        elif any(x in name_lower for x in ["test", "dev", "prod"]):
            return "Development"
        else:
            return "General"
    
    def _extract_location_from_workspace(self, workspace_id: str) -> str:
        """Extract location information - could be enhanced with actual location data"""
        # This is a placeholder - in reality, you might have workspace metadata 
        # that indicates geographic location or organizational unit
        return "Global"
    
    def _categorize_item_type(self, item_type: str) -> str:
        """Categorize item types into broader categories"""
        categories = {
            "Report": "Visualization",
            "Dashboard": "Visualization", 
            "PaginatedReport": "Visualization",
            "SemanticModel": "Data Model",
            "DataPipeline": "Data Processing",
            "Dataflow": "Data Processing",
            "Notebook": "Development",
            "SparkJobDefinition": "Development",
            "Lakehouse": "Storage",
            "Warehouse": "Storage",
            "KQLDatabase": "Storage",
            "MLExperiment": "Machine Learning",
            "MLModel": "Machine Learning",
            "Eventstream": "Real-time",
            "Eventhouse": "Real-time"
        }
        
        return categories.get(item_type, "Other")
    
    def _calculate_summary_metrics(self, historical_data: Dict[str, Any]):
        """Calculate overall summary metrics"""
        activities = historical_data["activities"]
        
        total_activities = len(activities)
        failed_activities = len([a for a in activities if a.get("status") == "Failed"])
        success_rate = ((total_activities - failed_activities) / total_activities * 100) if total_activities > 0 else 0.0
        total_duration = sum(a.get("duration_seconds", 0) for a in activities)
        
        historical_data["summary"].update({
            "total_workspaces": len(historical_data["workspaces"]),
            "total_items": len(historical_data["items"]),
            "total_activities": total_activities,
            "failed_activities": failed_activities,
            "success_rate_percent": round(success_rate, 2),
            "total_duration_seconds": total_duration
        })
    
    def _generate_dimensional_analysis(self, historical_data: Dict[str, Any]):
        """Generate analysis across all dimensions"""
        activities = historical_data["activities"]
        
        # Analysis by date
        by_date = {}
        for activity in activities:
            date = activity.get("start_time", "")[:10]  # YYYY-MM-DD
            if date not in by_date:
                by_date[date] = {"total": 0, "failed": 0, "duration": 0}
            
            by_date[date]["total"] += 1
            if activity.get("status") == "Failed":
                by_date[date]["failed"] += 1
            by_date[date]["duration"] += activity.get("duration_seconds", 0)
        
        # Calculate success rates for each date
        for date_data in by_date.values():
            if date_data["total"] > 0:
                date_data["success_rate"] = ((date_data["total"] - date_data["failed"]) / date_data["total"]) * 100
            else:
                date_data["success_rate"] = 0.0
        
        historical_data["dimensions"]["by_date"] = by_date
        
        # Analysis by other dimensions
        dimensions = ["domain", "submitted_by", "item_type", "status", "location"]
        
        for dimension in dimensions:
            dim_analysis = {}
            for activity in activities:
                key = activity.get(dimension, "Unknown")
                if key not in dim_analysis:
                    dim_analysis[key] = {"total": 0, "failed": 0, "duration": 0}
                
                dim_analysis[key]["total"] += 1
                if activity.get("status") == "Failed":
                    dim_analysis[key]["failed"] += 1
                dim_analysis[key]["duration"] += activity.get("duration_seconds", 0)
            
            # Calculate success rates
            for dim_data in dim_analysis.values():
                if dim_data["total"] > 0:
                    dim_data["success_rate"] = ((dim_data["total"] - dim_data["failed"]) / dim_data["total"]) * 100
                else:
                    dim_data["success_rate"] = 0.0
            
            historical_data["dimensions"][f"by_{dimension}"] = dim_analysis