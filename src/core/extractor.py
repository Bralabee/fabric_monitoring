"""
Microsoft Fabric Data Extraction Module

This module handles data extraction from Microsoft Fabric APIs including:
- Fabric Monitor Hub activities
- Power BI activity events  
- Workspace items and metadata
- Activity logs and metrics
"""

import os
import logging
import time
from typing import List, Dict, Any, Optional, Union
from datetime import datetime, timedelta, timezone
import json
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .auth import FabricAuthenticator
from .enrichment import (
    build_object_url,
    compute_duration_seconds,
    extract_user_from_metadata,
    infer_domain,
    infer_location,
    infer_location,
    normalize_user,
    normalize_status,
)


class FabricDataExtractor:
    """Extracts monitoring data from Microsoft Fabric and Power BI APIs"""
    
    def __init__(self, authenticator: FabricAuthenticator):
        """
        Initialize the data extractor.
        
        Args:
            authenticator: Configured FabricAuthenticator instance
        """
        self.auth = authenticator
        self.logger = logging.getLogger(__name__)
        
        # API base URLs
        self.fabric_base_url = os.getenv("FABRIC_API_BASE_URL", "https://api.fabric.microsoft.com")
        self.powerbi_base_url = os.getenv("POWERBI_API_BASE_URL", "https://api.powerbi.com")
        self.api_version = os.getenv("FABRIC_API_VERSION", "v1")
        
        # Configure requests session with retry strategy
        self.session = requests.Session()
        retry_strategy = Retry(
            total=int(os.getenv("MAX_RETRIES", "3")),
            backoff_factor=int(os.getenv("RETRY_BACKOFF_FACTOR", "2")),
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Request timeout
        self.timeout = int(os.getenv("API_REQUEST_TIMEOUT", "30"))

        # Simple in-memory caches for workspace metadata
        self._workspace_items_cache: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._workspace_lookup: Dict[str, Dict[str, Any]] = {}
        self._cached_tenant_workspaces: Optional[List[Dict[str, Any]]] = None
    
    def get_workspaces(self, tenant_wide: bool = False, exclude_personal: bool = True) -> List[Dict[str, Any]]:
        """
        Get list of workspaces.
        
        Args:
            tenant_wide: If True, uses Power BI Admin API to fetch ALL tenant workspaces.
                        If False, uses member-only /v1/workspaces endpoint (legacy behavior).
            exclude_personal: If True, filters out personal workspaces (type=PersonalGroup).
                             Only applies when tenant_wide=True.
        
        Returns:
            List of workspace dictionaries with id, name, and metadata.
            - tenant_wide=False: ~139 workspaces (member-only)
            - tenant_wide=True: ~286+ workspaces (all shared tenant workspaces)
        """
        if tenant_wide:
            return self._get_workspaces_tenant_wide(exclude_personal=exclude_personal)
        else:
            return self._get_workspaces_member_only()
    
    def _get_workspaces_tenant_wide(self, exclude_personal: bool = True) -> List[Dict[str, Any]]:
        """
        Fetch ALL tenant workspaces using Power BI Admin API.
        
        This returns the complete workspace inventory across the entire tenant,
        not just workspaces where the service principal is a member.
        """
        try:
            workspaces = []
            skip = 0
            page_size = 200
            
            filter_clause = "$filter=type ne 'PersonalGroup'" if exclude_personal else ""
            filter_param = f"&{filter_clause}" if filter_clause else ""
            
            self.logger.info("Fetching tenant-wide workspaces via Power BI Admin API")
            
            while True:
                url = f"{self.powerbi_base_url}/v1.0/myorg/admin/groups?$top={page_size}&$skip={skip}{filter_param}"
                headers = self.auth.get_powerbi_headers()
                
                self.logger.info(f"Fetching page at skip={skip}...")
                
                # Retry loop for 429s
                while True:
                    response = self.session.get(url, headers=headers, timeout=self.timeout)
                    
                    if response.status_code == 429:
                        retry_after = int(response.headers.get('Retry-After', 60))
                        self.logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                        print(f"   ⏳ Rate limited. Waiting {retry_after}s...", end='\r', flush=True)
                        time.sleep(retry_after + 1)
                        continue
                    
                    break
                
                response.raise_for_status()
                data = response.json()
                items = data.get("value", [])
                
                if not items:
                    break
                
                workspaces.extend(items)
                self.logger.info(f"Retrieved {len(items)} workspaces (total: {len(workspaces)})")
                print(f"   ⏳ Retrieved {len(workspaces)} workspaces...", end='\r', flush=True)
                
                if len(items) < page_size:
                    break
                
                skip += page_size
            
            print(f"   ✅ Retrieved {len(workspaces)} workspaces total.          ")
            self.logger.info(f"Total tenant-wide workspaces retrieved: {len(workspaces)}")
            return workspaces
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch tenant-wide workspaces: {str(e)}")
            raise
    
    def _get_workspaces_member_only(self) -> List[Dict[str, Any]]:
        """
        Fetch only workspaces where the service principal is a member.
        
        This is the legacy behavior using /v1/workspaces endpoint.
        Returns ~139 workspaces (member-only, not tenant-wide).
        """
        try:
            url = f"{self.fabric_base_url}/{self.api_version}/workspaces"
            headers = self.auth.get_fabric_headers()
            
            self.logger.info("Fetching member workspaces (legacy behavior)")
            response = self.session.get(url, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            workspaces = data.get("value", [])
            
            self.logger.info(f"Retrieved {len(workspaces)} member workspaces")
            return workspaces
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch workspaces: {str(e)}")
            raise
    
    def get_workspace_activities(self, workspace_id: str, start_date: datetime, 
                               end_date: datetime, activity_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get activities for a specific workspace within date range.
        
        Args:
            workspace_id: Fabric workspace ID
            start_date: Start date for activity query
            end_date: End date for activity query  
            activity_types: Optional list of activity types to filter
            
        Returns:
            List of activity dictionaries
        """
        try:
            # Format dates for API (ISO 8601 format)
            start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            
            url = f"{self.fabric_base_url}/{self.api_version}/workspaces/{workspace_id}/activities"
            headers = self.auth.get_fabric_headers()
            
            params = {
                "startDateTime": start_str,
                "endDateTime": end_str
            }
            
            if activity_types:
                params["activityTypes"] = ",".join(activity_types)
            
            self.logger.info(f"Fetching activities for workspace {workspace_id} from {start_str} to {end_str}")
            
            # Retry loop for 429s
            while True:
                response = self.session.get(url, headers=headers, params=params, timeout=self.timeout)
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after + 1)
                    continue
                
                break
            
            if response.status_code == 404:
                self.logger.warning(f"Activities endpoint not found for workspace {workspace_id} - using Power BI API fallback")
                return self._get_powerbi_activities_fallback(workspace_id, start_date, end_date, activity_types)
            
            response.raise_for_status()
            data = response.json()
            activities = data.get("value", [])
            
            self.logger.info(f"Retrieved {len(activities)} activities for workspace {workspace_id}")
            return activities
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch activities for workspace {workspace_id}: {str(e)}")
            # Try Power BI API as fallback
            return self._get_powerbi_activities_fallback(workspace_id, start_date, end_date, activity_types)
    
    def _get_powerbi_activities_fallback(self, workspace_id: str, start_date: datetime, 
                                       end_date: datetime, activity_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Fallback method to get activities using Power BI Admin API.
        """
        try:
            # Power BI Admin API for activity events requires literal single quotes
            start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
            end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')
            
            base_url = f"{self.powerbi_base_url}/v1.0/myorg/admin/activityevents"
            query = f"startDateTime='{start_str}'&endDateTime='{end_str}'"
            url = f"{base_url}?{query}"
            
            headers = self.auth.get_powerbi_headers()
            
            self.logger.info(f"Using Power BI Admin API fallback for workspace {workspace_id}")
            
            # Use PreparedRequest to prevent requests from encoding the URL
            req = requests.Request('GET', url, headers=headers)
            prepped = self.session.prepare_request(req)
            prepped.url = url # Force the URL to be exactly as we constructed it
            
            # Retry loop for 429s
            while True:
                response = self.session.send(prepped, timeout=self.timeout)
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 60))
                    self.logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after + 1)
                    continue
                
                break
            
            response.raise_for_status()
            
            data = response.json()
            all_activities = data.get("activityEventEntities", [])
            
            # Filter by workspace if specified
            if workspace_id:
                workspace_activities = [
                    activity for activity in all_activities 
                    if activity.get("WorkspaceId") == workspace_id
                ]
            else:
                workspace_activities = all_activities
            
            # Filter by activity types if specified
            if activity_types:
                workspace_activities = [
                    activity for activity in workspace_activities
                    if activity.get("Activity") in activity_types
                ]
            
            self.logger.info(f"Retrieved {len(workspace_activities)} activities via Power BI API")
            return workspace_activities
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Power BI API fallback also failed: {str(e)}")
            return []
    
    def get_tenant_wide_activities(self, start_date: datetime, end_date: datetime, 
                                  workspace_ids: Optional[List[str]] = None,
                                  activity_types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """
        Get ALL tenant activities using Power BI Admin Activity API (single call, no per-workspace loop).
        
        Args:
            start_date: Start datetime for activity query
            end_date: End datetime for activity query
            workspace_ids: Optional list of workspace IDs to filter (applied post-fetch)
            activity_types: Optional list of activity types to filter (applied post-fetch)
            
        Returns:
            List of all tenant activities for the date range
        """
        try:
            # Power BI Admin API requires literal single quotes in the URL
            # We must manually construct the URL and bypass requests' automatic encoding
            start_str = start_date.strftime('%Y-%m-%dT%H:%M:%S')
            end_str = end_date.strftime('%Y-%m-%dT%H:%M:%S')
            
            # Base URL without query params
            base_url = f"{self.powerbi_base_url}/v1.0/myorg/admin/activityevents"
            
            # Construct query string with literal quotes
            query = f"startDateTime='{start_str}'&endDateTime='{end_str}'"
            url = f"{base_url}?{query}"
            
            headers = self.auth.get_powerbi_headers()
            
            self.logger.info(f"Fetching tenant-wide activities from {start_date.strftime('%Y-%m-%d %H:%M:%S')} to {end_date.strftime('%Y-%m-%d %H:%M:%S')}")
            
            all_activities = []
            
            while url:
                # Use PreparedRequest to prevent requests from encoding the URL
                req = requests.Request('GET', url, headers=headers)
                prepped = self.session.prepare_request(req)
                prepped.url = url # Force the URL to be exactly as we constructed it
                
                # Retry loop for 429s
                while True:
                    response = self.session.send(prepped, timeout=self.timeout)
                    
                    if response.status_code == 429:
                        retry_after = int(response.headers.get('Retry-After', 60))
                        self.logger.warning(f"Rate limited. Waiting {retry_after} seconds...")
                        print(f"   ⏳ Rate limited. Waiting {retry_after}s...", end='\r', flush=True)
                        time.sleep(retry_after + 1)
                        continue
                    
                    break
                
                response.raise_for_status()
                data = response.json()
                items = data.get("activityEventEntities", [])
                all_activities.extend(items)
                
                self.logger.info(f"Retrieved {len(items)} activities (Total: {len(all_activities)})")
                print(f"   ⏳ Fetched {len(all_activities)} activities...", end='\r', flush=True)
                
                # Check for continuation URI
                url = data.get("continuationUri")
                # continuationUri is already a full URL, we'll use it as is in the next loop
                # and the same prepped.url override will ensure it's not re-encoded
            
            print(f"   ✅ Fetched {len(all_activities)} activities total.          ")
            self.logger.info(f"Retrieved {len(all_activities)} tenant-wide activities")
            
            # Filter by workspace IDs if specified
            if workspace_ids:
                all_activities = [
                    activity for activity in all_activities
                    if activity.get("WorkspaceId") in workspace_ids
                ]
                self.logger.info(f"Filtered to {len(all_activities)} activities for specified workspaces")
            
            # Filter by activity types if specified
            if activity_types:
                all_activities = [
                    activity for activity in all_activities
                    if activity.get("Activity") in activity_types
                ]
                self.logger.info(f"Filtered to {len(all_activities)} activities for specified types")
            
            return all_activities
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch tenant-wide activities: {str(e)}")
            raise
    
    def get_daily_activities(self, date: datetime, workspace_ids: Optional[List[str]] = None, 
                           activity_types: Optional[List[str]] = None, tenant_wide: bool = True) -> List[Dict[str, Any]]:
        """
        Get all activities for a specific date.
        
        Args:
            date: Date to fetch activities for
            workspace_ids: Optional list of workspace IDs to filter
            activity_types: Optional list of activity types to filter
            tenant_wide: If True, uses Power BI Admin API (all tenant activities in one call).
                        If False, uses per-workspace API (only member workspaces).
            
        Returns:
            List of all activities for the specified date
        """
        # Set up date range (full day)
        start_date = date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_date = start_date + timedelta(days=1) - timedelta(microseconds=1)
        
        if tenant_wide:
            # Use Power BI Admin API to get ALL tenant activities in a single call
            self.logger.info(f"Using tenant-wide Power BI Admin API for {date.strftime('%Y-%m-%d')}")
            
            # Fetch all tenant activities at once
            all_activities = self.get_tenant_wide_activities(
                start_date=start_date,
                end_date=end_date,
                workspace_ids=workspace_ids,
                activity_types=activity_types
            )
            
            # Get workspace metadata for enrichment
            if not self._workspace_lookup:
                self.logger.info("Populating workspace lookup cache...")
                workspaces = self.get_workspaces(tenant_wide=True, exclude_personal=True)
                self._workspace_lookup = {ws.get("id"): ws for ws in workspaces if ws.get("id")}
            
            # Enrich activities with workspace and item metadata
            enriched_activities = []
            for activity in all_activities:
                workspace_id = activity.get("WorkspaceId")
                if not workspace_id:
                    continue
                
                workspace_info = self._workspace_lookup.get(workspace_id, {"id": workspace_id, "name": "Unknown"})
                enriched = self._enrich_activity(activity, workspace_id, workspace_info)
                
                # Try to attach item metadata (optional, may fail for some workspaces)
                try:
                    item_lookup = self._get_workspace_items_lookup(workspace_id)
                    if item_lookup:
                        self._attach_item_metadata(enriched, workspace_id, item_lookup)
                except Exception:
                    pass  # Skip item metadata if unavailable
                
                enriched_activities.append(enriched)
            
            self.logger.info(f"Enriched {len(enriched_activities)} activities for {date.strftime('%Y-%m-%d')}")
            return enriched_activities
            
        else:
            # Legacy per-workspace loop (member workspaces only)
            self.logger.info(f"Using per-workspace API for member workspaces on {date.strftime('%Y-%m-%d')}")
            
            all_activities = []
            target_workspaces = self.get_workspaces(tenant_wide=False, exclude_personal=True)
            self._workspace_lookup = {ws.get("id"): ws for ws in target_workspaces if ws.get("id")}
            
            self.logger.info(f"Fetching activities for {len(target_workspaces)} member workspaces")
            
            for workspace in target_workspaces:
                workspace_id = workspace.get("id")
                if not workspace_id:
                    continue
                    
                try:
                    activities = self.get_workspace_activities(
                        workspace_id=workspace_id,
                        start_date=start_date,
                        end_date=end_date,
                        activity_types=activity_types
                    )
                    
                    item_lookup = self._get_workspace_items_lookup(workspace_id)

                    for activity in activities:
                        enriched = self._enrich_activity(activity, workspace_id, workspace)
                        if item_lookup:
                            self._attach_item_metadata(enriched, workspace_id, item_lookup)
                        all_activities.append(enriched)
                    
                except Exception as e:
                    self.logger.warning(f"Failed to fetch activities for workspace {workspace_id}: {str(e)}")
                    continue
            
            self.logger.info(f"Total activities retrieved for {date.strftime('%Y-%m-%d')}: {len(all_activities)}")
            return all_activities
    
    def get_workspace_items(self, workspace_id: str) -> List[Dict[str, Any]]:
        """
        Get all items (reports, datasets, etc.) in a workspace.
        
        Args:
            workspace_id: Fabric workspace ID
            
        Returns:
            List of workspace items
        """
        try:
            url = f"{self.fabric_base_url}/{self.api_version}/workspaces/{workspace_id}/items"
            headers = self.auth.get_fabric_headers()
            
            self.logger.info(f"Fetching items for workspace {workspace_id}")
            response = self.session.get(url, headers=headers, timeout=self.timeout)
            response.raise_for_status()
            
            data = response.json()
            items = data.get("value", [])
            
            self.logger.info(f"Retrieved {len(items)} items for workspace {workspace_id}")
            return items
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch items for workspace {workspace_id}: {str(e)}")
            return []

    # ------------------------------------------------------------------
    # Enrichment helpers
    # ------------------------------------------------------------------

    def _get_workspace_items_lookup(self, workspace_id: str) -> Dict[str, Dict[str, Any]]:
        """Return (and cache) workspace items keyed by item id."""
        if workspace_id in self._workspace_items_cache:
            return self._workspace_items_cache[workspace_id]

        try:
            items = self.get_workspace_items(workspace_id)
            lookup = {item.get("id"): item for item in items if item.get("id")}
            self._workspace_items_cache[workspace_id] = lookup
            return lookup
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.warning(f"Unable to cache workspace items for {workspace_id}: {exc}")
            self._workspace_items_cache[workspace_id] = {}
            return {}

    def _enrich_activity(self, activity: Dict[str, Any], workspace_id: str, workspace: Dict[str, Any]) -> Dict[str, Any]:
        """Attach workspace context, derived fields, and normalized values."""
        workspace_name = workspace.get("displayName") or workspace.get("name") or "Unknown"
        activity["_workspace_id"] = workspace_id
        activity["_workspace_name"] = workspace_name
        activity["_extraction_date"] = datetime.now().isoformat()
        activity.setdefault("WorkspaceId", workspace_id)
        activity.setdefault("WorkspaceName", workspace_name)

        # Derived identifiers and names
        item_id = self._extract_item_id(activity)
        if item_id:
            activity.setdefault("ItemId", item_id)

        if not activity.get("ItemName"):
            for key in ["ArtifactName", "ObjectName", "Name", "DatasetName", "ReportName"]:
                value = activity.get(key)
                if value:
                    activity["ItemName"] = value
                    break

        submitted_by = normalize_user(activity.get("SubmittedBy") or activity.get("UserId") or activity.get("UserKey"))
        if submitted_by:
            activity["SubmittedBy"] = submitted_by

        duration_seconds = compute_duration_seconds(activity)
        if duration_seconds is not None:
            activity["DurationSeconds"] = duration_seconds

        if not activity.get("ItemType"):
            fallback_type = activity.get("ArtifactType") or activity.get("ArtifactKind") or activity.get("ObjectType")
            if fallback_type:
                activity["ItemType"] = fallback_type

        # Domain/location heuristics
        domain_source = activity.get("ItemName") or workspace_name
        activity["Domain"] = activity.get("Domain") or infer_domain(domain_source)
        activity["Location"] = activity.get("Location") or infer_location(workspace)
        
        # Normalize status
        # API uses 'IsSuccess' boolean, but sometimes 'Status' string might exist in other contexts
        is_success = activity.get("IsSuccess")
        if is_success is not None:
            activity["Status"] = "Succeeded" if is_success else "Failed"
        else:
            # Fallback to existing Status field or default
            activity["Status"] = normalize_status(activity.get("Status") or activity.get("status"))

        return activity

    def _attach_item_metadata(self, activity: Dict[str, Any], workspace_id: str, item_lookup: Dict[str, Dict[str, Any]]) -> None:
        """Merge workspace item metadata into the activity record."""
        item_id = activity.get("ItemId")
        metadata = item_lookup.get(item_id) if item_id else None
        if not metadata:
            return

        activity["ItemName"] = metadata.get("displayName") or activity.get("ItemName")
        activity["ItemType"] = metadata.get("type") or activity.get("ItemType")
        activity["ObjectUrl"] = activity.get("ObjectUrl") or build_object_url(workspace_id, item_id, activity.get("ItemType"))

        created_by = extract_user_from_metadata(metadata.get("createdByUser"))
        if created_by:
            activity["CreatedBy"] = created_by

        modified_by = extract_user_from_metadata(metadata.get("modifiedByUser"))
        if modified_by:
            activity["LastUpdatedBy"] = modified_by

        activity["Domain"] = activity.get("Domain") or infer_domain(metadata.get("displayName"))

    def _extract_item_id(self, activity: Dict[str, Any]) -> Optional[str]:
        """Look across multiple potential keys to find an item identifier."""
        candidate_keys = [
            "ItemId",
            "ArtifactId",
            "ObjectId",
            "ArtifactInstanceId",
            "DatasetId",
            "ReportId",
        ]
        for key in candidate_keys:
            value = activity.get(key)
            if value:
                return str(value)
        return None
    
    def test_api_connectivity(self) -> Dict[str, bool]:
        """
        Test connectivity to Fabric and Power BI APIs.
        
        Returns:
            Dictionary with connectivity test results
        """
        results = {
            "fabric_api": False,
            "powerbi_api": False,
            "authentication": False
        }
        
        # Test authentication
        try:
            results["authentication"] = self.auth.validate_credentials()
        except Exception as e:
            self.logger.error(f"Authentication test failed: {str(e)}")
        
        # Test Fabric API (use member-only for connectivity test to avoid rate limits)
        try:
            workspaces = self.get_workspaces(tenant_wide=False, exclude_personal=True)
            results["fabric_api"] = len(workspaces) >= 0  # Even 0 workspaces means API is accessible
        except Exception as e:
            self.logger.error(f"Fabric API test failed: {str(e)}")
        
        # Test Power BI API
        try:
            url = f"{self.powerbi_base_url}/v1.0/myorg/groups"
            headers = self.auth.get_powerbi_headers()
            response = self.session.get(url, headers=headers, timeout=self.timeout)
            results["powerbi_api"] = response.status_code == 200
        except Exception as e:
            self.logger.error(f"Power BI API test failed: {str(e)}")
        
        return results