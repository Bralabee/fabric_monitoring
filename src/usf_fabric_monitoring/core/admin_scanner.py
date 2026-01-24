"""
Admin Scanner API Client for Microsoft Fabric

Provides bulk workspace metadata retrieval including lineage information
using the Admin Scanner API (POST /admin/workspaces/getInfo).

This is an additive feature that complements the existing iterative extraction 
approach, offering O(1) batch operations vs O(N) iterative calls.

Reference: https://learn.microsoft.com/en-us/rest/api/fabric/admin/workspaces
"""

import time
import logging
from typing import List, Dict, Any, Optional

import requests


logger = logging.getLogger(__name__)


class AdminScannerError(Exception):
    """Exception raised for Admin Scanner API errors."""
    pass


class AdminScannerClient:
    """
    Client for Microsoft Fabric Admin Scanner API.
    
    Provides bulk metadata retrieval with lineage and datasource details.
    Reduces API calls from O(N) to O(1) per batch (max 100 workspaces).
    
    Prerequisites:
        - Fabric Administrator role, OR
        - Service Principal with Tenant.Read.All permission
        - "Service principals can use read-only admin APIs" enabled in tenant
    """

    def __init__(self, token: str, api_base: str = "https://api.powerbi.com/v1.0/myorg"):
        """
        Initialize the Admin Scanner client.
        
        Args:
            token: OAuth2 bearer token with admin permissions
            api_base: Power BI Admin API base URL (Admin Scanner uses Power BI API, not Fabric API)
        """
        self.token = token
        self.api_base = api_base
        self.headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}

        # Rate limiting: 500 requests/hour, max 16 concurrent
        self.max_retries = 5
        self.base_delay = 2
        self.batch_size = 100  # Max workspaces per getInfo call

    def _make_request(self, method: str, url: str, **kwargs) -> Optional[requests.Response]:
        """Make HTTP request with retry logic for 429 rate limiting."""
        for attempt in range(self.max_retries):
            try:
                response = requests.request(method, url, headers=self.headers, **kwargs)

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", self.base_delay * (2 ** attempt)))
                    logger.warning(f"Rate limited. Waiting {retry_after}s before retry {attempt + 1}/{self.max_retries}...")
                    time.sleep(retry_after)
                    continue

                return response

            except requests.RequestException as e:
                logger.error(f"Request failed: {str(e)}")
                if attempt == self.max_retries - 1:
                    raise AdminScannerError(f"Request failed after {self.max_retries} retries: {e}")
                time.sleep(self.base_delay * (2 ** attempt))

        return None

    def scan_workspaces(
        self,
        workspace_ids: List[str],
        lineage: bool = True,
        datasource_details: bool = True,
        dataset_schema: bool = False,
        dataset_expressions: bool = False,
        poll_interval: int = 5,
        max_poll_time: int = 300
    ) -> Dict[str, Any]:
        """
        Scan workspaces using Admin Scanner API.
        
        This is a 3-step process:
        1. POST /admin/workspaces/getInfo - Initiate scan
        2. GET /admin/workspaces/scanStatus/{scanId} - Poll until complete
        3. GET /admin/workspaces/scanResult/{scanId} - Retrieve results
        
        Args:
            workspace_ids: List of workspace GUIDs to scan
            lineage: Include lineage information (default True)
            datasource_details: Include datasource details (default True)
            dataset_schema: Include dataset schema (default False)
            dataset_expressions: Include M expressions (default False)
            poll_interval: Seconds between status polls (default 5)
            max_poll_time: Maximum seconds to wait for scan (default 300)
            
        Returns:
            Dictionary containing scan results with workspaces and lineage data
            
        Raises:
            AdminScannerError: If scan fails or times out
        """
        if not workspace_ids:
            return {"workspaces": [], "lineage": []}

        # Process in batches of 100
        all_results = {"workspaces": [], "lineage": []}

        for i in range(0, len(workspace_ids), self.batch_size):
            batch = workspace_ids[i:i + self.batch_size]
            logger.info(f"Scanning batch {i // self.batch_size + 1}: {len(batch)} workspaces")

            batch_result = self._scan_batch(
                batch, lineage, datasource_details, dataset_schema,
                dataset_expressions, poll_interval, max_poll_time
            )

            all_results["workspaces"].extend(batch_result.get("workspaces", []))
            if "lineage" in batch_result:
                all_results["lineage"].extend(batch_result.get("lineage", []))

        return all_results

    def _scan_batch(
        self,
        workspace_ids: List[str],
        lineage: bool,
        datasource_details: bool,
        dataset_schema: bool,
        dataset_expressions: bool,
        poll_interval: int,
        max_poll_time: int
    ) -> Dict[str, Any]:
        """Scan a single batch of workspaces (max 100)."""

        # Step 1: Initiate scan
        scan_id = self._initiate_scan(
            workspace_ids, lineage, datasource_details, dataset_schema, dataset_expressions
        )

        if not scan_id:
            raise AdminScannerError("Failed to initiate scan - no scan ID returned")

        logger.info(f"Scan initiated with ID: {scan_id}")

        # Step 2: Poll for completion
        start_time = time.time()
        while True:
            elapsed = time.time() - start_time
            if elapsed > max_poll_time:
                raise AdminScannerError(f"Scan timed out after {max_poll_time} seconds")

            status = self._get_scan_status(scan_id)

            if status == "Succeeded":
                logger.info(f"Scan completed successfully in {elapsed:.1f}s")
                break
            elif status == "Failed":
                raise AdminScannerError("Scan failed")
            elif status in ("NotStarted", "Running"):
                logger.debug(f"Scan status: {status}, waiting {poll_interval}s...")
                time.sleep(poll_interval)
            else:
                logger.warning(f"Unknown scan status: {status}")
                time.sleep(poll_interval)

        # Step 3: Get results
        return self._get_scan_result(scan_id)

    def _initiate_scan(
        self,
        workspace_ids: List[str],
        lineage: bool,
        datasource_details: bool,
        dataset_schema: bool,
        dataset_expressions: bool
    ) -> Optional[str]:
        """
        POST /admin/workspaces/getInfo to initiate a scan.
        
        Returns:
            Scan ID if successful, None otherwise
        """
        url = f"{self.api_base}/admin/workspaces/getInfo"

        # Build query parameters
        params = {
            "lineage": str(lineage).lower(),
            "datasourceDetails": str(datasource_details).lower(),
            "datasetSchema": str(dataset_schema).lower(),
            "datasetExpressions": str(dataset_expressions).lower()
        }

        # Body contains workspace IDs
        body = {"workspaces": workspace_ids}

        response = self._make_request("POST", url, params=params, json=body)

        if response is None:
            return None

        if response.status_code == 202:
            # Accepted - scan initiated
            data = response.json()
            return data.get("id") or data.get("scanId")
        elif response.status_code == 200:
            # Some versions return 200
            data = response.json()
            return data.get("id") or data.get("scanId")
        else:
            logger.error(f"Failed to initiate scan: {response.status_code} - {response.text}")
            return None

    def _get_scan_status(self, scan_id: str) -> str:
        """
        GET /admin/workspaces/scanStatus/{scanId}
        
        Returns:
            Status string: "NotStarted", "Running", "Succeeded", "Failed"
        """
        url = f"{self.api_base}/admin/workspaces/scanStatus/{scan_id}"

        response = self._make_request("GET", url)

        if response is None or response.status_code != 200:
            return "Failed"

        data = response.json()
        return data.get("status", "Unknown")

    def _get_scan_result(self, scan_id: str) -> Dict[str, Any]:
        """
        GET /admin/workspaces/scanResult/{scanId}
        
        Returns:
            Complete scan result including workspaces and lineage
        """
        url = f"{self.api_base}/admin/workspaces/scanResult/{scan_id}"

        response = self._make_request("GET", url)

        if response is None or response.status_code != 200:
            raise AdminScannerError(f"Failed to get scan results: {response.status_code if response else 'No response'}")

        return response.json()

    def normalize_lineage_results(self, scan_result: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Normalize Scanner API results to match iterative extraction format.
        
        This ensures compatibility with existing downstream processing.
        
        Args:
            scan_result: Raw scan result from Admin Scanner API
            
        Returns:
            List of lineage records in standard format
        """
        lineage_data = []

        workspaces = scan_result.get("workspaces", [])

        for ws in workspaces:
            ws_id = ws.get("id")
            ws_name = ws.get("name", "Unknown")

            # Process each artifact type
            for artifact_type in ["lakehouses", "warehouses", "mirroredDatabases", "kqlDatabases"]:
                artifacts = ws.get(artifact_type, [])
                for artifact in artifacts:
                    # Extract lineage if available
                    upstream = artifact.get("upstreamDataflows", [])
                    downstream = artifact.get("downstreamDataflows", [])
                    datasources = artifact.get("datasourceUsages", [])

                    lineage_data.append({
                        "Workspace Name": ws_name,
                        "Workspace ID": ws_id,
                        "Item Name": artifact.get("name", "Unknown"),
                        "Item ID": artifact.get("id"),
                        "Item Type": artifact_type.rstrip("s").title(),  # lakehouses -> Lakehouse
                        "Shortcut Name": None,
                        "Shortcut Path": None,
                        "Source Type": "Scanner API",
                        "Source Connection": None,
                        "Source Database": None,
                        "Connection ID": None,
                        "Upstream Count": len(upstream),
                        "Downstream Count": len(downstream),
                        "Datasource Count": len(datasources),
                        "Full Definition": str({
                            "upstream": upstream,
                            "downstream": downstream,
                            "datasources": datasources
                        })
                    })

            # Process shortcuts separately if available
            shortcuts = ws.get("shortcuts", [])
            for shortcut in shortcuts:
                target = shortcut.get("target", {})
                lineage_data.append({
                    "Workspace Name": ws_name,
                    "Workspace ID": ws_id,
                    "Item Name": shortcut.get("itemName", "Unknown"),
                    "Item ID": shortcut.get("itemId"),
                    "Item Type": "Lakehouse Shortcut",
                    "Shortcut Name": shortcut.get("name"),
                    "Shortcut Path": shortcut.get("path"),
                    "Source Type": target.get("type", "Unknown"),
                    "Source Connection": target.get("location") or target.get("path"),
                    "Source Database": None,
                    "Connection ID": target.get("connectionId"),
                    "Full Definition": str(shortcut)
                })

        return lineage_data
