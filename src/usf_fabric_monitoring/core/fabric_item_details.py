"""
Microsoft Fabric Item Detail Extraction Module

This module handles extraction of detailed item information from Microsoft Fabric APIs including:
- Job instances for Pipelines and Notebooks
- Table details for Lakehouses
"""

import logging
import os
from typing import List, Dict, Any
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .auth import FabricAuthenticator

class FabricItemDetailExtractor:
    """Extracts detailed item information from Microsoft Fabric APIs"""

    def __init__(self, authenticator: FabricAuthenticator):
        """
        Initialize the item detail extractor.

        Args:
            authenticator: Configured FabricAuthenticator instance
        """
        self.auth = authenticator
        self.logger = logging.getLogger(__name__)

        # API base URLs
        self.fabric_base_url = os.getenv("FABRIC_API_BASE_URL", "https://api.fabric.microsoft.com")
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

    def get_item_job_instances(self, workspace_id: str, item_id: str) -> List[Dict[str, Any]]:
        """
        Get job instances for a specific item (Pipeline, Notebook, etc.).

        Args:
            workspace_id: Fabric workspace ID
            item_id: Fabric item ID

        Returns:
            List of job instances
        """
        try:
            url = f"{self.fabric_base_url}/{self.api_version}/workspaces/{workspace_id}/items/{item_id}/jobs/instances"
            headers = self.auth.get_fabric_headers()

            self.logger.debug(f"Fetching job instances for item {item_id} in workspace {workspace_id}")

            response = self.session.get(url, headers=headers, timeout=self.timeout)

            if response.status_code == 404:
                self.logger.warning(f"Job instances endpoint not found for item {item_id}")
                return []

            response.raise_for_status()

            data = response.json()
            return data.get("value", [])

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch job instances for item {item_id}: {str(e)}")
            return []

    def get_lakehouse_tables(self, workspace_id: str, lakehouse_id: str) -> List[Dict[str, Any]]:
        """
        Get tables for a specific Lakehouse.

        Args:
            workspace_id: Fabric workspace ID
            lakehouse_id: Fabric Lakehouse ID

        Returns:
            List of tables
        """
        try:
            url = f"{self.fabric_base_url}/{self.api_version}/workspaces/{workspace_id}/lakehouses/{lakehouse_id}/tables"
            headers = self.auth.get_fabric_headers()

            self.logger.debug(f"Fetching tables for lakehouse {lakehouse_id} in workspace {workspace_id}")

            response = self.session.get(url, headers=headers, timeout=self.timeout)

            if response.status_code == 404:
                self.logger.warning(f"Tables endpoint not found for lakehouse {lakehouse_id}")
                return []

            response.raise_for_status()

            data = response.json()
            return data.get("value", [])

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch tables for lakehouse {lakehouse_id}: {str(e)}")
            return []
