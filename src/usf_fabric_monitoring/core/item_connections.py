"""
Item Connections Extractor for Microsoft Fabric

Extracts item-level connections and relationships using Fabric REST APIs:
- GET /workspaces/{id}/items/{itemId}/connections
- GET /groups/{id}/datasets/{datasetId}/datasources

These endpoints provide additional lineage detail not captured by Admin Scanner.
"""

import logging
from typing import Any

import requests

logger = logging.getLogger(__name__)


class ItemConnectionsExtractor:
    """
    Extracts item-level connection and datasource information.

    This complements the Admin Scanner API by providing:
    - Semantic Model data connections
    - Dataset-to-datasource bindings
    - Item-to-item relationships via connections endpoint
    """

    def __init__(self, token: str):
        """
        Initialize the extractor.

        Args:
            token: OAuth2 bearer token
        """
        self.token = token
        self.headers = {"Authorization": f"Bearer {token}"}
        self.fabric_base = "https://api.fabric.microsoft.com/v1"
        self.powerbi_base = "https://api.powerbi.com/v1.0/myorg"
        self.max_retries = 3
        self.base_delay = 1

    def _make_request(self, url: str, method: str = "GET") -> requests.Response | None:
        """Make HTTP request with retry logic."""
        import time

        for attempt in range(self.max_retries):
            try:
                response = requests.request(method, url, headers=self.headers)

                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", self.base_delay * (2**attempt)))
                    logger.warning(f"Rate limited. Waiting {retry_after}s...")
                    time.sleep(retry_after)
                    continue

                return response

            except requests.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt == self.max_retries - 1:
                    return None
                time.sleep(self.base_delay * (2**attempt))

        return None

    def get_item_connections(self, workspace_id: str, item_id: str) -> list[dict[str, Any]]:
        """
        Get connections for a specific item.

        API: GET /workspaces/{workspaceId}/items/{itemId}/connections

        Args:
            workspace_id: Workspace GUID
            item_id: Item GUID

        Returns:
            List of connection records
        """
        url = f"{self.fabric_base}/workspaces/{workspace_id}/items/{item_id}/connections"

        response = self._make_request(url)
        if response is None or response.status_code != 200:
            logger.debug(f"No connections for item {item_id}: {response.status_code if response else 'No response'}")
            return []

        connections = response.json().get("value", [])

        return [
            {
                "item_id": item_id,
                "workspace_id": workspace_id,
                "connection_id": conn.get("id"),
                "connection_type": conn.get("connectivityType"),
                "connection_path": conn.get("connectionDetails", {}).get("path"),
                "gateway_id": conn.get("gatewayId"),
                "data_source_type": conn.get("datasourceType"),
                "raw": conn,
            }
            for conn in connections
        ]

    def get_dataset_datasources(self, group_id: str, dataset_id: str) -> list[dict[str, Any]]:
        """
        Get datasources for a Power BI dataset/semantic model.

        API: GET /groups/{groupId}/datasets/{datasetId}/datasources

        Args:
            group_id: Workspace GUID (Power BI terminology)
            dataset_id: Dataset GUID

        Returns:
            List of datasource records
        """
        url = f"{self.powerbi_base}/groups/{group_id}/datasets/{dataset_id}/datasources"

        response = self._make_request(url)
        if response is None or response.status_code != 200:
            logger.debug(f"No datasources for dataset {dataset_id}")
            return []

        datasources = response.json().get("value", [])

        return [
            {
                "dataset_id": dataset_id,
                "group_id": group_id,
                "datasource_id": ds.get("datasourceId"),
                "datasource_type": ds.get("datasourceType"),
                "connection_details": ds.get("connectionDetails"),
                "gateway_id": ds.get("gatewayId"),
                "raw": ds,
            }
            for ds in datasources
        ]

    def get_semantic_models(self, workspace_id: str) -> list[dict[str, Any]]:
        """
        Get all semantic models in a workspace.

        API: GET /workspaces/{workspaceId}/items?type=SemanticModel

        Args:
            workspace_id: Workspace GUID

        Returns:
            List of semantic model records
        """
        url = f"{self.fabric_base}/workspaces/{workspace_id}/items?type=SemanticModel"

        all_items = []
        while url:
            response = self._make_request(url)
            if response is None or response.status_code != 200:
                break

            data = response.json()
            all_items.extend(data.get("value", []))
            url = data.get("continuationUri")

        return all_items

    def get_dataflows(self, workspace_id: str) -> list[dict[str, Any]]:
        """
        Get all dataflows in a workspace.

        API: GET /workspaces/{workspaceId}/items?type=Dataflow

        Args:
            workspace_id: Workspace GUID

        Returns:
            List of dataflow records
        """
        url = f"{self.fabric_base}/workspaces/{workspace_id}/items?type=Dataflow"

        all_items = []
        while url:
            response = self._make_request(url)
            if response is None or response.status_code != 200:
                break

            data = response.json()
            all_items.extend(data.get("value", []))
            url = data.get("continuationUri")

        return all_items

    def extract_all_connections(self, workspaces: list[dict]) -> list[dict[str, Any]]:
        """
        Extract connections for all items across workspaces.

        Args:
            workspaces: List of workspace dicts with 'id' and items

        Returns:
            List of connection records
        """
        all_connections = []

        for ws in workspaces:
            ws_id = ws.get("id")
            ws_name = ws.get("displayName", ws.get("name", "Unknown"))

            logger.info(f"Extracting connections from: {ws_name}")

            # Get semantic models and their connections
            models = self.get_semantic_models(ws_id)
            for model in models:
                connections = self.get_item_connections(ws_id, model.get("id"))
                for conn in connections:
                    conn["workspace_name"] = ws_name
                    conn["item_name"] = model.get("displayName", model.get("name"))
                    conn["item_type"] = "SemanticModel"
                all_connections.extend(connections)

                # Also get datasources for Power BI datasets
                datasources = self.get_dataset_datasources(ws_id, model.get("id"))
                for ds in datasources:
                    ds["workspace_name"] = ws_name
                    ds["item_name"] = model.get("displayName", model.get("name"))
                    ds["item_type"] = "SemanticModel"
                all_connections.extend(datasources)

        return all_connections


def create_item_connections_extractor(token: str) -> ItemConnectionsExtractor:
    """Factory function to create an extractor instance."""
    return ItemConnectionsExtractor(token)
