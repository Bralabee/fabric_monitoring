"""Workspace access enforcement helpers.

This module inventories Fabric workspaces and ensures the configured security
principals retain the required roles. It supports Fabric REST APIs with an
automatic fallback to the legacy Power BI admin endpoints so the automation can
continue to run even when Fabric endpoints are unavailable.
"""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, FrozenSet, Iterable, List, Optional, Sequence

import requests

from .auth import FabricAuthenticator, create_authenticator_from_env


LOGGER = logging.getLogger(__name__)

FABRIC_WORKSPACES_ENDPOINT = "https://api.fabric.microsoft.com/v1/admin/workspaces"
FABRIC_USERS_ENDPOINT_TEMPLATE = "https://api.fabric.microsoft.com/v1/workspaces/{workspace_id}/users"
POWER_BI_GROUPS_ENDPOINT = "https://api.powerbi.com/v1.0/myorg/admin/groups"
POWER_BI_GROUP_USERS_TEMPLATE = "https://api.powerbi.com/v1.0/myorg/admin/groups/{workspace_id}/users"


@dataclass
class AccessRequirement:
    """Represents an access requirement for a security principal."""

    object_id: str
    display_name: str
    role: str
    fabric_principal_type: str = "AadGroup"
    powerbi_principal_type: str = "Group"


@dataclass
class SuppressionRules:
    """Workspaces that should be skipped by the enforcer."""

    workspace_ids: FrozenSet[str]
    workspace_names: FrozenSet[str]

    @classmethod
    def from_payload(cls, payload: Optional[Dict[str, Any]]) -> "SuppressionRules":
        payload = payload or {}
        return cls(
            workspace_ids=frozenset(str(x).lower() for x in payload.get("workspaceIds", [])),
            workspace_names=frozenset(str(x).lower() for x in payload.get("workspaceNames", [])),
        )

    def is_suppressed(self, workspace_id: str, workspace_name: str) -> bool:
        workspace_id = (workspace_id or "").lower()
        workspace_name = (workspace_name or "").lower()
        return workspace_id in self.workspace_ids or workspace_name in self.workspace_names


class WorkspaceAccessError(RuntimeError):
    """Raised when the enforcer cannot complete its task."""


class WorkspaceAccessEnforcer:
    """Ensures critical security groups remain assigned to every workspace."""

    def __init__(
        self,
        access_requirements: Sequence[AccessRequirement],
        suppressions: Optional[SuppressionRules] = None,
        *,
        authenticator: Optional[FabricAuthenticator] = None,
        api_preference: str = "auto",
        dry_run: bool = False,
        logger: Optional[logging.Logger] = None,
        exclude_personal_workspaces: bool = True,
    ) -> None:
        if not access_requirements:
            raise WorkspaceAccessError("No access requirements were provided")

        self.access_requirements = tuple(access_requirements)
        self.suppressions = suppressions or SuppressionRules.from_payload(None)
        self.authenticator = authenticator or create_authenticator_from_env()
        self.logger = logger or LOGGER
        self.dry_run = dry_run

        preference = api_preference.lower()
        if preference not in {"auto", "fabric", "powerbi"}:
            raise WorkspaceAccessError(
                "api_preference must be one of 'auto', 'fabric', or 'powerbi'"
            )
        self.api_preference = preference
        self.fabric_endpoint = os.getenv("FABRIC_WORKSPACES_ENDPOINT", FABRIC_WORKSPACES_ENDPOINT)
        self.fabric_users_template = os.getenv(
            "FABRIC_USERS_ENDPOINT_TEMPLATE", FABRIC_USERS_ENDPOINT_TEMPLATE
        )
        self.pbi_endpoint = os.getenv("POWER_BI_GROUPS_ENDPOINT", POWER_BI_GROUPS_ENDPOINT)
        self.pbi_users_template = os.getenv(
            "POWER_BI_GROUP_USERS_TEMPLATE", POWER_BI_GROUP_USERS_TEMPLATE
        )

        self.request_timeout = int(os.getenv("FABRIC_ENFORCER_TIMEOUT", "30"))
        self.max_retries = int(os.getenv("FABRIC_ENFORCER_MAX_RETRIES", "3"))
        self.page_size = int(os.getenv("FABRIC_ENFORCER_PAGE_SIZE", "100"))
        self.exclude_personal_workspaces = exclude_personal_workspaces

    # ------------------------------------------------------------------
    # Public helpers
    # ------------------------------------------------------------------
    def enforce(
        self,
        *,
        workspace_filter: Optional[Iterable[str]] = None,
        max_workspaces: Optional[int] = None,
        fabric_only: bool = False,
    ) -> Dict[str, Any]:
        """Execute the enforcement workflow."""

        self.logger.info("="*60)
        self.logger.info("Starting workspace access enforcement")
        self.logger.info(f"API preference: {self.api_preference}")
        self.logger.info(f"Exclude personal workspaces: {self.exclude_personal_workspaces}")
        self.logger.info(f"Fabric workspaces only: {fabric_only}")
        self.logger.info(f"Dry run: {self.dry_run}")
        self.logger.info("="*60)

        workspaces = self._fetch_workspaces()
        self.logger.info(f"Total workspaces retrieved: {len(workspaces)}")
        
        if fabric_only:
            original_count = len(workspaces)
            workspaces = [ws for ws in workspaces if self._is_fabric_workspace(ws)]
            self.logger.info(f"Filtered to {len(workspaces)} Fabric workspaces (from {original_count})")

        if workspace_filter:
            filter_tokens = {str(token).lower() for token in workspace_filter if str(token).strip()}
            workspaces = [
                ws
                for ws in workspaces
                if (ws.get("id", "").lower() in filter_tokens)
                or (ws.get("name", "").lower() in filter_tokens)
            ]

        if max_workspaces is not None:
            self.logger.info(f"Limiting enforcement to first {max_workspaces} workspaces")
            workspaces = workspaces[: max(0, max_workspaces)]

        self.logger.info("="*60)
        self.logger.info(f"Evaluating {len(workspaces)} workspaces...")
        self.logger.info("="*60)

        summary: Dict[str, Any] = {
            "workspace_count": len(workspaces),
            "actions": [],
            "dry_run": self.dry_run,
        }

        for idx, workspace in enumerate(workspaces, 1):
            if idx % 10 == 0 or idx == 1:
                self.logger.info(f"Progress: {idx}/{len(workspaces)} workspaces evaluated")
            workspace_summary = self._enforce_workspace(workspace)
            summary["actions"].append(workspace_summary)

        self.logger.info("="*60)
        self.logger.info(f"Completed evaluation of {len(workspaces)} workspaces")
        self.logger.info("="*60)

        return summary

    def _is_fabric_workspace(self, workspace: Dict[str, Any]) -> bool:
        """Determine if a workspace is a Fabric/Premium workspace."""
        # Check for capacityId (Fabric/Premium)
        capacity_id = workspace.get("capacityId")
        if capacity_id and capacity_id != "00000000-0000-0000-0000-000000000000":
            return True
            
        # Check for isOnDedicatedCapacity (Power BI Premium/Fabric)
        if workspace.get("isOnDedicatedCapacity"):
            return True
            
        return False

    @staticmethod
    def load_access_requirements(path: Path) -> Sequence[AccessRequirement]:
        """Load access requirements from JSON."""

        if not path.exists():
            raise WorkspaceAccessError(f"Access requirement file not found: {path}")

        payload = json.loads(path.read_text(encoding="utf-8"))
        groups = payload.get("groups")
        if not groups:
            raise WorkspaceAccessError("No groups defined in access requirement file")

        requirements = []
        for group in groups:
            requirements.append(
                AccessRequirement(
                    object_id=group["objectId"],
                    display_name=group.get("displayName", group["objectId"]),
                    role=group.get("role", "Admin"),
                    fabric_principal_type=group.get("fabricPrincipalType", "AadGroup"),
                    powerbi_principal_type=group.get("powerBiPrincipalType", "Group"),
                )
            )

        return tuple(requirements)

    @staticmethod
    def load_suppressions(path: Optional[Path]) -> SuppressionRules:
        """Load suppression rules from JSON when available."""

        if not path:
            return SuppressionRules.from_payload(None)
        if not path.exists():
            return SuppressionRules.from_payload(None)
        payload = json.loads(path.read_text(encoding="utf-8"))
        return SuppressionRules.from_payload(payload)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _fetch_workspaces(self) -> List[Dict[str, Any]]:
        errors: List[str] = []
        aggregated: Dict[str, Dict[str, Any]] = {}
        source_tallies = {"legacy": 0, "fabric": 0, "powerbi": 0}

        def record(items: List[Dict[str, Any]], source: str) -> None:
            source_tallies[source] = len(items)
            for item in items:
                workspace_id = item.get("id")
                if not workspace_id:
                    continue
                entry = aggregated.setdefault(
                    workspace_id,
                    {
                        "id": workspace_id,
                        "name": item.get("name") or "Unnamed",
                        "sources": [],
                        "capacityId": item.get("capacityId"),
                        "isOnDedicatedCapacity": item.get("isOnDedicatedCapacity", False),
                    },
                )
                if item.get("name"):
                    entry["name"] = item["name"]
                # Update capacity info if available and not already set
                if item.get("capacityId"):
                    entry["capacityId"] = item["capacityId"]
                if item.get("isOnDedicatedCapacity"):
                    entry["isOnDedicatedCapacity"] = True
                    
                entry["sources"].append(source)

        # For tenant-wide enforcement, use ONLY admin APIs (Fabric/Power BI)
        # Do NOT use legacy extractor - it only returns member workspaces (~139)
        # We need ALL tenant workspaces (~286+) to enforce security policies
        
        sources_to_fetch = []
        if self.api_preference in {"auto", "fabric"}:
            sources_to_fetch.append(("fabric", self._fetch_fabric_workspaces))
        if self.api_preference in {"auto", "powerbi"}:
            sources_to_fetch.append(("powerbi", self._fetch_powerbi_workspaces))

        for source_name, fetch_func in sources_to_fetch:
            try:
                items = fetch_func()
                record(items, source_name)
                self.logger.info(
                    "Collected %s workspaces via %s admin endpoint", len(items), source_name.title()
                )
            except WorkspaceAccessError as exc:
                errors.append(f"{source_name}: {exc}")
                self.logger.warning("%s admin endpoint unavailable: %s", source_name.title(), exc)

        if not aggregated:
            joined = "; ".join(errors) or "No workspace sources succeeded"
            raise WorkspaceAccessError(f"Failed to enumerate workspaces: {joined}")

        # Build consolidated list from all sources (don't filter to legacy-only)
        # Legacy extractor only returns workspaces where user is a member (~139)
        # Power BI admin API returns ALL tenant workspaces (~286+)
        # For enforcement, we want ALL tenant workspaces to apply security policies
        consolidated: List[Dict[str, Any]] = []
        for entry in aggregated.values():
            sources = entry["sources"]
            consolidated.append(
                {
                    "id": entry["id"],
                    "name": entry["name"],
                    "source": self._select_workspace_source(sources),
                    "capacityId": entry.get("capacityId"),
                    "isOnDedicatedCapacity": entry.get("isOnDedicatedCapacity", False),
                }
            )

        self.logger.info(
            "Workspace inventory consolidated to %s unique entries (fabric=%s, powerbi=%s)",
            len(consolidated),
            source_tallies.get("fabric", 0),
            source_tallies.get("powerbi", 0),
        )
        return consolidated

    @staticmethod
    def _select_workspace_source(sources: Sequence[str]) -> str:
        """Prefer Fabric admin API, then Power BI admin API for workspace metadata."""
        for candidate in ("fabric", "powerbi"):
            if candidate in sources:
                return candidate
        return sources[0] if sources else "powerbi"

    def _fetch_fabric_workspaces(self) -> List[Dict[str, Any]]:
        """
        Fetch workspaces from Fabric admin API (/v1/admin/workspaces).
        
        **PERMISSION REQUIREMENTS (if returns 0 workspaces):**
        
        1. Azure AD/Entra ID Role:
           - Assign 'Fabric Administrator' or 'Power Platform Administrator' role
           - Go to: Azure Portal > Entra ID > Roles > Fabric Administrator > Add assignment
        
        2. Fabric Admin Portal Settings:
           - Enable: Admin Portal > Tenant settings > Developer settings > 
             'Service principals can use Fabric APIs' > Add your service principal
        
        3. API Permission (App Registration):
           - Not required - uses service principal authentication directly
           - Ensure AZURE_CLIENT_ID/CLIENT_SECRET/TENANT_ID are correct
        
        Alternative: Use Power BI Admin API which requires only Tenant.Read.All scope.
        
        By default, excludes personal workspaces to reduce API calls.
        """
        # Build filter to exclude personal workspaces
        filter_clause = "$filter=type ne 'PersonalGroup'" if self.exclude_personal_workspaces else ""
        filter_param = f"&{filter_clause}" if filter_clause else ""
        
        url = f"{self.fabric_endpoint}?$top={self.page_size}{filter_param}"
        workspaces: List[Dict[str, Any]] = []

        while url:
            payload = self._get_json(url, use_fabric=True)
            items = payload.get("value", [])
            for item in items:
                # Skip inactive workspaces (Deleted, RemovalPending, etc.)
                if item.get("state") and item.get("state") != "Active":
                    continue

                workspaces.append(
                    {
                        "id": item.get("id"),
                        "name": item.get("displayName") or item.get("name", "Unnamed"),
                        "source": "fabric",
                        "capacityId": item.get("capacityId"),
                    }
                )
            url = payload.get("nextLink") or payload.get("@odata.nextLink")

        if not workspaces:
            raise WorkspaceAccessError("Fabric admin endpoint returned no workspaces")
        return workspaces

    def _fetch_powerbi_workspaces(self) -> List[Dict[str, Any]]:
        """
        Fetch all workspaces from Power BI admin API using $skip pagination.
        
        **PERMISSION REQUIREMENTS:**
        - Requires 'Tenant.Read.All' API permission in Azure AD app registration
        - Or user must have 'Power BI Administrator' or 'Fabric Administrator' role
        
        **RATE LIMITS:**
        - Power BI Admin API has strict rate limits (429 errors common)
        - Use $filter to exclude personal workspaces to reduce API calls
        - If hitting limits, wait ~30-60 minutes before retrying
        
        **PAGINATION:**
        - Power BI admin groups API doesn't return @odata.nextLink
        - Must manually paginate using $skip until fewer items returned than $top
        
        By default, excludes personal workspaces (type=PersonalGroup) to reduce
        API calls and avoid rate limits. Set exclude_personal_workspaces=False to include them.
        """
        workspaces: List[Dict[str, Any]] = []
        skip = 0
        max_pages = 100  # Safety limit: 100 pages * 200 = 20,000 workspaces max
        page = 0
        
        # Build filter to exclude personal workspaces
        filter_clause = "$filter=type ne 'PersonalGroup'" if self.exclude_personal_workspaces else ""
        filter_param = f"&{filter_clause}" if filter_clause else ""
        
        while page < max_pages:
            url = f"{self.pbi_endpoint}?$top={self.page_size}&$skip={skip}{filter_param}"
            payload = self._get_json(url, use_fabric=False)
            items = payload.get("value", [])
            
            if not items:
                break
                
            for item in items:
                # Skip inactive workspaces
                if item.get("state") and item.get("state") != "Active":
                    continue

                workspaces.append(
                    {
                        "id": item.get("id"),
                        "name": item.get("name", "Unnamed"),
                        "source": "powerbi",
                        "capacityId": item.get("capacityId"),
                        "isOnDedicatedCapacity": item.get("isOnDedicatedCapacity", False),
                    }
                )
            
            page += 1
            self.logger.info(f"Power BI page {page}: fetched {len(items)} workspaces (total: {len(workspaces)})")
            
            # Stop if we got fewer items than requested (last page)
            if len(items) < self.page_size:
                break
                
            skip += self.page_size

        if not workspaces:
            raise WorkspaceAccessError("Power BI admin endpoint returned no workspaces")
        return workspaces

    def _enforce_workspace(self, workspace: Dict[str, Any]) -> Dict[str, Any]:
        workspace_id = workspace.get("id")
        workspace_name = workspace.get("name")
        if not workspace_id:
            return {
                "workspace": workspace,
                "status": "skipped",
                "reason": "missing_id",
                "actions": [],
            }

        if self.suppressions.is_suppressed(workspace_id, workspace_name):
            self.logger.info("Skipping suppressed workspace: %s", workspace_name)
            return {
                "workspace": workspace,
                "status": "suppressed",
                "actions": [],
            }

        existing_users = self._fetch_workspace_users(workspace_id)
        actions: List[Dict[str, Any]] = []

        for requirement in self.access_requirements:
            existing_role = self._locate_existing_role(existing_users, requirement.object_id)
            if existing_role and existing_role.lower() == requirement.role.lower():
                actions.append(
                    {
                        "group": requirement.display_name,
                        "action": "already_compliant",
                        "role": existing_role,
                    }
                )
                continue

            if existing_role and existing_role.lower() != requirement.role.lower():
                actions.append(
                    {
                        "group": requirement.display_name,
                        "action": "role_mismatch",
                        "current_role": existing_role,
                        "target_role": requirement.role,
                    }
                )
                continue

            remediation = self._assign_group(workspace_id, requirement)
            actions.append(remediation)

        return {
            "workspace": {"id": workspace_id, "name": workspace_name},
            "status": "evaluated",
            "actions": actions,
        }

    def _fetch_workspace_users(self, workspace_id: str) -> List[Dict[str, Any]]:
        errors: List[str] = []
        
        sources = []
        if self.api_preference in {"auto", "fabric"}:
            sources.append(("fabric", self.fabric_users_template, True))
        if self.api_preference in {"auto", "powerbi"}:
            sources.append(("powerbi", self.pbi_users_template, False))

        for source_name, url_template, use_fabric in sources:
            url = url_template.format(workspace_id=workspace_id)
            try:
                payload = self._get_json(url, use_fabric=use_fabric)
                return payload.get("value", payload.get("users", []))
            except WorkspaceAccessError as exc:
                if getattr(exc, "status_code", None) == 404:
                    # 404 might mean "Not Found" OR "Access Denied" (if not a member).
                    # We continue to the next source (e.g. Admin API) to verify.
                    errors.append(f"{source_name}: 404 Not Found")
                    continue
                
                errors.append(f"{source_name}: {exc}")
                self.logger.warning(
                    "%s user lookup failed for workspace %s: %s", 
                    source_name.title(), workspace_id, exc
                )

        # If we exhausted all sources and they all failed
        if any("404" in e for e in errors):
             self.logger.warning(
                "Workspace %s not found or inaccessible via any endpoint (404); skipping",
                workspace_id,
            )
             return []

        if errors:
            joined = "; ".join(errors)
            raise WorkspaceAccessError(
                f"Failed to fetch users for workspace {workspace_id}: {joined}"
            )

        raise WorkspaceAccessError(
            f"Unable to fetch users for workspace {workspace_id}: no API preference enabled"
        )

    def _assign_group(self, workspace_id: str, requirement: AccessRequirement) -> Dict[str, Any]:
        action_summary = {
            "group": requirement.display_name,
            "target_role": requirement.role,
            "action": "dry_run" if self.dry_run else "add_attempted",
        }

        if self.dry_run:
            return action_summary

        errors: List[str] = []
        
        sources = []
        if self.api_preference in {"auto", "fabric"}:
            sources.append(("fabric", self.fabric_users_template, True))
        if self.api_preference in {"auto", "powerbi"}:
            sources.append(("powerbi", self.pbi_users_template, False))

        for source_name, url_template, use_fabric in sources:
            url = url_template.format(workspace_id=workspace_id)
            
            if use_fabric:
                payload = {
                    "userObjectId": requirement.object_id,
                    "role": requirement.role,
                    "principalType": requirement.fabric_principal_type,
                }
            else:
                payload = {
                    "identifier": requirement.object_id,
                    "groupUserAccessRight": requirement.role,
                    "principalType": requirement.powerbi_principal_type,
                }

            try:
                self._request("POST", url, use_fabric=use_fabric, json=payload, expected_status=(200, 201))
                self.logger.info(
                    "Granted %s role to %s via %s API for workspace %s",
                    requirement.role,
                    requirement.display_name,
                    source_name.title(),
                    workspace_id,
                )
                action_summary["action"] = f"added_via_{source_name}"
                return action_summary
            except WorkspaceAccessError as exc:
                errors.append(f"{source_name}: {exc}")
                self.logger.warning(
                    "%s assignment failed for workspace %s (%s): %s",
                    source_name.title(),
                    workspace_id,
                    requirement.display_name,
                    exc,
                )

        joined = "; ".join(errors) if errors else "no API preference enabled"
        raise WorkspaceAccessError(
            f"Failed to assign {requirement.display_name} to workspace {workspace_id}: {joined}"
        )

    def _locate_existing_role(self, existing_users: Sequence[Dict[str, Any]], object_id: str) -> Optional[str]:
        for user in existing_users:
            identifier = str(
                user.get("identifier")
                or user.get("objectId")
                or user.get("userObjectId")
                or user.get("graphId")
                or ""
            ).lower()
            if identifier == object_id.lower():
                return user.get("groupUserAccessRight") or user.get("role")
        return None

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------
    def _get_json(self, url: str, *, use_fabric: bool) -> Dict[str, Any]:
        response = self._request("GET", url, use_fabric=use_fabric)
        try:
            return response.json()
        except ValueError as exc:
            raise WorkspaceAccessError(f"Endpoint returned invalid JSON: {exc}") from exc

    def _request(
        self,
        method: str,
        url: str,
        *,
        use_fabric: bool,
        expected_status: Sequence[int] = (200,),
        **kwargs: Any,
    ) -> requests.Response:
        headers = (
            self.authenticator.get_fabric_headers()
            if use_fabric
            else self.authenticator.get_powerbi_headers()
        )
        kwargs.setdefault("timeout", self.request_timeout)
        attempt = 0
        backoff = 1
        while True:
            attempt += 1
            try:
                response = requests.request(method, url, headers=headers, **kwargs)
            except requests.RequestException as exc:  # pragma: no cover - network failure guardrail
                if attempt >= self.max_retries:
                    raise WorkspaceAccessError(f"Failed to call {url}: {exc}") from exc
                time.sleep(backoff)
                backoff *= 2
                continue

            if response.status_code in expected_status:
                return response

            if response.status_code == 429 and attempt < self.max_retries:
                retry_after = int(response.headers.get("Retry-After", backoff))
                self.logger.warning(f"Rate limited (429) on {url}. Retrying after {retry_after}s...")
                time.sleep(retry_after)
                continue

            if 500 <= response.status_code < 600 and attempt < self.max_retries:
                self.logger.warning(f"Server error {response.status_code} on {url}. Retrying in {backoff}s...")
                time.sleep(backoff)
                backoff *= 2
                continue

            error = WorkspaceAccessError(
                f"{method} {url} failed with status {response.status_code}: {response.text}"
            )
            setattr(error, "status_code", response.status_code)
            raise error


__all__ = [
    "AccessRequirement",
    "SuppressionRules",
    "WorkspaceAccessEnforcer",
    "WorkspaceAccessError",
]
