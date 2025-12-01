"""Utility helpers for enriching Fabric activity records."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional

_INFERENCE_RULES = None


def _load_inference_rules() -> Dict[str, Any]:
    """Load inference rules from JSON configuration."""
    global _INFERENCE_RULES
    if _INFERENCE_RULES is not None:
        return _INFERENCE_RULES

    try:
        # config/inference_rules.json is two levels up from src/core
        config_path = Path(__file__).resolve().parents[2] / "config" / "inference_rules.json"
        if config_path.exists():
            with open(config_path, "r", encoding="utf-8") as f:
                _INFERENCE_RULES = json.load(f)
        else:
            _INFERENCE_RULES = {}
    except Exception:
        _INFERENCE_RULES = {}

    return _INFERENCE_RULES


def _parse_datetime(value: Optional[str]) -> Optional[datetime]:
    """Parse an ISO8601 timestamp into a datetime instance."""
    if not value:
        return None
    try:
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        return datetime.fromisoformat(text)
    except Exception:
        return None


def compute_duration_seconds(activity: Dict[str, Any]) -> Optional[float]:
    """Best-effort duration calculation using duration fields or timestamps."""
    duration_ms = activity.get("DurationMs") or activity.get("Duration")
    if duration_ms is not None:
        try:
            return round(float(duration_ms) / 1000.0, 3)
        except (TypeError, ValueError):
            pass

    start = (
        _parse_datetime(activity.get("StartTime"))
        or _parse_datetime(activity.get("CreationTime"))
    )
    end = (
        _parse_datetime(activity.get("EndTime"))
        or _parse_datetime(activity.get("CompletionTime"))
        or _parse_datetime(activity.get("EndDateTime"))
    )

    if start and end:
        diff = (end - start).total_seconds()
        if diff >= 0:
            return round(diff, 3)
    
    # Fallback: If no end time, but we have start time, check for "Succeeded" status 
    # and maybe assume a minimal duration or leave as None. 
    # For now, we return None to indicate "unknown duration".
    return None


def normalize_status(status: Optional[str]) -> str:
    """Normalize activity status, defaulting to 'Succeeded' if missing."""
    if not status:
        return "Succeeded"
    
    s = str(status).strip().title()
    if s.lower() in ["", "none", "nan", "null"]:
        return "Succeeded"
    return s


def normalize_user(user_value: Optional[str]) -> Optional[str]:
    """Return the friendly portion of a UPN/email value."""
    if not user_value:
        return None
    value = str(user_value).strip()
    if "|" in value:
        value = value.split("|")[-1]
    if "@" in value:
        return value.split("@", 1)[0]
    return value or None


def extract_user_from_metadata(user_obj: Optional[Dict[str, Any]]) -> Optional[str]:
    """Pull the best available name from an item metadata user object."""
    if not user_obj:
        return None
    for key in ("displayName", "userPrincipalName", "email"):
        value = user_obj.get(key)
        if value:
            return normalize_user(value)
    return None


def infer_domain(name: Optional[str]) -> str:
    """Map an item/workspace name to a business domain bucket."""
    if not name:
        return "General"

    lowered = name.lower()
    rules = _load_inference_rules()
    domain_map = rules.get("domains", {})

    # Fallback if config is missing or empty
    if not domain_map:
        domain_map = {
            "Human Resources": ["hr", "human", "resource"],
            "Finance": ["finance", "financial", "budget"],
            "Sales": ["sales", "crm", "customer"],
            "Operations": ["ops", "operation", "admin"],
            "IT": ["it", "tech", "system"],
            "Analytics": ["analytics", "bi", "data"],
            "Development": ["dev", "test", "prod"],
        }

    for domain, keywords in domain_map.items():
        if any(keyword in lowered for keyword in keywords):
            return domain
    return "General"


def infer_location(workspace: Optional[Dict[str, Any]]) -> str:
    """Determine workspace location using metadata when possible."""
    if not workspace:
        return "Global"

    for key in ("region", "Region", "location", "Location"):
        value = workspace.get(key)
        if value:
            return value

    display_name = workspace.get("displayName") or workspace.get("name")
    if display_name:
        lowered = display_name.lower()
        rules = _load_inference_rules()
        location_map = rules.get("locations", {})

        # Use configured rules first
        for location, keywords in location_map.items():
            if any(keyword in lowered for keyword in keywords):
                return location

        # Fallback hardcoded logic if not matched or config missing
        if "emea" in lowered:
            return "EMEA"
        if any(k in lowered for k in ["us", "usa", "america"]):
            return "Americas"
        if any(k in lowered for k in ["apac", "asia", "pacific"]):
            return "APAC"
    return "Global"


def build_object_url(workspace_id: str, item_id: Optional[str], item_type: Optional[str]) -> Optional[str]:
    """Construct a Power BI / Fabric object URL when possible."""
    if not (workspace_id and item_id):
        return None

    base_url = "https://app.powerbi.com"
    type_map = {
        "Report": "reports",
        "Dashboard": "dashboards",
        "SemanticModel": "datasets",
        "Dataset": "datasets",
        "Lakehouse": "lakehouses",
        "Warehouse": "warehouses",
        "DataPipeline": "datapipelines",
        "Notebook": "notebooks",
        "Dataflow": "dataflows",
        "SparkJobDefinition": "items",
    }

    segment = type_map.get(item_type or "") or "items"
    return f"{base_url}/groups/{workspace_id}/{segment}/{item_id}"
