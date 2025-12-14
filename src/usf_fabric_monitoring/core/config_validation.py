"""JSON config validation utilities.

These validators are intended to prevent silent misconfiguration while keeping
runtime behavior non-breaking.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional

from jsonschema import Draft7Validator


_INFERENCE_RULES_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "domains": {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
        "locations": {
            "type": "object",
            "additionalProperties": {
                "type": "array",
                "items": {"type": "string"},
            },
        },
    },
}


_WORKSPACE_ACCESS_TARGETS_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "description": {"type": "string"},
        "groups": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": True,
                "required": ["displayName", "objectId", "role"],
                "properties": {
                    "displayName": {"type": "string"},
                    "objectId": {"type": "string"},
                    "role": {"type": "string"},
                    "fabricPrincipalType": {"type": "string"},
                    "powerBiPrincipalType": {"type": "string"},
                },
            },
        },
    },
    "required": ["groups"],
}


_WORKSPACE_ACCESS_SUPPRESSIONS_SCHEMA: Dict[str, Any] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "description": {"type": "string"},
        "workspaceIds": {"type": "array", "items": {"type": "string"}},
        "workspaceNames": {"type": "array", "items": {"type": "string"}},
    },
}


SCHEMAS_BY_FILENAME: Mapping[str, Dict[str, Any]] = {
    "inference_rules.json": _INFERENCE_RULES_SCHEMA,
    "workspace_access_targets.json": _WORKSPACE_ACCESS_TARGETS_SCHEMA,
    "workspace_access_suppressions.json": _WORKSPACE_ACCESS_SUPPRESSIONS_SCHEMA,
}


def _load_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def validate_data(schema: Mapping[str, Any], data: Any) -> List[str]:
    validator = Draft7Validator(schema)
    errors = []
    for err in sorted(validator.iter_errors(data), key=str):
        loc = "/".join(str(p) for p in err.path) if err.path else "(root)"
        errors.append(f"{loc}: {err.message}")
    return errors


def validate_file(path: Path) -> List[str]:
    schema = SCHEMAS_BY_FILENAME.get(path.name)
    if not schema:
        return []
    data = _load_json(path)
    return validate_data(schema, data)


def validate_config_dir(config_dir: Path, *, only_known_files: bool = True) -> Dict[str, List[str]]:
    results: Dict[str, List[str]] = {}
    if not config_dir.exists():
        return results

    candidates = list(config_dir.glob("*.json"))
    for path in candidates:
        if only_known_files and path.name not in SCHEMAS_BY_FILENAME:
            continue
        errors = validate_file(path)
        if errors:
            results[str(path)] = errors
    return results
