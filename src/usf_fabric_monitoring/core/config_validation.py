"""JSON config validation utilities.

These validators are intended to prevent silent misconfiguration while keeping
runtime behavior non-breaking. Supports both inline schemas (backward compatible)
and external JSON schema files for comprehensive validation.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from jsonschema import Draft7Validator

logger = logging.getLogger(__name__)

# =============================================================================
# SCHEMA FILE PATHS
# =============================================================================


def _get_project_root() -> Path:
    """Get project root directory, handling both installed and development contexts."""
    # Try to find project root by looking for config directory
    current = Path(__file__).resolve()
    for parent in [current] + list(current.parents):
        if (parent / "config").is_dir():
            return parent
        if (parent / "pyproject.toml").exists():
            return parent
    # Fallback: assume standard src layout
    return current.parent.parent.parent.parent


def _get_schemas_dir() -> Path:
    """Get the schemas directory path."""
    return _get_project_root() / "config" / "schemas"


# Schema file mapping
SCHEMA_FILES: dict[str, str] = {
    "inference_rules.json": "inference_rules.schema.json",
    "workspace_access_targets.json": "workspace_access_targets.schema.json",
    "workspace_access_suppressions.json": "workspace_access_suppressions.schema.json",
}


# =============================================================================
# INLINE SCHEMAS (Fallback for when external files not available)
# =============================================================================

_INFERENCE_RULES_SCHEMA: dict[str, Any] = {
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


_WORKSPACE_ACCESS_TARGETS_SCHEMA: dict[str, Any] = {
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


_WORKSPACE_ACCESS_SUPPRESSIONS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": True,
    "properties": {
        "description": {"type": "string"},
        "workspaceIds": {"type": "array", "items": {"type": "string"}},
        "workspaceNames": {"type": "array", "items": {"type": "string"}},
    },
}


# Inline schemas as fallback
_INLINE_SCHEMAS: Mapping[str, dict[str, Any]] = {
    "inference_rules.json": _INFERENCE_RULES_SCHEMA,
    "workspace_access_targets.json": _WORKSPACE_ACCESS_TARGETS_SCHEMA,
    "workspace_access_suppressions.json": _WORKSPACE_ACCESS_SUPPRESSIONS_SCHEMA,
}

# Public alias for backward compatibility
SCHEMAS_BY_FILENAME = _INLINE_SCHEMAS


# =============================================================================
# SCHEMA LOADING
# =============================================================================


def _load_json(path: Path) -> Any:
    """Load JSON file with proper encoding."""
    return json.loads(path.read_text(encoding="utf-8"))


def load_schema_file(config_filename: str) -> dict[str, Any] | None:
    """
    Load a JSON schema from the external schema file.

    Args:
        config_filename: Name of the config file (e.g., "inference_rules.json")

    Returns:
        Schema dictionary if found, None otherwise
    """
    schema_filename = SCHEMA_FILES.get(config_filename)
    if not schema_filename:
        return None

    schema_path = _get_schemas_dir() / schema_filename
    if not schema_path.exists():
        logger.debug(f"Schema file not found: {schema_path}, using inline schema")
        return None

    try:
        return _load_json(schema_path)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"Failed to load schema file {schema_path}: {e}")
        return None


def get_schema(config_filename: str) -> dict[str, Any]:
    """
    Get schema for a config file, preferring external schema file over inline.

    Args:
        config_filename: Name of the config file (e.g., "inference_rules.json")

    Returns:
        Schema dictionary (external if available, inline as fallback)
    """
    # Try external schema first
    external_schema = load_schema_file(config_filename)
    if external_schema:
        return external_schema

    # Fall back to inline schema
    return _INLINE_SCHEMAS.get(config_filename, {})


# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================


class ConfigValidationError(Exception):
    """Exception raised when config validation fails."""

    def __init__(self, filename: str, errors: list[str]):
        self.filename = filename
        self.errors = errors
        error_list = "\n  - ".join(errors)
        super().__init__(f"Config validation failed for {filename}:\n  - {error_list}")


def validate_data(schema: Mapping[str, Any], data: Any) -> list[str]:
    """
    Validate data against a JSON schema.

    Args:
        schema: JSON schema dictionary
        data: Data to validate

    Returns:
        List of error messages (empty if valid)
    """
    validator = Draft7Validator(schema)
    errors = []
    for err in sorted(validator.iter_errors(data), key=str):
        loc = "/".join(str(p) for p in err.path) if err.path else "(root)"
        errors.append(f"{loc}: {err.message}")
    return errors


def validate_file(path: Path, *, use_external_schema: bool = True) -> list[str]:
    """
    Validate a config file against its schema.

    Args:
        path: Path to the config file
        use_external_schema: If True, prefer external schema files

    Returns:
        List of error messages (empty if valid)
    """
    if use_external_schema:
        schema = get_schema(path.name)
    else:
        schema = _INLINE_SCHEMAS.get(path.name)

    if not schema:
        return []

    try:
        data = _load_json(path)
    except json.JSONDecodeError as e:
        return [f"Invalid JSON: {e}"]
    except OSError as e:
        return [f"Could not read file: {e}"]

    return validate_data(schema, data)


def validate_file_or_raise(path: Path, *, use_external_schema: bool = True) -> None:
    """
    Validate a config file and raise exception if invalid.

    Args:
        path: Path to the config file
        use_external_schema: If True, prefer external schema files

    Raises:
        ConfigValidationError: If validation fails
    """
    errors = validate_file(path, use_external_schema=use_external_schema)
    if errors:
        raise ConfigValidationError(path.name, errors)


def validate_config_dir(
    config_dir: Path, *, only_known_files: bool = True, use_external_schema: bool = True
) -> dict[str, list[str]]:
    """
    Validate all config files in a directory.

    Args:
        config_dir: Path to config directory
        only_known_files: If True, only validate files with known schemas
        use_external_schema: If True, prefer external schema files

    Returns:
        Dictionary mapping file paths to lists of errors (only files with errors)
    """
    results: dict[str, list[str]] = {}
    if not config_dir.exists():
        return results

    candidates = list(config_dir.glob("*.json"))
    for path in candidates:
        # Skip schema files themselves
        if path.parent.name == "schemas":
            continue
        if only_known_files and path.name not in SCHEMA_FILES:
            continue
        errors = validate_file(path, use_external_schema=use_external_schema)
        if errors:
            results[str(path)] = errors
    return results


def validate_all_configs(*, raise_on_error: bool = False) -> tuple[int, int, dict[str, list[str]]]:
    """
    Validate all configuration files in the project.

    Args:
        raise_on_error: If True, raise ConfigValidationError on first failure

    Returns:
        Tuple of (files_checked, files_valid, errors_by_file)
    """
    config_dir = _get_project_root() / "config"
    errors_by_file = validate_config_dir(config_dir, use_external_schema=True)

    files_checked = len(list(config_dir.glob("*.json")))
    files_valid = files_checked - len(errors_by_file)

    if raise_on_error and errors_by_file:
        first_file = next(iter(errors_by_file))
        raise ConfigValidationError(Path(first_file).name, errors_by_file[first_file])

    return files_checked, files_valid, errors_by_file


def print_validation_report(errors_by_file: dict[str, list[str]]) -> None:
    """Print a human-readable validation report."""
    if not errors_by_file:
        print("✅ All config files are valid")
        return

    print(f"❌ {len(errors_by_file)} config file(s) have validation errors:\n")
    for filepath, errors in errors_by_file.items():
        print(f"  📄 {Path(filepath).name}")
        for error in errors:
            print(f"     - {error}")
        print()
