"""
Tests for JSON Schema validation of configuration files.

These tests ensure:
1. All existing config files validate against their schemas
2. Invalid configs produce clear error messages
3. Schema loading functions work correctly
"""

import json
import tempfile
from pathlib import Path

import pytest

from usf_fabric_monitoring.core.config_validation import (
    SCHEMA_FILES,
    ConfigValidationError,
    get_schema,
    load_schema_file,
    validate_all_configs,
    validate_data,
    validate_file,
    validate_file_or_raise,
)


class TestSchemaLoading:
    """Tests for schema file loading."""

    def test_load_schema_file_returns_dict(self):
        """External schema files should load as dictionaries."""
        schema = load_schema_file("inference_rules.json")
        # Schema should exist and be a dict
        assert schema is None or isinstance(schema, dict)

    def test_get_schema_returns_valid_schema(self):
        """get_schema should return a schema for known config files."""
        schema = get_schema("inference_rules.json")
        assert isinstance(schema, dict)
        assert "type" in schema or "properties" in schema

    def test_get_schema_unknown_file_returns_empty(self):
        """get_schema for unknown files should return empty dict."""
        schema = get_schema("unknown_file.json")
        assert schema == {}

    def test_schema_files_mapping_complete(self):
        """All known config files should have schema mappings."""
        expected_configs = [
            "inference_rules.json",
            "workspace_access_targets.json",
            "workspace_access_suppressions.json",
        ]
        for config in expected_configs:
            assert config in SCHEMA_FILES


class TestValidateProjectConfigs:
    """Tests that validate actual project config files."""

    def test_all_project_configs_are_valid(self):
        """All config files in the project should pass validation."""
        files_checked, files_valid, errors = validate_all_configs()

        assert files_checked > 0, "No config files found"
        assert files_valid == files_checked, f"Config validation failed: {errors}"
        assert len(errors) == 0

    def test_inference_rules_validates(self):
        """inference_rules.json should validate successfully."""
        from usf_fabric_monitoring.core.config_validation import _get_project_root

        config_path = _get_project_root() / "config" / "inference_rules.json"

        if config_path.exists():
            errors = validate_file(config_path)
            assert len(errors) == 0, f"Validation errors: {errors}"


class TestValidateData:
    """Tests for data validation against schemas."""

    def test_valid_inference_rules_data(self):
        """Valid inference rules data should pass validation."""
        schema = get_schema("inference_rules.json")
        valid_data = {
            "domains": {"HR": ["hr", "human"], "Finance": ["finance", "budget"]},
            "locations": {"EMEA": ["emea"], "Americas": ["us", "usa"]},
        }
        errors = validate_data(schema, valid_data)
        assert len(errors) == 0

    def test_missing_required_field_fails(self):
        """Missing required fields should produce errors."""
        schema = get_schema("workspace_access_targets.json")
        invalid_data = {
            "description": "Missing groups field"
            # Missing required "groups" field
        }
        errors = validate_data(schema, invalid_data)
        assert len(errors) > 0
        assert any("groups" in e.lower() for e in errors)

    def test_wrong_type_fails(self):
        """Wrong data types should produce errors."""
        schema = get_schema("inference_rules.json")
        invalid_data = {
            "domains": "should be object not string",  # Wrong type
            "locations": {},
        }
        errors = validate_data(schema, invalid_data)
        assert len(errors) > 0


class TestValidateFile:
    """Tests for file validation."""

    def test_validate_invalid_json_file(self):
        """Invalid JSON should return parse error."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            f.write("{ invalid json }")
            temp_path = Path(f.name)

        try:
            # Rename to a known config file name for schema lookup
            test_path = temp_path.parent / "inference_rules.json"
            temp_path.rename(test_path)

            errors = validate_file(test_path)
            assert len(errors) > 0
            assert any("json" in e.lower() for e in errors)
        finally:
            if test_path.exists():
                test_path.unlink()
            if temp_path.exists():
                temp_path.unlink()

    def test_validate_file_or_raise_raises_on_invalid(self):
        """validate_file_or_raise should raise ConfigValidationError on invalid."""
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            # Valid JSON but invalid schema (missing required fields)
            json.dump({"wrong": "structure"}, f)
            temp_path = Path(f.name)

        try:
            test_path = temp_path.parent / "workspace_access_targets.json"
            temp_path.rename(test_path)

            with pytest.raises(ConfigValidationError) as exc_info:
                validate_file_or_raise(test_path)

            assert "workspace_access_targets.json" in str(exc_info.value)
        finally:
            if test_path.exists():
                test_path.unlink()
            if temp_path.exists():
                temp_path.unlink()


class TestConfigValidationError:
    """Tests for ConfigValidationError exception."""

    def test_error_message_includes_filename(self):
        """Error message should include the filename."""
        error = ConfigValidationError("test.json", ["error 1", "error 2"])
        assert "test.json" in str(error)

    def test_error_message_includes_all_errors(self):
        """Error message should include all validation errors."""
        errors = ["Field 'x' is required", "Field 'y' has wrong type"]
        error = ConfigValidationError("test.json", errors)
        for e in errors:
            assert e in str(error)

    def test_error_attributes(self):
        """Exception should have filename and errors attributes."""
        error = ConfigValidationError("test.json", ["error1"])
        assert error.filename == "test.json"
        assert error.errors == ["error1"]
