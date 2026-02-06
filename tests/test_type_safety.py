"""
Tests for Type Safety Utilities.

These tests verify defensive data handling for common production failure patterns.
"""

from datetime import datetime

import numpy as np
import pandas as pd

from usf_fabric_monitoring.core.type_safety import (
    coerce_surrogate_keys,
    ensure_columns_exist,
    microsecond_timestamps,
    safe_datetime,
    safe_int64,
    safe_string,
    safe_string_or_none,
    safe_workspace_lookup,
)


class TestSafeInt64:
    """Tests for safe_int64 function."""

    def test_integer_passthrough(self):
        """Integers should pass through unchanged."""
        assert safe_int64(42) == 42
        assert safe_int64(0) == 0
        assert safe_int64(-1) == -1

    def test_float_to_int(self):
        """Floats should convert to int."""
        assert safe_int64(42.0) == 42
        assert safe_int64(42.9) == 42  # Truncation

    def test_nan_returns_none(self):
        """NaN values should return None."""
        assert safe_int64(np.nan) is None
        assert safe_int64(float("nan")) is None

    def test_none_returns_none(self):
        """None should return None."""
        assert safe_int64(None) is None

    def test_inf_returns_none(self):
        """Infinity should return None."""
        assert safe_int64(float("inf")) is None
        assert safe_int64(float("-inf")) is None

    def test_string_numeric(self):
        """Numeric strings should convert."""
        assert safe_int64("123") == 123
        assert safe_int64("  456  ") == 456

    def test_string_invalid(self):
        """Non-numeric strings should return None."""
        assert safe_int64("abc") is None
        assert safe_int64("") is None

    def test_pandas_na(self):
        """Pandas NA should return None."""
        assert safe_int64(pd.NA) is None
        assert safe_int64(pd.NaT) is None


class TestCoerceSurrogateKeys:
    """Tests for coerce_surrogate_keys function."""

    def test_converts_sk_columns_to_int64(self):
        """Surrogate key columns should be converted to Int64."""
        df = pd.DataFrame({"user_sk": [1.0, 2.0, 3.0], "workspace_sk": [10, 20, 30], "other_col": ["a", "b", "c"]})

        result = coerce_surrogate_keys(df)

        assert result["user_sk"].dtype == pd.Int64Dtype()
        assert result["workspace_sk"].dtype == pd.Int64Dtype()
        assert result["other_col"].dtype == object

    def test_handles_null_in_sk_columns(self):
        """NULL values in SK columns should become pandas NA."""
        df = pd.DataFrame(
            {
                "user_sk": [1.0, np.nan, 3.0],
            }
        )

        result = coerce_surrogate_keys(df)

        assert result["user_sk"].dtype == pd.Int64Dtype()
        assert result["user_sk"].iloc[0] == 1
        assert pd.isna(result["user_sk"].iloc[1])
        assert result["user_sk"].iloc[2] == 3

    def test_does_not_modify_original_by_default(self):
        """Should return a copy by default."""
        df = pd.DataFrame({"user_sk": [1.0, 2.0]})
        original_dtype = df["user_sk"].dtype

        result = coerce_surrogate_keys(df)

        assert df["user_sk"].dtype == original_dtype
        assert result is not df


class TestSafeDatetime:
    """Tests for safe_datetime function."""

    def test_iso8601_with_z(self):
        """ISO8601 with Z suffix should parse."""
        result = safe_datetime("2024-01-15T10:30:00Z")
        assert result is not None
        assert result.year == 2024
        assert result.month == 1
        assert result.day == 15
        assert result.hour == 10
        assert result.minute == 30

    def test_iso8601_with_microseconds(self):
        """ISO8601 with microseconds should parse."""
        result = safe_datetime("2024-01-15T10:30:00.123456Z")
        assert result is not None
        assert result.microsecond == 123456

    def test_none_returns_none(self):
        """None should return None."""
        assert safe_datetime(None) is None

    def test_datetime_passthrough(self):
        """datetime objects should pass through."""
        dt = datetime(2024, 1, 15, 10, 30)
        result = safe_datetime(dt)
        assert result == dt

    def test_pandas_timestamp(self):
        """Pandas Timestamp should convert."""
        ts = pd.Timestamp("2024-01-15 10:30:00")
        result = safe_datetime(ts)
        assert result is not None
        assert result.year == 2024

    def test_invalid_string(self):
        """Invalid datetime strings should return None."""
        assert safe_datetime("not-a-date") is None
        assert safe_datetime("") is None


class TestSafeWorkspaceLookup:
    """Tests for safe_workspace_lookup function."""

    def test_id_lookup_priority(self):
        """ID lookup should take priority over name."""
        id_lookup = {"abc-123": "Workspace A"}
        name_lookup = {}

        result = safe_workspace_lookup(
            workspace_id="abc-123", workspace_name="Wrong Name", id_lookup=id_lookup, name_lookup=name_lookup
        )

        assert result == "Workspace A"

    def test_name_fallback(self):
        """Should fall back to workspace_name when ID not found."""
        result = safe_workspace_lookup(
            workspace_id="unknown", workspace_name="My Workspace", id_lookup={}, name_lookup={}
        )

        assert result == "My Workspace"

    def test_default_when_all_fail(self):
        """Should return default when both lookups fail."""
        result = safe_workspace_lookup(
            workspace_id=None, workspace_name=None, id_lookup={}, name_lookup={}, default="Fallback"
        )

        assert result == "Fallback"

    def test_handles_nan_workspace_id(self):
        """Should handle NaN workspace_id gracefully."""
        result = safe_workspace_lookup(workspace_id=np.nan, workspace_name="Valid Name", id_lookup={}, name_lookup={})

        assert result == "Valid Name"


class TestSafeString:
    """Tests for string handling functions."""

    def test_safe_string_normal(self):
        """Normal strings should pass through."""
        assert safe_string("hello") == "hello"
        assert safe_string("  spaces  ") == "spaces"

    def test_safe_string_none(self):
        """None should return default."""
        assert safe_string(None) == ""
        assert safe_string(None, default="N/A") == "N/A"

    def test_safe_string_nan(self):
        """NaN should return default."""
        assert safe_string(np.nan) == ""

    def test_safe_string_or_none_empty(self):
        """Empty strings should return None."""
        assert safe_string_or_none("") is None
        assert safe_string_or_none("   ") is None

    def test_safe_string_or_none_valid(self):
        """Valid strings should return stripped value."""
        assert safe_string_or_none("  hello  ") == "hello"


class TestDataFrameUtilities:
    """Tests for DataFrame utility functions."""

    def test_ensure_columns_exist_adds_missing(self):
        """Missing columns should be added with fill value."""
        df = pd.DataFrame({"existing": [1, 2, 3]})

        result = ensure_columns_exist(df, ["existing", "new_col"], fill_value=0)

        assert "new_col" in result.columns
        assert list(result["new_col"]) == [0, 0, 0]

    def test_ensure_columns_exist_preserves_existing(self):
        """Existing columns should be preserved."""
        df = pd.DataFrame({"existing": [1, 2, 3]})

        result = ensure_columns_exist(df, ["existing"], fill_value=0)

        assert list(result["existing"]) == [1, 2, 3]

    def test_microsecond_timestamps(self):
        """Datetime columns should be truncated to microseconds."""
        df = pd.DataFrame({"dt": pd.to_datetime(["2024-01-15T10:30:00.123456789"]), "other": [1]})

        result = microsecond_timestamps(df)

        # Nanoseconds should be truncated
        assert result["dt"].iloc[0].nanosecond == 0
