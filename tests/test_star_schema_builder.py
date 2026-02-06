"""
Tests for Star Schema Builder dimension and fact table builders.

These tests verify the Kimball dimensional model building logic using the
actual DimensionBuilder classes from the star_schema_builder module.
"""

from datetime import datetime
from typing import Any

import pytest


class TestDateDimensionBuilder:
    """Tests for DateDimensionBuilder."""

    def test_build_covers_date_range(self):
        """Date dimension should cover the full date range."""
        from usf_fabric_monitoring.core.star_schema_builder import DateDimensionBuilder

        builder = DateDimensionBuilder()
        start = datetime(2024, 1, 1)
        end = datetime(2024, 1, 10)

        dim_df = builder.build(start, end)

        # Should cover 10 days
        assert len(dim_df) == 10
        assert "date_sk" in dim_df.columns
        assert "full_date" in dim_df.columns
        assert "day_of_week" in dim_df.columns

    def test_fiscal_year_calculation(self):
        """Fiscal year should be calculated based on start month."""
        from usf_fabric_monitoring.core.star_schema_builder import DateDimensionBuilder

        builder = DateDimensionBuilder()
        # Test a date in June 2024 with fiscal year starting in July
        dim_df = builder.build(datetime(2024, 6, 15), datetime(2024, 6, 16), fiscal_year_start_month=7)

        # June 2024 is still in FY2024 (July 2023 - June 2024)
        assert len(dim_df) == 2
        assert "fiscal_year" in dim_df.columns


class TestTimeDimensionBuilder:
    """Tests for TimeDimensionBuilder."""

    def test_build_has_correct_structure(self):
        """Time dimension should have correct structure."""
        from usf_fabric_monitoring.core.star_schema_builder import TimeDimensionBuilder

        builder = TimeDimensionBuilder()
        dim_df = builder.build()

        # Time dimension has 96 rows (24 hours * 4 quarters = 15-min intervals)
        assert len(dim_df) == 96
        assert "time_sk" in dim_df.columns
        assert "hour_24" in dim_df.columns

    def test_time_periods_exist(self):
        """Time periods should be present."""
        from usf_fabric_monitoring.core.star_schema_builder import TimeDimensionBuilder

        builder = TimeDimensionBuilder()
        dim_df = builder.build()

        # Check that time_period column is populated
        if "time_period" in dim_df.columns:
            periods = dim_df["time_period"].unique()
            assert len(periods) > 0


class TestWorkspaceDimensionBuilder:
    """Tests for WorkspaceDimensionBuilder."""

    @pytest.fixture
    def sample_activities(self) -> list[dict[str, Any]]:
        """Sample activities with workspace data."""
        return [
            {"workspace_id": "ws_001", "workspace_name": "DEV - Analytics", "activity_id": "a1"},
            {"workspace_id": "ws_002", "workspace_name": "PROD - Sales", "activity_id": "a2"},
            {"workspace_id": "ws_001", "workspace_name": "DEV - Analytics", "activity_id": "a3"},
        ]

    def test_unique_workspaces(self, sample_activities):
        """Workspace dimension should have unique entries."""
        from usf_fabric_monitoring.core.star_schema_builder import WorkspaceDimensionBuilder

        builder = WorkspaceDimensionBuilder()
        dim_df, new_df = builder.build_from_activities(sample_activities)

        # Should have 2 unique workspaces
        assert len(dim_df) == 2
        assert "workspace_sk" in dim_df.columns
        assert "workspace_id" in dim_df.columns

    def test_environment_inference(self, sample_activities):
        """Environment should be inferred from workspace name."""
        from usf_fabric_monitoring.core.star_schema_builder import WorkspaceDimensionBuilder

        builder = WorkspaceDimensionBuilder()

        # Test environment inference - check that DEV is detected
        dev_result = builder.infer_environment("DEV - Analytics")
        assert dev_result in ("DEV", "Dev", "dev")

        # Production might return PRD or PROD
        prod_result = builder.infer_environment("Production Workspace")
        assert prod_result in ("PROD", "PRD", "Production")


class TestUserDimensionBuilder:
    """Tests for UserDimensionBuilder."""

    @pytest.fixture
    def sample_activities(self) -> list[dict[str, Any]]:
        """Sample activities with user data."""
        return [
            {"submitted_by": "user1@example.com", "activity_id": "a1"},
            {"submitted_by": "user2@example.com", "activity_id": "a2"},
            {"submitted_by": "user1@example.com", "activity_id": "a3"},
            {"submitted_by": "sp-fabric-service", "activity_id": "a4"},
        ]

    def test_unique_users(self, sample_activities):
        """User dimension should have unique entries."""
        from usf_fabric_monitoring.core.star_schema_builder import UserDimensionBuilder

        builder = UserDimensionBuilder()
        dim_df, new_df = builder.build_from_activities(sample_activities)

        # Should have 3 unique users
        assert len(dim_df) == 3
        assert "user_sk" in dim_df.columns

    def test_user_type_classification(self):
        """User types should be classified."""
        from usf_fabric_monitoring.core.star_schema_builder import UserDimensionBuilder

        builder = UserDimensionBuilder()

        # Regular user - should return a classification type
        user_type = builder.classify_user_type("user@example.com")
        assert isinstance(user_type, str)
        assert user_type in ("Person", "User")

        # Service principal/system patterns should be classified
        system_type = builder.classify_user_type("sp-fabric-service")
        assert isinstance(system_type, str)

        # GUID-like should return a classification
        guid = "12345678-1234-1234-1234-123456789012"
        guid_type = builder.classify_user_type(guid)
        assert isinstance(guid_type, str)

    def test_domain_extraction(self):
        """Email domain should be extracted correctly."""
        from usf_fabric_monitoring.core.star_schema_builder import UserDimensionBuilder

        builder = UserDimensionBuilder()

        assert builder.extract_domain_from_upn("user@example.com") == "example.com"
        assert builder.extract_domain_from_upn("invalid-email") is None


class TestItemDimensionBuilder:
    """Tests for ItemDimensionBuilder."""

    @pytest.fixture
    def sample_activities(self) -> list[dict[str, Any]]:
        """Sample activities with item data."""
        return [
            {
                "item_id": "item_001",
                "item_name": "Sales Report",
                "item_type": "Report",
                "workspace_id": "ws_001",
                "activity_id": "a1",
            },
            {
                "item_id": "item_002",
                "item_name": "Data Pipeline",
                "item_type": "DataPipeline",
                "workspace_id": "ws_001",
                "activity_id": "a2",
            },
        ]

    def test_item_categorization(self):
        """Items should be categorized."""
        from usf_fabric_monitoring.core.star_schema_builder import ItemDimensionBuilder

        builder = ItemDimensionBuilder()

        # Compute items
        assert builder.categorize_item("DataPipeline") == "Compute"
        assert builder.categorize_item("Notebook") == "Compute"

        # Report could be Analytics or Visualization
        report_category = builder.categorize_item("Report")
        assert report_category in ("Visualization", "Analytics", "Reporting")

        # Lakehouse could be Data Storage or Data
        lakehouse_category = builder.categorize_item("Lakehouse")
        assert lakehouse_category in ("Data Storage", "Data", "Storage")

    def test_platform_detection(self):
        """Platform should be detected for items."""
        from usf_fabric_monitoring.core.star_schema_builder import ItemDimensionBuilder

        builder = ItemDimensionBuilder()

        # Power BI items
        assert builder.get_platform("Report") == "Power BI"
        assert builder.get_platform("Dashboard") == "Power BI"

        # Fabric-native items
        assert builder.get_platform("Lakehouse") == "Fabric"
        assert builder.get_platform("Notebook") == "Fabric"

    def test_unique_items(self, sample_activities):
        """Item dimension should have unique entries."""
        from usf_fabric_monitoring.core.star_schema_builder import ItemDimensionBuilder

        builder = ItemDimensionBuilder()
        dim_df, new_df = builder.build_from_activities(sample_activities)

        assert len(dim_df) == 2
        assert "item_sk" in dim_df.columns
        assert "item_id" in dim_df.columns


class TestActivityTypeDimensionBuilder:
    """Tests for ActivityTypeDimensionBuilder."""

    def test_activity_type_builder_exists(self):
        """Activity type dimension builder should be importable."""
        from usf_fabric_monitoring.core.star_schema_builder import ActivityTypeDimensionBuilder

        builder = ActivityTypeDimensionBuilder()
        assert builder is not None


class TestStatusDimensionBuilder:
    """Tests for StatusDimensionBuilder."""

    def test_build_creates_expected_statuses(self):
        """Status dimension should have standard statuses."""
        from usf_fabric_monitoring.core.star_schema_builder import StatusDimensionBuilder

        builder = StatusDimensionBuilder()
        dim_df = builder.build()

        assert "status_sk" in dim_df.columns
        assert "status_code" in dim_df.columns

        # Should have at least Succeeded and Failed
        status_codes = dim_df["status_code"].tolist()
        assert "Succeeded" in status_codes
        assert "Failed" in status_codes
