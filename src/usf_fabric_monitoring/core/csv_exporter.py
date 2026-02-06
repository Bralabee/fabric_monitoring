"""
CSV Export Module for Fabric Monitoring Data
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd


class CSVExporter:
    """Handles exporting Fabric monitoring data to CSV files"""

    def __init__(self, export_base_path: str = "./exports"):
        """Initialize CSV exporter."""
        self.export_base_path = Path(export_base_path)
        self.logger = logging.getLogger(__name__)

        # Create export directory structure
        self.export_base_path.mkdir(parents=True, exist_ok=True)
        self.daily_path = self.export_base_path / "daily"
        self.summary_path = self.export_base_path / "summary"

        for path in [self.daily_path, self.summary_path]:
            path.mkdir(parents=True, exist_ok=True)

    def export_daily_activities(
        self, activities: list[dict[str, Any]], date: datetime, custom_suffix: str = None
    ) -> str:
        """Export daily activities to CSV file."""
        if not activities:
            self.logger.warning(f"No activities to export for {date.strftime('%Y-%m-%d')}")
            return None

        # Use custom suffix if provided, otherwise use default date format
        if custom_suffix:
            filename = f"fabric_activities_{custom_suffix}.csv"
        else:
            filename = f"fabric_activities_{date.strftime('%Y%m%d')}.csv"
        file_path = self.daily_path / filename

        try:
            # Convert to DataFrame and normalize
            df = pd.DataFrame(activities)
            df_clean = self._normalize_activities_data(df)

            # Export to CSV
            df_clean.to_csv(file_path, index=False, encoding="utf-8")

            self.logger.info(
                f"Exported {len(activities)} daily activities for {date.strftime('%Y-%m-%d')} to {file_path}"
            )
            return str(file_path)

        except Exception as e:
            self.logger.error(f"Failed to export daily activities: {str(e)}")
            raise

    def export_activity_summary(
        self, activities: list[dict[str, Any]], date: datetime, custom_suffix: str = None
    ) -> str:
        """Export activity summary statistics to CSV."""
        if not activities:
            return None

        # Use custom suffix if provided, otherwise use default date format
        if custom_suffix:
            filename = f"fabric_summary_{custom_suffix}.csv"
        else:
            filename = f"fabric_summary_{date.strftime('%Y%m%d')}.csv"
        file_path = self.summary_path / filename

        try:
            df = pd.DataFrame(activities)
            summary_data = self._generate_activity_summary(df, date)

            summary_df = pd.DataFrame(summary_data)
            summary_df.to_csv(file_path, index=False, encoding="utf-8")

            self.logger.info(f"Exported daily summary for {date.strftime('%Y-%m-%d')} to {file_path}")
            return str(file_path)

        except Exception as e:
            self.logger.error(f"Failed to export activity summary: {str(e)}")
            raise

    def _normalize_activities_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize and clean activities data for CSV export."""
        df_clean = df.copy()

        # Standard columns to extract/normalize
        standard_columns = [
            "Id",
            "Activity",
            "ActivityId",
            "ItemName",
            "ItemType",
            "ItemId",
            "WorkspaceId",
            "WorkspaceName",
            "UserId",
            "UserKey",
            "SubmittedBy",
            "CreatedBy",
            "LastUpdatedBy",
            "CreationTime",
            "StartTime",
            "EndTime",
            "CompletionTime",
            "Status",
            "Duration",
            "DurationMs",
            "DurationSeconds",
            "RecordType",
            "UserType",
            "UserAgent",
            "ClientIP",
            "ObjectUrl",
            "Domain",
            "Location",
            "_workspace_id",
            "_workspace_name",
            "_extraction_date",
        ]

        normalized_data = {}

        for col in standard_columns:
            if col in df_clean.columns:
                normalized_data[col] = df_clean[col]
            else:
                # Try common alternative names
                alt_names = {
                    "Activity": ["ActivityName", "Operation"],
                    "ItemName": ["ReportName", "DatasetName", "DashboardName"],
                    "WorkspaceId": ["GroupId"],
                    "WorkspaceName": ["GroupName"],
                    "UserId": ["UserPrincipalName", "User"],
                    "CreationTime": ["ActivityDateTime", "EventDateTime", "Timestamp"],
                    "StartTime": ["ActivityDateTime"],
                    "EndTime": ["CompletionTime"],
                    "Duration": ["DurationMs", "ExecutionTime"],
                }

                found = False
                for alt_col in alt_names.get(col, []):
                    if alt_col in df_clean.columns:
                        normalized_data[col] = df_clean[alt_col]
                        found = True
                        break

                if not found:
                    normalized_data[col] = None

        normalized_df = pd.DataFrame(normalized_data)

        # Clean up datetime columns
        datetime_columns = ["CreationTime", "StartTime", "EndTime", "_extraction_date"]
        for col in datetime_columns:
            if col in normalized_df.columns and normalized_df[col] is not None:
                try:
                    # Use ISO8601 format for parsing to avoid dateutil fallback warning
                    normalized_df[col] = pd.to_datetime(normalized_df[col], errors="coerce", format="ISO8601")
                    normalized_df[col] = normalized_df[col].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                except Exception:
                    # Fallback to mixed format parsing if ISO8601 fails
                    try:
                        normalized_df[col] = pd.to_datetime(normalized_df[col], errors="coerce", format="mixed")
                        normalized_df[col] = normalized_df[col].dt.strftime("%Y-%m-%d %H:%M:%S UTC")
                    except Exception:
                        pass

        # Clean up numeric columns
        numeric_columns = ["DurationMs", "Duration"]
        for col in numeric_columns:
            if col in normalized_df.columns:
                normalized_df[col] = pd.to_numeric(normalized_df[col], errors="coerce")

        # Remove entirely null columns
        normalized_df = normalized_df.dropna(axis=1, how="all")

        # Sort by creation time if available
        if "CreationTime" in normalized_df.columns:
            try:
                normalized_df = normalized_df.sort_values("CreationTime")
            except Exception:
                pass

        return normalized_df

    def _generate_activity_summary(self, df: pd.DataFrame, export_date: datetime) -> list[dict[str, Any]]:
        """Generate summary statistics from activities data."""
        summary_data = []

        # Overall summary
        total_activities = len(df)
        unique_users = df["UserId"].nunique() if "UserId" in df.columns else 0
        unique_workspaces = df["WorkspaceId"].nunique() if "WorkspaceId" in df.columns else 0

        summary_data.extend(
            [
                {
                    "summary_type": "overall",
                    "date": export_date.strftime("%Y-%m-%d"),
                    "metric": "total_activities",
                    "value": total_activities,
                    "details": f"{total_activities} total activities",
                },
                {
                    "summary_type": "overall",
                    "date": export_date.strftime("%Y-%m-%d"),
                    "metric": "unique_users",
                    "value": unique_users,
                    "details": f"{unique_users} unique users",
                },
                {
                    "summary_type": "overall",
                    "date": export_date.strftime("%Y-%m-%d"),
                    "metric": "unique_workspaces",
                    "value": unique_workspaces,
                    "details": f"{unique_workspaces} unique workspaces",
                },
            ]
        )

        # Activity type breakdown
        if "Activity" in df.columns:
            activity_counts = df["Activity"].value_counts()
            for activity_type, count in activity_counts.items():
                summary_data.append(
                    {
                        "summary_type": "by_activity_type",
                        "date": export_date.strftime("%Y-%m-%d"),
                        "metric": f"activity_{activity_type}",
                        "value": count,
                        "details": f"{count} {activity_type} activities",
                    }
                )

        # Status breakdown
        if "Status" in df.columns:
            status_counts = df["Status"].value_counts()
            for status, count in status_counts.items():
                summary_data.append(
                    {
                        "summary_type": "by_status",
                        "date": export_date.strftime("%Y-%m-%d"),
                        "metric": f"status_{status}",
                        "value": count,
                        "details": f"{count} activities with status {status}",
                    }
                )

        # Duration statistics if available
        if "DurationMs" in df.columns:
            duration_data = df["DurationMs"].dropna()
            if len(duration_data) > 0:
                summary_data.extend(
                    [
                        {
                            "summary_type": "duration_stats",
                            "date": export_date.strftime("%Y-%m-%d"),
                            "metric": "avg_duration_ms",
                            "value": round(duration_data.mean(), 2),
                            "details": f"Average duration: {round(duration_data.mean(), 2)}ms",
                        },
                        {
                            "summary_type": "duration_stats",
                            "date": export_date.strftime("%Y-%m-%d"),
                            "metric": "max_duration_ms",
                            "value": duration_data.max(),
                            "details": f"Maximum duration: {duration_data.max()}ms",
                        },
                    ]
                )

        return summary_data

    def get_export_file_info(self, export_date: datetime) -> dict[str, Any]:
        """Get information about exported files for a specific date."""
        date_str = export_date.strftime("%Y%m%d")

        file_info = {"export_date": export_date.strftime("%Y-%m-%d"), "files": {}}

        # Check for daily activities file
        daily_file = self.daily_path / f"fabric_activities_{date_str}.csv"
        if daily_file.exists():
            file_info["files"]["daily_activities"] = {
                "path": str(daily_file),
                "size_bytes": daily_file.stat().st_size,
                "created": datetime.fromtimestamp(daily_file.stat().st_ctime),
            }

        # Check for summary file
        summary_file = self.summary_path / f"fabric_summary_{date_str}.csv"
        if summary_file.exists():
            file_info["files"]["summary"] = {
                "path": str(summary_file),
                "size_bytes": summary_file.stat().st_size,
                "created": datetime.fromtimestamp(summary_file.stat().st_ctime),
            }

        return file_info
