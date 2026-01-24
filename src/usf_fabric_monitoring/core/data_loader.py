"""Helpers for reading enriched activity exports into analyzer-friendly records."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, List

import pandas as pd

RENAME_MAP = {
    "ActivityId": "activity_id",
    "Activity": "activity_type",
    "ItemId": "item_id",
    "ItemName": "item_name",
    "ItemType": "item_type",
    "WorkspaceId": "workspace_id",
    "WorkspaceName": "workspace_name",
    "_workspace_name": "workspace_name",
    "_workspace_id": "workspace_id",
    "SubmittedBy": "submitted_by",
    "CreatedBy": "created_by",
    "LastUpdatedBy": "last_updated_by",
    "Domain": "domain",
    "Location": "location",
    "ObjectUrl": "object_url",
    "Status": "status",
    "DurationSeconds": "duration_seconds",
    "CreationTime": "creation_time",
    "StartTime": "start_time",
    "EndTime": "end_time",
    "CompletionTime": "completion_time",
}

REQUIRED_COLUMNS = [
    "activity_id",
    "activity_type",
    "item_id",
    "item_name",
    "item_type",
    "workspace_id",
    "workspace_name",
    "submitted_by",
    "created_by",
    "last_updated_by",
    "domain",
    "location",
    "object_url",
    "status",
    "start_time",
    "end_time",
    "duration_seconds",
]


def load_activities_from_directory(export_dir: str) -> List[Dict[str, object]]:
    """Read all daily CSV exports within a directory and normalize column names."""
    base_path = Path(export_dir)
    daily_dir = base_path / "daily"
    if not daily_dir.exists():
        return []

    frames = []
    for csv_path in sorted(daily_dir.glob("fabric_activities_*.csv")):
        frames.append(pd.read_csv(csv_path))

    if not frames:
        return []

    df = pd.concat(frames, ignore_index=True)
    df = _rename_columns(df)
    df = _ensure_required_columns(df)
    df["duration_seconds"] = pd.to_numeric(df["duration_seconds"], errors="coerce").fillna(0)

    # Fallbacks for start/end time
    df["start_time"] = _coalesce_datetime(df, ["start_time", "creation_time"])
    df["end_time"] = _coalesce_datetime(df, ["end_time", "completion_time"])

    return df.to_dict(orient="records")


def _rename_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Rename columns while coalescing duplicates targeting the same field."""

    for source, target in RENAME_MAP.items():
        if source not in df.columns:
            continue

        if target in df.columns and target != source:
            # Prefer existing target data and only fill null gaps from the source column.
            df[target] = df[target].where(df[target].notna(), df[source])
            df = df.drop(columns=[source])
            continue

        df = df.rename(columns={source: target})

    return df


def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    for column in REQUIRED_COLUMNS:
        if column not in df.columns:
            df[column] = None
    return df


def _coalesce_datetime(df: pd.DataFrame, columns: List[str]) -> pd.Series:
    """Return the first non-null datetime string across provided columns."""
    series = None
    for column in columns:
        if column not in df.columns:
            continue
        candidate = df[column]
        if series is None:
            series = candidate
        else:
            series = series.fillna(candidate)
    if series is None:
        return pd.Series([None] * len(df))
    return series
