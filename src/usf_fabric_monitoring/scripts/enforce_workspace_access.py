"""
Workspace Access Enforcement CLI

Ensures critical security groups are assigned to every tenant workspace.
Supports both "assess" (dry-run) and "enforce" (apply changes) modes.

Usage:
    python enforce_workspace_access.py --mode assess
    python enforce_workspace_access.py --mode enforce --confirm
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Iterable, Optional

from dotenv import load_dotenv

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parents[2]))

from usf_fabric_monitoring.core.workspace_access_enforcer import WorkspaceAccessEnforcer, WorkspaceAccessError
from usf_fabric_monitoring.core.logger import setup_logging

PROJECT_ROOT = Path(__file__).resolve().parent


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Enforce security group assignment across Microsoft Fabric workspaces"
    )
    default_targets = PROJECT_ROOT / "config" / "workspace_access_targets.json"
    default_suppress = PROJECT_ROOT / "config" / "workspace_access_suppressions.json"
    default_output = Path(os.getenv("EXPORT_DIRECTORY", "exports/monitor_hub_analysis"))

    parser.add_argument(
        "--targets-file",
        type=Path,
        default=default_targets,
        help="Path to the JSON file that lists required security groups",
    )
    parser.add_argument(
        "--suppress-file",
        type=Path,
        default=default_suppress,
        help="Optional JSON file with workspace suppressions",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=default_output,
        help="Directory where enforcement summaries should be written",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate the enforcement without making changes",
    )
    parser.add_argument(
        "--workspace",
        action="append",
        help="Limit enforcement to the provided workspace ID or name (can be repeated)",
    )
    parser.add_argument(
        "--max-workspaces",
        type=int,
        default=None,
        help="Stop after evaluating the specified number of workspaces",
    )
    parser.add_argument(
        "--api-preference",
        choices=["auto", "fabric", "powerbi"],
        default=os.getenv("FABRIC_ENFORCER_API_PREFERENCE", "auto"),
        help="Control whether Fabric, Power BI, or both admin APIs are used",
    )
    parser.add_argument(
        "--log-level",
        default=os.getenv("FABRIC_ENFORCER_LOG_LEVEL", "INFO"),
        help="Python logging level (default: INFO)",
    )
    parser.add_argument(
        "--mode",
        choices=["assess", "enforce"],
        default=os.getenv("FABRIC_ENFORCER_MODE", "assess"),
        help="assess: force dry run and only produce reports; enforce: requires --confirm to apply changes",
    )
    parser.add_argument(
        "--confirm",
        action="store_true",
        help="Required acknowledgement when --mode enforce is used without --dry-run",
    )
    parser.add_argument(
        "--summary-format",
        choices=["json", "text"],
        default=os.getenv("FABRIC_ENFORCER_SUMMARY_FORMAT", "text"),
        help="Emit summary metrics as JSON (stdout only) or text (default text + JSON file)",
    )
    parser.add_argument(
        "--csv-summary",
        action="store_true",
        help="Write a CSV file listing every workspace, compliance status, and detected actions",
    )
    parser.add_argument(
        "--include-personal-workspaces",
        action="store_true",
        help="Include personal workspaces (type=PersonalGroup) in enforcement. Default: exclude them to reduce API calls and rate limits.",
    )
    parser.add_argument(
        "--fabric-only",
        action="store_true",
        help="Only enforce on Fabric/Premium workspaces (workspaces assigned to a capacity).",
    )

    return parser.parse_args(argv)


def write_summary(summary: dict, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = destination / f"workspace_access_enforcement_{timestamp}.json"
    output_file.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return output_file


def write_csv_summary(summary: dict, destination: Path) -> Path:
    destination.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    output_file = destination / f"workspace_access_enforcement_{timestamp}.csv"
    headers = [
        "workspace_id",
        "workspace_name",
        "status",
        "needs_action",
        "actions",
    ]
    
    rows = []
    for item in summary.get("actions", []):
        workspace = item.get("workspace", {})
        status = item.get("status", "unknown")
        actions = item.get("actions", [])
        
        needs_action = any(
            a.get("action") not in ["already_compliant"] for a in actions
        )
        
        action_summary = "; ".join(
            f"{a.get('group')}: {a.get('action')}"
            for a in actions
        )
        
        rows.append({
            "workspace_id": workspace.get("id", ""),
            "workspace_name": workspace.get("name", ""),
            "status": status,
            "needs_action": str(needs_action).lower(),
            "actions": action_summary,
        })
    
    import csv
    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    
    return output_file


def derive_report_metrics(summary: dict) -> dict:
    total_workspaces = summary.get("workspace_count", 0)
    compliant = 0
    needs_action = 0
    suppressed = 0
    skipped = 0

    for item in summary.get("actions", []):
        status = item.get("status")
        actions = item.get("actions", [])

        if status == "suppressed":
            suppressed += 1
        elif status == "skipped":
            skipped += 1
        elif all(a.get("action") == "already_compliant" for a in actions):
            compliant += 1
        else:
            needs_action += 1

    return {
        "total_workspaces": total_workspaces,
        "compliant_workspaces": compliant,
        "workspaces_needing_action": needs_action,
        "suppressed_workspaces": suppressed,
        "skipped_workspaces": skipped,
    }


def main(argv: Optional[Iterable[str]] = None) -> int:
    load_dotenv()
    args = parse_args(argv)
    
    logger = setup_logging(
        name="workspace_enforcer",
        level=getattr(logging, args.log_level.upper(), logging.INFO)
    )

    try:
        effective_dry_run = args.dry_run
        if args.mode == "assess":
            if not effective_dry_run:
                logger.info("Assess mode detected; forcing dry-run execution")
            effective_dry_run = True
        else:
            if effective_dry_run:
                logger.info("Enforce mode running with --dry-run; no changes will be made")
            elif not args.confirm:
                logger.error("Enforce mode requires --confirm (or run with --dry-run)")
                return 2

        requirements = WorkspaceAccessEnforcer.load_access_requirements(args.targets_file)
        suppressions = WorkspaceAccessEnforcer.load_suppressions(args.suppress_file)
        enforcer = WorkspaceAccessEnforcer(
            requirements,
            suppressions,
            api_preference=args.api_preference,
            dry_run=effective_dry_run,
            logger=logger,
            exclude_personal_workspaces=not args.include_personal_workspaces,
        )
        summary = enforcer.enforce(
            workspace_filter=args.workspace,
            max_workspaces=args.max_workspaces,
            fabric_only=args.fabric_only,
        )
        summary["mode"] = args.mode
        metrics = derive_report_metrics(summary)
        summary["metrics"] = metrics
        output_file = write_summary(summary, args.output_dir)
        csv_file = None
        if args.csv_summary:
            csv_file = write_csv_summary(summary, args.output_dir)
        logger.info("Enforcement summary captured at %s", output_file)
        if args.summary_format == "json":
            print(json.dumps(summary, indent=2))
        else:
            print("\n=== Workspace Access Summary ===")
            print(f"Mode: {summary['mode']} (dry_run={summary.get('dry_run')})")
            print(f"Total workspaces evaluated: {metrics['total_workspaces']}")
            print(f"  Compliant: {metrics['compliant_workspaces']}")
            print(f"  Needs action: {metrics['workspaces_needing_action']}")
            print(f"  Suppressed: {metrics['suppressed_workspaces']}")
            print(f"  Skipped: {metrics['skipped_workspaces']}")
            print(f"JSON artifact: {output_file}")
            if csv_file:
                print(f"CSV summary: {csv_file}")
        return 0
    except WorkspaceAccessError as exc:
        logger.error("Workspace access enforcement failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
