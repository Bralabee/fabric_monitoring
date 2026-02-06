import argparse
import csv
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

from dotenv import load_dotenv

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parents[2]))

from usf_fabric_monitoring.core.workspace_access_enforcer import WorkspaceAccessEnforcer, WorkspaceAccessError

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    stream=sys.stdout
)
logger = logging.getLogger("sp_auditor")

class ServicePrincipalAuditor(WorkspaceAccessEnforcer):
    """
    Audits workspaces to check if the Service Principal is a member.
    Reuses fetching logic from WorkspaceAccessEnforcer.
    """
    def __init__(self, sp_id: str, **kwargs):
        # Remove access_requirements from kwargs if present to avoid conflict
        kwargs.pop('access_requirements', None)

        # Pass dummy requirement to satisfy base class validation
        from usf_fabric_monitoring.core.workspace_access_enforcer import AccessRequirement
        dummy = AccessRequirement(object_id="dummy", display_name="Audit", role="Viewer")

        super().__init__(access_requirements=[dummy], **kwargs)
        self.sp_id = sp_id.lower()

    def audit(self, max_workspaces: int | None = None) -> list[dict[str, Any]]:
        logger.info("="*60)
        logger.info("Starting Service Principal Access Audit")
        logger.info(f"Target SP Identifier: {self.sp_id}")
        logger.info("="*60)

        # 1. Fetch all workspaces
        try:
            all_workspaces = self._fetch_workspaces()
        except WorkspaceAccessError as e:
            logger.error(f"Failed to fetch workspaces: {e}")
            return []

        # Filter for Fabric workspaces only
        workspaces = [ws for ws in all_workspaces if self._is_fabric_workspace(ws)]

        logger.info(f"Total workspaces found: {len(all_workspaces)}")
        logger.info(f"Fabric/Premium workspaces: {len(workspaces)}")

        if not workspaces:
            logger.warning("No Fabric workspaces found to audit.")
            return []

        if max_workspaces:
            workspaces = workspaces[:max_workspaces]
            logger.info(f"Limiting audit to first {max_workspaces} workspaces")

        results = []

        # 2. Check each workspace
        for i, ws in enumerate(workspaces, 1):
            ws_id = ws.get("id")
            ws_name = ws.get("name")

            if i % 10 == 0:
                print(f"   ⏳ Audited {i}/{len(workspaces)} workspaces...", end='\r', flush=True)

            try:
                users = self._fetch_workspace_users(ws_id)

                # Check if SP is in users
                is_member = False
                current_role = "None"

                for user in users:
                    # Check all possible identifier fields
                    identifier = str(
                        user.get("identifier")
                        or user.get("objectId")
                        or user.get("userObjectId")
                        or user.get("graphId")
                        or ""
                    ).lower()

                    if identifier == self.sp_id:
                        is_member = True
                        current_role = user.get("groupUserAccessRight") or user.get("role")
                        break

                results.append({
                    "workspace_id": ws_id,
                    "workspace_name": ws_name,
                    "is_member": is_member,
                    "current_role": current_role,
                    "capacity_id": ws.get("capacityId"),
                    "source": ws.get("source")
                })

            except Exception as e:
                logger.warning(f"Failed to audit workspace {ws_name} ({ws_id}): {e}")
                results.append({
                    "workspace_id": ws_id,
                    "workspace_name": ws_name,
                    "is_member": False,
                    "current_role": "Error",
                    "error": str(e)
                })

        print(f"   ✅ Audited {len(workspaces)} workspaces total.          ")
        return results

def main():
    load_dotenv()

    parser = argparse.ArgumentParser(description="Audit Service Principal access to Fabric workspaces")
    parser.add_argument("--sp-id", help="Service Principal Object ID or Client ID (defaults to AZURE_CLIENT_ID)")
    parser.add_argument("--max-workspaces", type=int, help="Limit number of workspaces to audit")
    parser.add_argument("--output-dir", default="exports/audit", help="Output directory for report")
    args = parser.parse_args()

    # Determine SP ID
    sp_id = args.sp_id or os.getenv("AZURE_CLIENT_ID")
    if not sp_id:
        logger.error("No Service Principal ID provided. Set AZURE_CLIENT_ID env var or use --sp-id.")
        sys.exit(1)

    # Initialize Auditor
    # We pass dummy requirements because the base class expects them, but we won't use them.
    # We construct a dummy AccessRequirement just to satisfy the init.
    from usf_fabric_monitoring.core.workspace_access_enforcer import AccessRequirement
    dummy_req = AccessRequirement(object_id="dummy", display_name="dummy", role="Viewer")

    auditor = ServicePrincipalAuditor(
        sp_id=sp_id,
        access_requirements=[dummy_req],
        api_preference="auto", # Use auto to try both APIs
        exclude_personal_workspaces=True # Usually want to skip personal
    )

    # Run Audit
    results = auditor.audit(max_workspaces=args.max_workspaces)

    # Generate Report
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = output_dir / f"sp_access_audit_{timestamp}.csv"

    missing_count = 0

    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["workspace_id", "workspace_name", "is_member", "current_role", "capacity_id", "source", "error"])
        writer.writeheader()
        for row in results:
            writer.writerow(row)
            if not row["is_member"]:
                missing_count += 1

    logger.info("="*60)
    logger.info("Audit Complete.")
    logger.info(f"Total Workspaces: {len(results)}")
    logger.info(f"Access MISSING: {missing_count}")
    logger.info(f"Access CONFIRMED: {len(results) - missing_count}")
    logger.info(f"Report saved to: {csv_path}")
    logger.info("="*60)

if __name__ == "__main__":
    main()
