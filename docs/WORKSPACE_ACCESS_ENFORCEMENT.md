# Workspace Access Enforcement

This playbook automates the daily verification that mandatory Azure AD security groups keep the expected roles in every Microsoft Fabric workspace. It relies on the Fabric admin APIs when available and gracefully falls back to the Power BI admin endpoints whenever Fabric returns `404` or other transient failures.

## Prerequisites

- A service principal with Fabric administrator permissions (same credentials already stored in `.env`).
- `config/workspace_access_targets.json` listing the Azure AD group object IDs, target roles, and optional principal type overrides.
- (Optional) `config/workspace_access_suppressions.json` listing workspace IDs or names that should be skipped (sandboxes, labs, etc.).
- Updated `.env` entries for any custom timeout, retry, or API preference overrides (see `.env.template` under the *Workspace Access Enforcement* section).

## Running the enforcement locally

```bash
# Activate the Conda environment first if desired
conda activate fabric-monitoring

# Assess only (default). Forces dry-run and produces the checkpoint JSON
python enforce_workspace_access.py --mode assess

# Emit the same data in JSON on stdout (for dashboards)
python enforce_workspace_access.py --mode assess --summary-format json

# After reviewing the JSON, enforce with an explicit confirmation
python enforce_workspace_access.py --mode enforce --confirm

# Limit to specific workspaces (ID or friendly name)
python enforce_workspace_access.py --workspace 00000000-0000-0000-0000-000000000000 --workspace "Critical Workspace"
```

A convenience Make target is also available:

```bash
make enforce-access MODE=assess   # default behavior
make enforce-access MODE=enforce CONFIRM=1  # apply changes after approval
make enforce-access MODE=assess CSV_SUMMARY=1  # emit CSV alongside JSON checkpoint
```

Each run writes a timestamped JSON summary to `exports/monitor_hub_analysis/`, showing the evaluated workspaces, skipped entries, and any additions made. The JSON artifact is designed for downstream alerting (Logic Apps, Sentinel, etc.).

When `--csv-summary` (or `CSV_SUMMARY=1`) is supplied, the script stores a second artifact named `workspace_access_enforcement_<timestamp>.csv` in the same directory with columns for workspace ID, name, compliance status, and pending actions for easier spreadsheet reviews.

## Scheduling guidance

1. **Azure Automation / Functions** – Package the repository (or just `enforce_workspace_access.py` with `src/core/`) and execute daily with `python enforce_workspace_access.py`. Use managed identity for secrets and store suppress/target JSON files in Storage or Key Vault if you cannot bake them into the package.
2. **GitHub Actions** – Add a nightly workflow that checks out this repo, installs dependencies, and runs the script with `--dry-run` plus an `if` gate to fail when actions include unexpected additions. Upload the JSON artifact for auditing.
3. **Cron / Task Scheduler** – On self-hosted runners, invoke `make enforce-access` daily. Redirect STDOUT/ERR to your preferred log aggregator.

## Operational guardrails

- **Explicit assess/enforce checkpoint:** `--mode assess` forces a dry run. `--mode enforce --confirm` is the only combination that performs POSTs, giving you an approval step between detection and remediation.
- **Dry run first:** Even in enforce mode you can add `--dry-run` if you want a final rehearsal before confirming.
- **Pre-enforcement metrics:** Each run now prints a concise summary of total workspaces, compliant count, items needing action, suppressed, and skipped so you can quantify the blast radius before approving enforcement.
- **API preference:** Set `FABRIC_ENFORCER_API_PREFERENCE=powerbi` to stay on the legacy admin endpoint until Fabric APIs stabilize, or leave it at `auto` to leverage Fabric when possible.
- **Suppression list:** Keep lab and break-glass workspaces in the suppression file to avoid churn.
- **Alerting:** Watch for `role_mismatch` entries in the JSON summary – they indicate a workspace where the group exists but has the wrong role and currently needs manual promotion.

By running this daily job you maintain continuous evidence that the platform administrators and contributors retain the proper access level across every Fabric workspace, reducing the risk of undetected access drift.
