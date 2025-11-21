# Tenant-Wide Monitoring Implementation

## Overview

This document describes the tenant-wide monitoring implementation that enables comprehensive activity tracking across all workspaces in the Fabric tenant.

## Architecture Changes

### Before (Member-Only)
- **API Strategy**: Per-workspace loop using `/v1/workspaces` endpoint
- **Scope**: Only workspaces where service principal is a member (~139 workspaces)
- **Performance**: 139 separate API calls, manageable but incomplete coverage
- **Use Case**: Personal workspace monitoring

### After (Tenant-Wide)
- **API Strategy**: Single Power BI Admin API call using `/v1.0/myorg/admin/activityevents`
- **Scope**: All tenant workspaces excluding personal workspaces (~2187 shared workspaces)
- **Performance**: 1 API call per day, fast and efficient
- **Use Case**: Comprehensive tenant-wide monitoring and governance

## Implementation Details

### 1. New Methods in `extractor.py`

#### `get_tenant_wide_activities()`
```python
def get_tenant_wide_activities(start_date, end_date, workspace_ids=None, activity_types=None)
```
- Fetches ALL tenant activities in a single Power BI Admin API call
- Supports post-fetch filtering by workspace IDs and activity types
- Handles rate limiting with proper error messages

#### Updated `get_daily_activities()`
```python
def get_daily_activities(date, workspace_ids=None, activity_types=None, tenant_wide=True)
```
- **tenant_wide=True**: Uses single Power BI Admin API call (default)
- **tenant_wide=False**: Falls back to per-workspace loop (legacy behavior)

### 2. Updated Scripts

All extraction scripts now use tenant-wide monitoring by default:

- **`extract_historical_data.py`**: Multi-day extraction (up to 28 days)
- **`extract_daily_data.py`**: Single-day extraction (Real API calls)
- **`monitor_hub_pipeline.py`**: Complete analysis pipeline

### 3. CLI Updates

#### Monitor Hub Pipeline
```bash
# Tenant-wide monitoring (default, ~2187 workspaces)
python monitor_hub_pipeline.py --days 7

# Member-only monitoring (~139 workspaces)
python monitor_hub_pipeline.py --days 7 --member-only
```

#### Makefile
```bash
# Tenant-wide monitoring
make monitor-hub DAYS=7

# Member-only monitoring
make monitor-hub DAYS=7 MEMBER_ONLY=1

# Custom output directory
make monitor-hub DAYS=30 OUTPUT_DIR=exports/custom
```

### 4. Workspace Access Enforcement

The enforcement tool now supports tenant-wide workspace enumeration:

```bash
# Assess all tenant workspaces
make enforce-access MODE=assess CSV_SUMMARY=1

# Enforce with specific API preference
make enforce-access MODE=enforce CONFIRM=1 API_PREFERENCE=powerbi

# Limit workspaces for testing
make enforce-access MODE=assess MAX_WORKSPACES=10
```

## Benefits

### 1. Complete Coverage
- Monitors ALL shared workspaces in the tenant
- No blind spots from missing workspace memberships
- Ensures compliance across entire organization

### 2. Performance
- **Before**: 2187 separate API calls (one per workspace) - hours to complete
- **After**: 1 API call per day - completes in seconds
- Dramatically reduces API rate limit concerns

### 3. Consistency
- Both monitoring and enforcement use the same tenant-wide approach
- Unified workspace inventory across all tools
- Consistent security policy application

### 4. Flexibility
- Tenant-wide is default behavior
- `--member-only` flag available for backward compatibility
- Easy to switch between modes based on use case

## Technical Details

### Power BI Admin Activity API

**Endpoint**: `GET https://api.powerbi.com/v1.0/myorg/admin/activityevents`

**Parameters**:
- `startDateTime`: ISO format with quotes (e.g., `'2025-11-18T00:00:00'`)
- `endDateTime`: ISO format with quotes (e.g., `'2025-11-18T23:59:59'`)

**Returns**: All activity events across all tenant workspaces in a single response

**Rate Limits**: 
- Subject to Power BI API rate limits
- Retry-After header indicates wait time (typically 30-60 minutes)
- Personal workspace exclusion reduces payload size significantly

### Workspace Enumeration

**Endpoint**: `GET https://api.powerbi.com/v1.0/myorg/admin/groups`

**Query Parameters**:
- `$top=200`: Page size
- `$skip=N`: Pagination offset
- `$filter=type ne 'PersonalGroup'`: Exclude personal workspaces (reduces from ~5000 to ~286)

**Returns**: All shared workspaces in the tenant with pagination support

## Migration Guide

### For Existing Scripts

No code changes required! All scripts automatically use tenant-wide monitoring.

If you need member-only behavior:

```python
# Python code
activities = extractor.get_daily_activities(
    date=target_date,
    tenant_wide=False  # Use member-only mode
)
```

```bash
# CLI
python monitor_hub_pipeline.py --days 7 --member-only
```

### For Enforcement

The enforcement tool automatically uses tenant-wide workspace enumeration:

```bash
# This now checks ALL 2187 shared workspaces
python enforce_workspace_access.py --mode assess
```

## Troubleshooting

### Issue: Rate Limit Exceeded (429)

**Symptom**: `Rate limit exceeded. Retry after XXXX seconds.`

**Solution**:
1. Wait for the specified retry period
2. Exclude personal workspaces (already implemented)
3. Request Fabric Administrator role to use Fabric Admin API instead

### Issue: No Activities Returned

**Symptom**: 0 activities retrieved for date range

**Possible Causes**:
1. No actual activities occurred on that date
2. Service principal lacks permissions
3. Date range outside activity retention period (typically 30 days)

**Solution**:
- Verify service principal has Tenant.Read.All or Power BI Admin role
- Check date range is within last 30 days
- Test with a known active date

### Issue: Slow Performance

**Symptom**: Extraction takes a long time

**Possible Causes**:
1. Large date range (many days)
2. High volume of activities
3. Network latency

**Solution**:
- Reduce date range
- Run during off-peak hours
- Use workspace filtering if monitoring specific workspaces

## API Permissions

### Required Permissions

For tenant-wide monitoring, your service principal needs:

**Option 1: Power BI Admin API** (Current Implementation)
- Azure AD App Registration with `Tenant.Read.All` (Application permission)
- OR Power BI Administrator role in Entra ID

**Option 2: Fabric Admin API** (Alternative)
- Fabric Administrator role in Entra ID
- Service principal API access enabled in Fabric Admin Portal

### Verification

Check current permissions:
```bash
# Test workspace enumeration
python -c "
from src.core.auth import create_authenticator_from_env
from src.core.extractor import FabricDataExtractor
auth = create_authenticator_from_env()
extractor = FabricDataExtractor(auth)
workspaces = extractor.get_workspaces(tenant_wide=True)
print(f'Found {len(workspaces)} workspaces')
"
```

Expected output: `Found 2187 workspaces` (or similar count)

## Best Practices

### 1. Use Tenant-Wide for Governance
- Security group enforcement
- Compliance auditing
- Organization-wide reporting

### 2. Use Member-Only for Personal Monitoring
- Individual workspace activity tracking
- Development/testing workspaces
- Reduced API quota consumption

### 3. Filter Personal Workspaces
- Always exclude personal workspaces for tenant-wide operations
- Reduces from ~5000 to ~286 workspaces
- Significantly improves API performance

### 4. Monitor Rate Limits
- Log Retry-After headers
- Implement exponential backoff
- Consider caching workspace lists

## Related Documentation

- [Workspace API Comparison](./docs/WORKSPACE_API_COMPARISON.md)
- [Why Monitoring Works But Enforcement Fails](./docs/WHY_MONITORING_WORKS_BUT_ENFORCEMENT_FAILS.md)
- [Workspace Access Enforcement Guide](./WORKSPACE_ACCESS_ENFORCEMENT.md)

## Changelog

### Version 2.0 (Current)
- Implemented tenant-wide activity monitoring using Power BI Admin API
- Added `get_tenant_wide_activities()` method
- Updated all extraction scripts to use tenant-wide by default
- Added `--member-only` flag for backward compatibility
- Removed legacy simulation mode parameter
- Updated Makefile with monitor-hub target

### Version 1.0 (Legacy)
- Member-only monitoring using per-workspace API calls
- Supported only ~139 workspaces
- Used `/v1/workspaces` endpoint exclusively
