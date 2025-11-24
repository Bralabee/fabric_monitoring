# Domain & Location Inference Review

## Findings

### 1. Domain Inference
- **Status**: **Working but Generic**.
- **Observation**: The majority of activities (approx. 75% in sample) are categorized as "**General**".
- **Breakdown**:
  - `General`: ~6380 activities
  - `IT`: ~904 activities
  - `Development`: ~679 activities
  - `Sales`, `Analytics`, `Operations`, `Finance`, `HR`: < 350 each
- **Logic**: The system scans item/workspace names for keywords (e.g., "finance", "budget" -> Finance).
- **Assessment**: This is functioning as designed, but relies heavily on your team using descriptive names. If "General" is too high, we need to add more specific keywords relevant to your business (e.g., project codes, department acronyms).

### 2. Location Inference
- **Status**: ⚠️ **Ineffective (Defaults to Global)**.
- **Observation**: **100%** of the sampled activities show "**Global**" as the location.
- **Logic**:
  1. Checks for `region` or `location` metadata (often empty in standard API responses).
  2. Scans workspace names for "EMEA", "US", "APAC", etc.
  3. Defaults to "Global".
- **Assessment**: The current keyword list is too narrow ("emea", "us", "usa", "america", "apac", "asia"). Unless your workspaces are named like "Sales_US_East", this will always return "Global".

## Recommendations

### 1. Improve Location Logic
To get actual location data, we have two options:
- **Option A (Code Change)**: Expand the keyword list in `src/core/enrichment.py` to include your specific location codes (e.g., "UK", "NY", "London", "Remote").
- **Option B (Metadata)**: If your workspaces are assigned to Capacities in specific regions, we could theoretically look up the Capacity's region, but this requires extra API calls and permissions. **Option A is recommended for now.**

### 2. Refine Domain Keywords
- Review the "General" items. If you see common patterns (e.g., "Proj_Alpha", "Mkt_Campaign"), we should add "Proj" -> "Projects" or "Mkt" -> "Marketing" to the keyword map.

### 3. Action Plan
- **No immediate code change required** unless you can provide a list of your specific naming conventions (Department codes, Location codes).
- If you provide these lists, I can update `src/core/enrichment.py` to drastically improve the categorization accuracy.
