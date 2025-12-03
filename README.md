# Microsoft Fabric Monitoring & Governance System

A comprehensive Python-based solution for monitoring, analyzing, and governing Microsoft Fabric workspaces. This project provides tools for historical activity analysis (Monitor Hub) and automated security group enforcement.

## 🚀 Key Features

### 1. Monitor Hub Analysis (`monitor_hub_pipeline.py`)
- **Historical Data Extraction**: Retrieves up to 28 days of activity data (API limit compliant).
- **Detailed Job History**: Automatically fetches granular job execution details (JSON) to ensure accurate failure tracking.
- **Pagination Support**: Handles high-volume data (>5,000 events/day) using `continuationUri` to ensure zero data loss.
- **Comprehensive Reporting**: Generates CSV reports on:
  - Activity volume and success rates (enriched with job status).
  - Long-running operations.
  - User and item-level usage patterns.
  - Failure analysis.
  - **Parquet Integration**: Automatically persists merged activity data to Parquet format for seamless integration with Delta Tables and downstream analytics.
  - **Enhanced Accuracy**: Robust handling of activity duration and status normalization.
  - **Configurable Inference**: Domain and location mapping logic is now configurable via JSON.
- **Scope Control**: Analyze all tenant workspaces or filter to specific subsets.

### 2. Workspace Access Enforcement (`enforce_workspace_access.py`)
- **Security Compliance**: Ensures required security groups are assigned to workspaces.
- **Dual Modes**:
  - `assess`: Dry-run mode to audit current permissions and identify gaps.
  - `enforce`: Applies changes to add missing principals (requires confirmation).
- **Flexible Targeting**: Configure target groups via JSON files.
- **Suppression Support**: Whitelist specific workspaces to skip enforcement.

### 3. Lineage Extraction (`extract_lineage.py`)
- **Mirrored Database Analysis**: Scans workspaces for Mirrored Databases.
- **Source Tracing**: Extracts source connection details (e.g., Snowflake, Azure SQL) and database names.
- **Inventory Reporting**: Generates a CSV inventory of all mirrored assets and their origins.

## 📋 Prerequisites

- **Python 3.11+**
- **Conda** (for environment management)
- **Microsoft Fabric Permissions**:
  - Service Principal or User credentials with Fabric Admin rights.
  - Access to the Power BI Activity Events API.

## 🛠️ Installation & Setup

This project uses a `Makefile` to simplify environment management.

1.  **Clone the repository**:
    ```bash
    git clone <repository-url>
    cd usf_fabric_monitoring
    ```

2.  **Create the environment**:
    ```bash
    make create
    ```

3.  **Configure Environment Variables**:
    Copy the template and edit your settings.
    ```bash
    cp .env.template .env
    ```
    *Ensure you set `FABRIC_TOKEN`, `TENANT_ID`, and other auth details.*

## ⚙️ Configuration

The system is configured via the `.env` file and JSON configuration files in the `config/` directory.

### Environment Variables (`.env`)

| Variable | Default | Description |
|----------|---------|-------------|
| `MAX_HISTORICAL_DAYS` | `28` | Maximum days to extract (API limit). |
| `DEFAULT_ANALYSIS_DAYS` | `7` | Default window for analysis. |
| `EXPORT_DIRECTORY` | `exports/monitor_hub_analysis` | Output path for reports. |
| `API_RATE_LIMIT_REQUESTS_PER_HOUR` | `180` | Rate limiting safety buffer. |

### Business Logic Configuration (`config/`)

- **`inference_rules.json`**: Maps keywords to business domains (e.g., "HR", "Finance") and locations (e.g., "EMEA", "Americas"). Update this file to customize how activities are categorized without changing code.
- **`workspace_access_targets.json`**: Defines the security groups and roles that must be present on workspaces.
- **`workspace_access_suppressions.json`**: Lists workspaces to exclude from enforcement.

## 📖 Usage Guide

### Running Monitor Hub Analysis

Analyze the last 7 days of activity across the tenant:
```bash
make monitor-hub
```

Analyze a specific window (e.g., 14 days) and output to a custom directory:
```bash
make monitor-hub DAYS=14 OUTPUT_DIR=exports/custom_report
```

### Enforcing Workspace Access

**Audit Mode (Dry Run)**:
Check which workspaces are missing required groups without making changes.
```bash
make enforce-access MODE=assess CSV_SUMMARY=1
```

**Enforcement Mode**:
Apply changes to fix permission gaps.
```bash
make enforce-access MODE=enforce CONFIRM=1
```

### Lineage Extraction

Extract lineage details for Mirrored Databases:
```bash
make extract-lineage
```
*Output: `exports/lineage/mirrored_lineage_YYYYMMDD_HHMMSS.csv`*

### Interactive Analysis (Notebooks)

For advanced analysis and visualization, use the consolidated Jupyter Notebook:

1.  **Launch Jupyter**:
    ```bash
    jupyter notebook
    ```
2.  **Open**: `notebooks/Monitor_Hub_Analysis.ipynb`
3.  **Run**: This notebook handles the end-to-end workflow:
    -   Installs dependencies.
    -   Runs the extraction pipeline (`monitor_hub_pipeline.py`).
    -   Uses PySpark to analyze the detailed JSON logs.
    -   Visualizes failures, user activity, and error trends.

### Manual Report Generation

Regenerate reports from previously extracted data (useful if extraction was interrupted):
```bash
make generate-reports
```

### Development Commands

- **Update Environment**: `make update` (after changing `environment.yml`)
- **Run Tests**: `make test`
- **Lint Code**: `make lint`
- **Format Code**: `make format`
- **Clean Cache**: `make clean`

## 📂 Project Structure

```text
usf_fabric_monitoring/
├── config/                 # Configuration files (targets, suppressions)
├── docs/                   # Documentation
├── exports/                # Generated reports and data
├── notebooks/              # Jupyter Notebooks for interactive analysis
├── src/                    # Source code
│   └── usf_fabric_monitoring/
│       ├── core/           # Core logic (extractors, enforcers, pipeline)
│       └── scripts/        # Entry point scripts (monitor_hub_pipeline.py, etc.)
├── Makefile                # Command automation
├── environment.yml         # Conda environment definition
└── requirements.txt        # Pip dependencies
```

## ⚠️ Recent Updates (API Compliance)

The system has been refactored to strictly adhere to Microsoft Fabric API limitations:
- **28-Day Limit**: Historical extraction is capped at 28 days.
- **Pagination**: Robust handling for days with >5,000 events.
- **Rate Limiting**: Built-in throttling to prevent 429 errors.

See `REFACTORING_SUMMARY.md` for more details.

## ☁️ Deployment to Microsoft Fabric

To run this solution directly within a Microsoft Fabric Notebook:

1.  **Build the Package**:
    Run the following command locally to create a Python Wheel (`.whl`) file:
    ```bash
    python -m build
    ```
    This will generate a file like `dist/usf_fabric_monitoring-0.1.11-py3-none-any.whl`.

2.  **Upload to Fabric**:
    -   Navigate to your Fabric Workspace.
    -   Create a **Fabric Environment** (or use an existing one).
    -   In the "Public Libraries" or "Custom Libraries" section, upload the `.whl` file you just built.
    -   Save and Publish the Environment.

3.  **Configure the Notebook**:
    -   Open `notebooks/Monitor_Hub_Analysis.ipynb` in Fabric (or import it).
    -   In the notebook settings (Environment), select the Environment you created in step 2.
    -   This ensures the `usf_fabric_monitoring` library is installed and available to the notebook.

4.  **Run**:
    -   Execute the notebook cells. The library will be automatically detected.
