# Microsoft Fabric Monitoring & Governance System

A comprehensive Python-based solution for monitoring, analyzing, and governing Microsoft Fabric workspaces. This project provides tools for historical activity analysis (Monitor Hub), automated security group enforcement, and star schema analytics for business intelligence.

> **Current Version: 0.3.21** - Multi-Node Selection & Directional Arrow Coloring  
> See [CHANGELOG.md](CHANGELOG.md) for release notes | [CONTRIBUTING.md](CONTRIBUTING.md) for contribution guidelines | [SECURITY.md](SECURITY.md) for security policies
## 🚀 Key Features

### 1. Monitor Hub Analysis (`monitor_hub_pipeline.py`)
- **Historical Data Extraction**: Retrieves up to 28 days of activity data (API limit compliant).
- **Enhanced Duration Calculation**: Revolutionary Smart Merge technology that restores missing timing data by correlating activity logs with detailed job execution data, achieving 100% duration data recovery.
- **Detailed Job History**: Automatically fetches granular job execution details (JSON) to ensure accurate failure tracking and precise timing analysis.
- **Pagination Support**: Handles high-volume data (>5,000 events/day) using `continuationUri` to ensure zero data loss.
- **Comprehensive Reporting**: Generates CSV reports with enhanced accuracy:
  - Activity volume and success rates (enriched with job status)
  - Long-running operations with precise timing
  - User and item-level usage patterns
  - Advanced failure analysis with duration insights
  - **Parquet Integration**: Automatically persists merged activity data to Parquet format for seamless integration with Delta Tables and downstream analytics.
  - **Enhanced Accuracy**: Robust handling of activity duration and status normalization with Smart Merge technology.
  - **Configurable Inference**: Domain and location mapping logic is now configurable via JSON.
- **Offline Analysis**: Works with existing CSV data files without requiring API calls, perfect for testing and validation.
- **Scope Control**: Analyze all tenant workspaces or filter to specific subsets.

### 2. Workspace Access Enforcement (`enforce_workspace_access.py`)
- **Security Compliance**: Ensures required security groups are assigned to workspaces.
- **Dual Modes**:
  - `assess`: Dry-run mode to audit current permissions and identify gaps.
  - `enforce`: Applies changes to add missing principals (requires confirmation).
- **Flexible Targeting**: Configure target groups via JSON files.
- **Suppression Support**: Whitelist specific workspaces to skip enforcement.

### 3. Lineage Extraction (`extract_lineage.py`)
- **Mirrored Database Analysis**: Scans workspaces for Mirrored Databases (Snowflake, Azure SQL, Cosmos DB).
- **OneLake Shortcut Analysis**: Extracts OneLake shortcuts from **Lakehouses** and **KQL Databases**.
- **Unified Inventory**: Outputs a consolidated CSV schema (`Workspace`, `Item`, `Type`, `Source Connection`) for all external dependencies.
- **Hybrid Extraction Modes** ⭐ NEW:
  - `auto` (default): Automatically selects mode based on workspace count
  - `iterative`: O(N) individual API calls per workspace (granular, slower)
  - `scanner`: O(1) batch Admin Scanner API calls (faster for large tenants)
  - Configurable threshold (default: 50 workspaces) for auto mode switching
  - Automatic fallback to iterative on scanner errors
- **Lineage Explorer**: Interactive D3.js force-directed graph visualization with Neo4j-powered analysis:
  - **Unified Light Theme**: Professional light color scheme optimized for readability
  - **Query Explorer**: 40+ pre-built Neo4j queries across 10 categories (Overview, Dependencies, Security, etc.)
  - **Multi-Node Selection**: Ctrl+Click to select multiple nodes for batch analysis
    - Children button: Select all direct children (1 level downstream)
    - Descendants button: Select all descendants (up to 10 levels downstream)
    - Selection count badge and Clear button in toolbar
  - **Directional Arrow Coloring**: Color-coded data flow visualization
    - Green arrows: Inward connections (incoming data)
    - Blue arrows: Outward connections (outgoing data)
  - **Graph-Native Model**: Direct CSV → JSON graph conversion for clean, efficient data flow.
  - **Smart Filtering**: Filter by workspace, item type, source type with instant updates.
  - **Node Details**: Click any node to see connections, metadata, and related items.
  - **Zoom & Pan**: Navigate large graphs with intuitive controls.
  - **Neo4j Graph Analysis** ⭐ NEW:
    - **Right-Click Context Menu**: Access impact analysis, upstream trace, path finding from any node
    - **Path Finder**: Find and visualize shortest path between any two items
    - **Impact Analysis**: Show all downstream items affected by changes to a node
    - **Upstream Trace**: Display all data sources feeding into an item
    - **Smart Filtering**: Filter by external source type (ADLS, S3, Snowflake)
    - **Cross-Workspace View**: Highlight dependencies spanning workspaces
    - **Centrality Highlighting**: Size nodes by importance/connection count
    - **Statistics Dashboard**: Comprehensive analytics at `/dashboard.html`
- **Command**: `make lineage-explorer` (starts server at http://127.0.0.1:8000)

### 4. Star Schema Analytics (`build_star_schema.py`) ⭐ NEW in v0.3.0
- **Kimball-Style Dimensional Model**: Transforms raw Monitor Hub data into a proper star schema for analytics.
- **7 Dimension Tables**: `dim_date`, `dim_time`, `dim_workspace`, `dim_item`, `dim_user`, `dim_activity_type`, `dim_status`
- **2 Fact Tables**: `fact_activity` (1M+ records), `fact_daily_metrics` (pre-aggregated)
- **Incremental Loading**: High-water mark pattern for efficient delta loads
- **SCD Type 2 Support**: Tracks slowly changing dimensions (workspace names, item descriptions)
- **Delta Lake DDL**: Generates deployment scripts for Fabric Lakehouses
- **CLI & Notebook Options**: Run via `usf-star-schema` command or `notebooks/Fabric_Star_Schema_Builder.ipynb`

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
    Authentication options:
    - Recommended: set `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` (Service Principal)
    - Alternative: rely on `DefaultAzureCredential` (Managed Identity / Azure CLI login / Fabric notebook identity)

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

**Extraction Modes** (for large tenants):
```bash
# Auto mode (default) - selects based on workspace count
python scripts/extract_lineage.py --mode auto

# Scanner mode - batch API, ~18 seconds for 178 workspaces
python scripts/extract_lineage.py --mode scanner

# Iterative mode - granular API calls (legacy)
python scripts/extract_lineage.py --mode iterative

# Custom threshold for auto mode
python scripts/extract_lineage.py --mode auto --threshold 100
```

> **Note**: Scanner mode requires Fabric Administrator role or Service Principal with `Tenant.Read.All` permission.

*Output: `exports/lineage/lineage_YYYYMMDD_HHMMSS.json`*

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
├── config/                 # Configuration files (targets, suppressions, inference rules)
├── docs/                   # Documentation (FABRIC_DEPLOYMENT.md, etc.)
├── exports/                # Generated reports and data
│   └── star_schema/        # Star schema output (dimensions, facts, DDL)
├── notebooks/              # Jupyter Notebooks for interactive analysis
│   ├── Monitor_Hub_Analysis.ipynb
│   ├── Workspace_Access_Enforcement.ipynb
│   └── Fabric_Star_Schema_Builder.ipynb
├── scripts/                # Development utilities and convenience wrappers
├── src/                    # Source code (industry-standard src layout)
│   └── usf_fabric_monitoring/
│       ├── core/           # Core logic (pipeline, star_schema_builder, admin_scanner)
│       └── scripts/        # CLI entry points (installable via pip)
│           ├── monitor_hub_pipeline.py     → usf-monitor-hub
│           ├── extract_lineage.py          → usf-extract-lineage
│           ├── enforce_workspace_access.py → usf-enforce-access
│           ├── build_star_schema.py        → usf-star-schema
│           └── validate_config.py          → usf-validate-config
├── lineage_explorer/       # Interactive D3.js lineage visualization with Neo4j
│   ├── static/             # HTML/CSS/JS for web interface
│   ├── graph_database/     # Neo4j client, data loader, queries
│   └── server.py           # FastAPI backend
├── tests/                  # Test suite
├── Makefile                # Command automation
├── environment.yml         # Conda environment definition
└── pyproject.toml          # Python package metadata + pip dependencies
```

## ⚠️ Recent Updates (v0.3.0 - Star Schema Analytics Release)

This release introduces powerful star schema analytics capabilities:

### 🏗️ **Star Schema Builder**
- **Dimensional Modeling**: Kimball-style star schema with 7 dimensions and 2 fact tables
- **1M+ Records**: Processes complete Monitor Hub activity data into analytical format
- **Incremental Loads**: High-water mark tracking for efficient delta processing
- **SCD Type 2**: Slowly changing dimensions for historical accuracy
- **Delta Lake DDL**: Auto-generated scripts for Fabric deployment

### 🔧 **Smart Merge Technology** (from v0.2.0)
- **Dual API Correlation**: Combines Activity Events API (audit log) with Job History API (job executions) using `merge_asof` by `item_id` + 5-minute timestamp tolerance
- **Failure Detection**: Activity Events report ALL status="Succeeded" (audit entries can't fail), while Job History provides actual failure data for job executions
- **Duration Recovery**: Restores missing timing data by matching activities with detailed job execution records
- **Validated Metrics (Dec 2025)**: 1.28M activities, 6,218 failures (99.52% success), 512 workspaces, 46 activity types
- **Offline Validation**: Complete test suite validates functionality without API calls
- **Backward Compatibility**: Gracefully handles scenarios with missing job details

### 📊 **Advanced Analytics & Visualization**
- **16+ Visualizations**: Comprehensive dashboard with interactive Plotly charts
- **Executive Dashboard**: High-level KPIs and trends for leadership reporting
- **Performance Insights**: Deep analysis of long-running operations and bottlenecks
- **Failure Analysis**: Detailed error tracking with root cause identification

### 📚 **Documentation & Governance**
- **Fabric Deployment Guide**: Complete deployment options ([docs/FABRIC_DEPLOYMENT.md](docs/FABRIC_DEPLOYMENT.md))
- **Comprehensive Project Analysis**: Complete gap assessment and improvement roadmap ([PROJECT_ANALYSIS.md](PROJECT_ANALYSIS.md))
- **Contribution Guidelines**: Standardized development workflow and coding standards ([CONTRIBUTING.md](CONTRIBUTING.md))
- **Security Policy**: Best practices and vulnerability reporting procedures ([SECURITY.md](SECURITY.md))

### 🧪 **Testing & Quality**
- **Test Coverage**: Enhanced test suite with offline analysis capabilities
- **Comparative Analysis**: Before/after validation of duration fixes
- **End-to-End Validation**: Complete pipeline testing from data loading to report generation

See [CHANGELOG.md](CHANGELOG.md) for detailed release notes.

## ☁️ Deployment to Microsoft Fabric

To run this solution directly within a Microsoft Fabric Notebook:

1.  **Build the Package**:
    Run the following command locally to create a Python Wheel (`.whl`) file:
    ```bash
    make build
    ```
    This will generate a file like `dist/usf_fabric_monitoring-0.3.9-py3-none-any.whl`.

2.  **Upload to Fabric**:
    -   Navigate to your Fabric Workspace.
    -   Create a **Fabric Environment** (or use an existing one).
    -   In the "Public Libraries" or "Custom Libraries" section, upload the `.whl` file.
    -   Save and Publish the Environment.

3.  **Configure the Notebook**:
    -   Import one of the available notebooks:
        - `notebooks/Monitor_Hub_Analysis.ipynb` - Historical activity analysis
        - `notebooks/Workspace_Access_Enforcement.ipynb` - Security compliance
        - `notebooks/Fabric_Star_Schema_Builder.ipynb` - Star schema analytics ⭐ NEW
    -   In the notebook settings (Environment), select the Environment you created in step 2.
    -   This ensures the `usf_fabric_monitoring` library is installed and available to the notebook.

4.  **Run**:
    -   Execute the notebook cells. The library will be automatically detected.

For advanced deployment options (inline pip install, lakehouse file reference, etc.), see [docs/FABRIC_DEPLOYMENT.md](docs/FABRIC_DEPLOYMENT.md).

## 🔗 Related Projects

- **[usf_fabric_cli_cicd](../usf_fabric_cli_cicd)**: Lightweight CLI for Fabric deployment automation (v1.3.1)
- **[usf-fabric-cicd](../usf-fabric-cicd)**: Original monolithic framework (predecessor to CLI tool)
- **[stakeholder_presentations](../stakeholder_presentations)**: Executive presentation decks for monitoring/CI/CD solutions
- **[fabric-purview-playbook-webapp](../fabric-purview-playbook-webapp)**: Delivery playbook web application
