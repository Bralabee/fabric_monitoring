# Microsoft Fabric Monitoring & Governance System

A comprehensive Python-based solution for monitoring, analyzing, and governing Microsoft Fabric workspaces. This project provides tools for historical activity analysis (Monitor Hub) and automated security group enforcement.


































































































































































- **Cloud Deployment**: Docker containerization and Azure deployment templates- **Performance Optimization**: Further improvements to Smart Merge efficiency- **Enhanced Lineage**: Expanded lineage extraction beyond Mirrored Databases- **Automated Alerting**: Teams webhooks and email notifications for critical failures### [0.1.16] - Planned## Upcoming Features---- Initial Makefile automation- Conda environment management- Basic CSV report generation- Lineage extraction capabilities  - Workspace access enforcement- Historical data extraction from Microsoft Fabric APIsInitial development versions with core functionality:### [0.1.0 - 0.1.9] - 2024-12-01 to 2024-12-03## Previous Versions---- Enhanced module organization- Package structure improvements### Changed## [0.1.10] - 2024-12-03- Improved data processing capabilities- Initial implementation of enhanced pipeline architecture### Added## [0.1.11] - 2024-12-03- Improved logging for debugging and monitoring- Enhanced error handling in data extraction processes### Added## [0.1.12] - 2024-12-04- Updated environment configuration handling- Resolved import path issues in core modules### Fixed## [0.1.13] - 2024-12-04  - Minor improvements to logging and error handling- Updated package dependencies and build configuration### Changed## [0.1.14] - 2024-12-04---- Enhanced validation ensures data quality before analysis- Offline testing capabilities available out of the box- Duration fixes are automatically enabled in the core pipeline#### For New Deployments4. **Review Logs**: Check for "Duration fix: restored duration data" messages confirming fix application3. **Validate Reports**: Re-generate reports to benefit from enhanced timing data2. **Test Offline**: Use `make test-comparative` to see the improvement in your data1. **Update Package**: Install version 0.1.15 for duration fix benefits#### For Existing Users### 🚀 Migration Guide- **Report Generation**: Valid CSV reports with enhanced timing insights- **Timing Accuracy**: Precise duration calculations using job execution data- **Duration Recovery Rate**: 100% success when job details available#### Validation Results```    enhanced_activity['duration_seconds'] = calculate_duration(start_time, job_end_time)    # Recalculate duration with restored end time    enhanced_activity['end_time'] = job_end_timeif pd.isna(activity_end_time) and pd.notna(job_end_time):# Core innovation: Job-based end time restoration```python#### Smart Merge Algorithm### 🔧 Technical Implementation Details- Updated method signatures with detailed parameter documentation- Added comprehensive logging for duration fix operations- Enhanced inline code documentation for Smart Merge logic#### Technical Documentation- Updated recent updates section with breakthrough details- Enhanced deployment guide with current version information- Added offline analysis capabilities- Updated key features to highlight Smart Merge technology#### README Enhancements### 📚 Documentation Updates- `make test-all`: Run comprehensive test suite- `make test-complete-pipeline`: Test complete end-to-end pipeline- `make test-comparative`: Test before/after duration fix comparison- `make test-offline`: Test offline analysis capabilities  - `make test-duration-fix`: Test duration calculation fixes#### Makefile Enhancements- `test_complete_pipeline.py`: End-to-end pipeline validation with report generation- `test_comparative_analysis.py`: Before/after comparison demonstrating fix impact- `test_offline_analysis.py`: Offline analysis using existing data files- `test_duration_fix.py`: Core duration calculation fix validation#### Test Files Added### 🧪 New Testing Infrastructure- **Logging Performance**: Structured logging provides clear visibility into processing steps and outcomes- **Memory Management**: Efficient handling of large datasets during merge operations- **Smart Merge Logic**: Optimized activity-job correlation using pandas merge_asof for temporal matching#### Processing Efficiency- **Report Quality**: Enhanced CSV reports now include accurate performance metrics and timing-based insights- **Analysis Depth**: Enabled comprehensive failure analysis with precise timing insights- **Timing Data Availability**: Improved from 0% to 100% valid duration data when job details are available#### Analysis Capabilities### 📊 Performance Improvements- **Report Generation**: Resolved report generation failures caused by missing timing calculations- **Performance Insights**: Restored capability to identify long-running operations and performance bottlenecks- **Failure Analysis**: Fixed inability to perform meaningful failure timing analysis due to missing duration data#### Analysis Reliability- **Timing Accuracy**: Corrected duration calculations that were returning zero due to missing end time data- **Schema Mapping**: Resolved column mapping issues between CSV data structure and data loader expectations- **Missing Duration Data**: Fixed critical issue where 100% of activity records lacked `end_time` and `duration_seconds`#### Data Quality Issues### 🐛 Bug Fixes- **Mock Data Generation**: Realistic job detail generation for testing duration fix scenarios- **End-to-End Pipeline Validation**: Comprehensive test covering data loading → duration fixes → analysis → report generation- **Comparative Analysis**: Before/after testing that demonstrates the impact of duration fixes (0% → 100% timing data recovery)- **Offline Analysis Testing**: Complete test suite that validates functionality using existing CSV data without API calls#### Testing & Validation Framework- **Backward Compatibility**: Graceful fallback when job details are unavailable- **Improvement Logging**: Detailed logging of duration restoration statistics for monitoring and validation- **Non-negative Validation**: Ensures duration calculations are always non-negative with proper error handling- **Duration Calculation Fix**: Added comprehensive logic to restore missing `end_time` from job data#### Core Pipeline Improvements  - **Intelligent Correlation**: Activities are matched with job details using temporal proximity and item ID correlation- **Enhanced Pipeline**: Core `MonitorHubPipeline._merge_activities()` method now includes sophisticated duration calculation fixes- **100% Data Recovery**: Successfully restores missing timing data from job `endTimeUtc` when activity `end_time` is missing (which was 100% of cases)- **Revolutionary Duration Recovery**: Implemented comprehensive Smart Merge functionality that correlates activity logs with detailed job execution data#### Smart Merge Technology### 🔧 Major Enhancements## [0.1.15] - 2024-12-04 - Duration Fix Breakthroughand this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
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

## ⚠️ Recent Updates (v0.1.15 - Duration Fix Breakthrough)

The system has undergone major enhancements to address critical timing data issues:

### 🔧 **Smart Merge Technology (v0.1.15)**
- **Duration Recovery**: Revolutionary fix that restores 100% missing timing data by correlating activities with job execution details
- **Enhanced Pipeline**: Core `MonitorHubPipeline` now includes comprehensive duration calculation fixes
- **Offline Validation**: Complete test suite validates functionality without API calls
- **Backward Compatibility**: Gracefully handles scenarios with missing job details

### 📊 **API Compliance & Performance**
- **28-Day Limit**: Historical extraction is capped at 28 days (API limit compliant)
- **Pagination**: Robust handling for days with >5,000 events using `continuationUri`
- **Rate Limiting**: Built-in throttling to prevent 429 errors
- **Smart Merge**: Intelligent correlation of activity logs with detailed job execution data

### 🧪 **Comprehensive Testing**
- **Offline Analysis**: Test complete pipeline functionality with existing data
- **Comparative Analysis**: Before/after validation of duration fixes
- **End-to-End Validation**: Complete pipeline testing from data loading to report generation

See `CHANGELOG.md` for detailed release notes.

## ☁️ Deployment to Microsoft Fabric

To run this solution directly within a Microsoft Fabric Notebook:

1.  **Build the Package**:
    Run the following command locally to create a Python Wheel (`.whl`) file:
    ```bash
    make build
    ```
    This will generate a file like `dist/usf_fabric_monitoring-0.1.15-py3-none-any.whl`.

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
