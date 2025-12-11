# GitHub Copilot Instructions for USF Fabric Monitoring

This repository hosts the **USF Fabric Monitoring System**, a Python-based governance and observability solution for Microsoft Fabric.

## 🏗 Architecture & Core Concepts
- **Library-First Design**: The core logic resides in the `usf_fabric_monitoring` Python package (`src/`). Notebooks in `notebooks/` are "thin wrappers" that import this package.
- **Smart Merge Technology**: A critical feature in `historical_analyzer.py` that correlates activity logs with detailed job execution data to recover missing duration metrics (100% data recovery goal).
- **Environment Agnostic**: Code runs both locally (simulating OneLake via `exports/`) and in Fabric Environments. Use `core.utils.resolve_path` for all file I/O.
- **Configuration**: Controlled via `.env` (secrets) and JSON files in `config/` (business logic like `inference_rules.json`).

## 🛠 Critical Workflows
- **Environment Management**: ALWAYS use `make` commands.
  - `make create`: Setup conda environment.
  - `make monitor-hub`: Run the main analysis pipeline.
  - `make enforce-access`: Run workspace security enforcement.
- **Testing**: Run `make test` or `make test-all` (includes duration fix validation).
- **Build & Deploy**:
  1. `python -m build` to generate `.whl`.
  2. Upload `.whl` to Fabric Environment.
  3. Attach Environment to Notebooks.

## 📝 Coding Conventions
- **Path Handling**: NEVER hardcode paths. Use `resolve_path()` to ensure compatibility between local Linux filesystems and Fabric OneLake (`/lakehouse/default/Files`).
- **Logging**: Use `core.logger.setup_logging`. Do not use `print()` for operational logs.
- **Type Hinting**: Required for all `core/` modules.
- **Pandas**: Avoid `SettingWithCopyWarning` by using `.loc` for assignments.
- **Secrets**: Load from `os.getenv()`. Supports both `.env` files and Azure Key Vault integration.

## 🔗 Key Components
- **`MonitorHubPipeline`** (`core/pipeline.py`): The main orchestrator for analysis.
- **`HistoricalAnalysisEngine`** (`core/historical_analyzer.py`): Implements the Smart Merge logic.
- **`WorkspaceAccessEnforcer`** (`core/workspace_access_enforcer.py`): Handles security group compliance.
- **`Monitor_Hub_Analysis.ipynb`**: The production entry point for users.
- **`Monitor_Hub_Analysis_Advanced.ipynb`**: For offline/deep-dive analysis.

## 🚨 Common Pitfalls
- **API Limits**: The system respects the 28-day historical limit of Fabric APIs.
- **Schema Validation**: Ensure `config/*.json` files match expected schemas (currently implicit).
- **Notebook State**: Notebooks rely on the attached Fabric Environment. Ensure the `.whl` is updated in the environment after code changes.
