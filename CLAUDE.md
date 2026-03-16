# usf_fabric_monitoring Context & Rules

This is the central nervous system for the `usf_fabric_monitoring` project. Claude Code must read and adhere to these rules whenever operating in this directory.

## Project Architecture & Layout
- **Core Technology:** Python 3.11+, PySpark, FastAPI, D3.js.
- **Purpose:** A comprehensive solution for monitoring, analyzing, and governing Microsoft Fabric workspaces (historical activity, workspace access enforcement, lineage extraction, and star schema building).
- **Structure (Strict `src` Layout):**
  - `/src/usf_fabric_monitoring/core/`: All reusable business logic lives here (auth, pipeline, extractors, schema builders).
  - `/src/usf_fabric_monitoring/scripts/`: CLI entry points (`usf-*` commands). Do **not** use the legacy root `/scripts/` folder for new CLI tools.
  - `/lineage_explorer/`: A standalone FastAPI + D3.js web application. It consumes JSON data from `/exports/lineage/`. It is completely separate from the core Python library.
  - `/notebooks/`: Interactive Jupyter notebooks for analysis (e.g., PySpark processing).
  - `/config/`: JSON configuration files governing business rules (inference mapping).

## Workflow Rules
1. **Never Break Working Code:** This project utilizes complex "Smart Merge" technologies and precise API handling (pagination, rate limiting). Do not carelessly edit `/core/` files without understanding the downstream pipelines.
2. **Environment & Testing:** Always use the conda environment (`make create`). Tests are in `/tests/` and mirror the `/src/` structure. Run `make test` and `make lint` before declaring a feature complete.
3. **Data Handling:** Output data must always be written to `/exports/` and never committed to Git. The project heavily utilizes Parquet integration and Delta Lake DDLs.

## Active Plugin & MCP Usage
You have access to advanced AI skills and Model Context Protocol (MCP) servers in this workspace:
- **`code-review` & `security-guidance`:** Use these plugins for code quality and securing API credential handling.
- **Power BI MCP:** Use the Power BI MCP server if tasked with debugging semantic models or DAX issues related to the monitoring outputs.
- **Get Shit Done (GSD):** Always use the `/gsd:new-project` orchestration framework when implementing complex new features (like a new extraction module or lineage graph update). DO NOT write large chunks of code in a single prompt.

## Deployment Context
Code here is frequently packaged as a `.whl` (Wheel) file via `make build` and deployed directly to Microsoft Fabric Environments and Notebooks. Ensure all dependencies remain compatible with the Fabric generic runtime.
