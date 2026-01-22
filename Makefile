# Fabric Monitoring System Makefile
# CRUD operations for conda environment management

# Environment variables
ENV_NAME = fabric-monitoring
ENV_FILE = environment.yml
PYTHON_VERSION = 3.11

# Colors for output
GREEN = \033[0;32m
YELLOW = \033[1;33m
RED = \033[0;31m
NC = \033[0m # No Color

.PHONY: help create update delete status install validate-config test test-smoke clean lint format check-env enforce-access monitor-hub extract-lineage compute-analysis generate-reports audit-sp-access extract-details build export activate dev-setup dev-check test-duration-fix test-offline test-comparative test-complete-pipeline test-all star-schema star-schema-ddl star-schema-describe

# Default target
help:
	@echo "$(GREEN)Fabric Monitoring System - Environment Management$(NC)"
	@echo ""
	@echo "$(YELLOW)Available commands:$(NC)"
	@echo "  $(GREEN)create$(NC)      - Create conda environment from environment.yml"
	@echo "  $(GREEN)update$(NC)      - Update existing environment with new dependencies"
	@echo "  $(GREEN)delete$(NC)      - Delete the conda environment"
	@echo "  $(GREEN)status$(NC)      - Show environment status and info"
	@echo "  $(GREEN)install$(NC)     - Install this package (editable) into the env"
	@echo "  $(GREEN)validate-config$(NC) - Validate config JSON files (schemas)"
	@echo "  $(GREEN)activate$(NC)    - Show activation command"
	@echo "  $(GREEN)test$(NC)        - Run tests in the environment"
	@echo "  $(GREEN)test-all$(NC)    - Run comprehensive test suite including duration fixes"
	@echo "  $(GREEN)test-duration-fix$(NC) - Test duration calculation fixes"
	@echo "  $(GREEN)test-offline$(NC) - Test offline analysis with existing data"
	@echo "  $(GREEN)lint$(NC)        - Run linting checks"
	@echo "  $(GREEN)format$(NC)      - Format code with black"
	@echo "  $(GREEN)clean$(NC)       - Clean cache and temporary files"
	@echo "  $(GREEN)export$(NC)      - Export current environment to new yml file"
	@echo "  $(GREEN)enforce-access$(NC) - Ensure required groups keep workspace access"
	@echo "  $(GREEN)monitor-hub$(NC) - Run Monitor Hub activity analysis"
	@echo "  $(GREEN)extract-lineage$(NC) - Extract Lineage (Mirrored Databases & Shortcuts)"
	@echo "  $(GREEN)compute-analysis$(NC) - Run Compute Analysis (alias for monitor-hub)"
	@echo "  $(GREEN)generate-reports$(NC) - Generate reports from existing extracted data"
	@echo "  $(GREEN)audit-sp-access$(NC)  - Audit workspaces where Service Principal is missing"
	@echo "  $(GREEN)star-schema$(NC) - Build star schema for analytics from Monitor Hub data"
	@echo "  $(GREEN)star-schema-ddl$(NC) - Print DDL statements for star schema tables"
	@echo "  $(GREEN)star-schema-describe$(NC) - Print schema description and sample queries"
	@echo ""
	@echo "$(YELLOW)Usage examples:$(NC)"
	@echo "  make create       # Create new environment"
	@echo "  make update       # Update existing environment"
	@echo "  make delete       # Remove environment"
	@echo "  make status       # Check environment status"
	@echo ""
	@echo "$(YELLOW)Enforcement examples:$(NC)"
	@echo "  make enforce-access MODE=assess CSV_SUMMARY=1"
	@echo "  make enforce-access MODE=enforce CONFIRM=1 API_PREFERENCE=powerbi"
	@echo "  make enforce-access MODE=assess MAX_WORKSPACES=10"
	@echo ""
	@echo "$(YELLOW)Monitoring examples:$(NC)"
	@echo "  make monitor-hub DAYS=7"
	@echo "  make monitor-hub DAYS=30 MEMBER_ONLY=1"
	@echo "  make monitor-hub DAYS=14 OUTPUT_DIR=exports/custom"
	@echo ""
	@echo "$(YELLOW)Star Schema examples:$(NC)"
	@echo "  make star-schema                           # Incremental build"
	@echo "  make star-schema FULL_REFRESH=1            # Full refresh"
	@echo "  make star-schema OUTPUT_DIR=exports/custom # Custom output"
	@echo "  make star-schema-ddl                       # Print DDL statements"

# CREATE: Create new conda environment
create:
	@echo "$(GREEN)Creating conda environment: $(ENV_NAME)$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		echo "$(YELLOW)Environment $(ENV_NAME) already exists. Use 'make update' to update it.$(NC)"; \
	else \
		conda env create -f $(ENV_FILE) && \
		echo "$(GREEN)✅ Environment $(ENV_NAME) created successfully!$(NC)" && \
		echo "$(YELLOW)Activate with: conda activate $(ENV_NAME)$(NC)"; \
	fi

# READ: Show environment status and information
status:
	@echo "$(GREEN)Environment Status: $(ENV_NAME)$(NC)"
	@echo "----------------------------------------"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		echo "$(GREEN)✅ Environment exists$(NC)"; \
		echo ""; \
		echo "$(YELLOW)Environment details:$(NC)"; \
		conda env list | grep "^$(ENV_NAME) "; \
		echo ""; \
		echo "$(YELLOW)Python version:$(NC)"; \
		conda run -n $(ENV_NAME) python --version; \
		echo ""; \
		echo "$(YELLOW)Installed packages:$(NC)"; \
		conda run -n $(ENV_NAME) pip list | head -10; \
		echo "... (showing first 10 packages)"; \
		echo ""; \
		echo "$(YELLOW)To see all packages:$(NC) conda run -n $(ENV_NAME) conda list"; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it with: make create$(NC)"; \
	fi

# UPDATE: Update existing environment
update:
	@echo "$(GREEN)Updating conda environment: $(ENV_NAME)$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda env update -f $(ENV_FILE) --prune && \
		echo "$(GREEN)✅ Environment $(ENV_NAME) updated successfully!$(NC)"; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

# DELETE: Remove conda environment
delete:
	@echo "$(RED)Deleting conda environment: $(ENV_NAME)$(NC)"
	@echo "$(YELLOW)⚠️  This will permanently delete the environment!$(NC)"
	@read -p "Are you sure? (y/N): " confirm && \
	if [ "$$confirm" = "y" ] || [ "$$confirm" = "Y" ]; then \
		conda env remove -n $(ENV_NAME) && \
		echo "$(GREEN)✅ Environment $(ENV_NAME) deleted successfully!$(NC)"; \
	else \
		echo "$(YELLOW)Deletion cancelled.$(NC)"; \
	fi

# INSTALL: Install additional pip dependencies
install:
	@echo "$(GREEN)Installing package (editable) into $(ENV_NAME)$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run -n $(ENV_NAME) pip install -e . && \
		echo "$(GREEN)✅ Package installed (editable) into $(ENV_NAME)!$(NC)"; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

validate-config:
	@echo "$(GREEN)Validating config JSON files$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) python -m usf_fabric_monitoring.scripts.validate_config; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

# Show activation command
activate:
	@echo "$(GREEN)To activate the environment, run:$(NC)"
	@echo "$(YELLOW)conda activate $(ENV_NAME)$(NC)"

# EXPORT: Export current environment to new file
export:
	@echo "$(GREEN)Exporting environment to environment_exported.yml$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda env export -n $(ENV_NAME) > environment_exported.yml && \
		echo "$(GREEN)✅ Environment exported to environment_exported.yml$(NC)"; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
	fi

# BUILD: Build python package (wheel)
build:
	@echo "$(GREEN)Building Python package$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run -n $(ENV_NAME) pip install build; \
		conda run -n $(ENV_NAME) python -m build; \
		echo "$(GREEN)✅ Package built successfully! Check dist/ folder.$(NC)"; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
	fi

# Development commands
test:
	@echo "$(GREEN)Running tests$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) python -m pytest tests/ -v --cov=src/; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
	fi

test-smoke:
	@echo "$(GREEN)Running smoke tests (safe subset)$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) python -m pytest -q \
			tests/test_basic_imports.py \
			tests/test_enrichment_fix.py \
			tests/test_url_encoding.py \
			tests/test_inference_config.py; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
	fi

lint:
	@echo "$(GREEN)Running linting checks$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) flake8 src/ && \
		conda run --no-capture-output -n $(ENV_NAME) mypy src/; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
	fi

format:
	@echo "$(GREEN)Formatting code with black$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) black src/ tests/; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
	fi

clean:
	@echo "$(GREEN)Cleaning cache and temporary files$(NC)"
	@find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	@find . -type f -name "*.pyc" -delete 2>/dev/null || true
	@find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@find . -type d -name "*.egg-info" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".pytest_cache" -exec rm -rf {} + 2>/dev/null || true
	@find . -type d -name ".mypy_cache" -exec rm -rf {} + 2>/dev/null || true
	@echo "$(GREEN)✅ Cache and temporary files cleaned$(NC)"

clean-logs:
	@echo "$(GREEN)Cleaning log files$(NC)"
	@find . -type f -name "*.log*" -delete 2>/dev/null || true
	@echo "$(GREEN)✅ Log files cleaned$(NC)"

# Check if environment exists (utility function)
check-env:
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		echo "$(GREEN)✅ Environment $(ENV_NAME) exists$(NC)"; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		exit 1; \
	fi

# Development workflow shortcuts
dev-setup: create install
	@echo "$(GREEN)✅ Development environment setup complete!$(NC)"
	@echo "$(YELLOW)Next steps:$(NC)"
	@echo "  1. conda activate $(ENV_NAME)"
	@echo "  2. Start developing!"

dev-check: lint test
	@echo "$(GREEN)✅ Development checks passed!$(NC)"

enforce-access:
	@echo "$(GREEN)Running workspace access enforcement$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		MODE_ARG=$${MODE:+--mode $$MODE}; \
		CONFIRM_ARG=$${CONFIRM:+--confirm}; \
		DRY_RUN_ARG=$${DRY_RUN:+--dry-run}; \
		CSV_ARG=$${CSV_SUMMARY:+--csv-summary}; \
		API_ARG=$${API_PREFERENCE:+--api-preference $$API_PREFERENCE}; \
		MAX_WS_ARG=$${MAX_WORKSPACES:+--max-workspaces $$MAX_WORKSPACES}; \
		INCLUDE_PERSONAL_ARG=$${INCLUDE_PERSONAL:+--include-personal-workspaces}; \
		conda run --no-capture-output -n $(ENV_NAME) python src/usf_fabric_monitoring/scripts/enforce_workspace_access.py $$MODE_ARG $$CONFIRM_ARG $$DRY_RUN_ARG $$CSV_ARG $$API_ARG $$MAX_WS_ARG $$INCLUDE_PERSONAL_ARG; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

# Monitor Hub Analysis
monitor-hub:
	@echo "$(GREEN)Running Monitor Hub Analysis$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		DAYS_ARG=$${DAYS:+--days $$DAYS}; \
		MEMBER_ONLY_ARG=$${MEMBER_ONLY:+--member-only}; \
		OUTPUT_ARG=$${OUTPUT_DIR:+--output-dir $$OUTPUT_DIR}; \
		conda run --no-capture-output -n $(ENV_NAME) python src/usf_fabric_monitoring/scripts/monitor_hub_pipeline.py $$DAYS_ARG $$MEMBER_ONLY_ARG $$OUTPUT_ARG; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

# Quick commands
.PHONY: c u d s
c: create
u: update  
d: delete
s: status

# Lineage Extraction
extract-lineage:
	@echo "$(GREEN)Running Lineage Extraction (Mirrored DBs & Shortcuts)$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		OUTPUT_ARG=$${OUTPUT_DIR:+--output-dir $$OUTPUT_DIR}; \
		conda run --no-capture-output -n $(ENV_NAME) python src/usf_fabric_monitoring/scripts/extract_lineage.py $$OUTPUT_ARG; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

# Compute Analysis (Alias for monitor-hub)
compute-analysis: monitor-hub

# Manual Report Generation
generate-reports:
	@echo "$(GREEN)Generating reports from existing data$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) python src/usf_fabric_monitoring/scripts/generate_reports_manual.py; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

# Audit Service Principal Access
audit-sp-access:
	@echo "$(GREEN)Auditing Service Principal access to workspaces$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) python src/usf_fabric_monitoring/scripts/audit_sp_access.py; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

# Extract Fabric Item Details
extract-details:
	@echo "$(GREEN)Extracting Fabric Item Details$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		WS_ARG=$${WORKSPACE:+--workspace $$WORKSPACE}; \
		conda run --no-capture-output -n $(ENV_NAME) python src/usf_fabric_monitoring/scripts/extract_fabric_item_details.py $$WS_ARG; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

# Comprehensive Test Suite
test-duration-fix:
	@echo "$(GREEN)Testing Duration Calculation Fixes$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) python tools/dev_tests/test_duration_fix.py; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

test-offline:
	@echo "$(GREEN)Testing Offline Analysis$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) python tools/dev_tests/test_offline_analysis.py; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

test-comparative:
	@echo "$(GREEN)Testing Comparative Analysis (Before/After Duration Fixes)$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) python tools/dev_tests/test_comparative_analysis.py; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

test-complete-pipeline:
	@echo "$(GREEN)Testing Complete End-to-End Pipeline$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) python tools/dev_tests/test_complete_pipeline.py; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

test-all: test test-duration-fix test-comparative test-complete-pipeline
	@echo "$(GREEN)Comprehensive test suite completed$(NC)"

# Star Schema Builder
star-schema:
	@echo "$(GREEN)Building Star Schema for Analytics$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		INPUT_ARG=$${INPUT_DIR:+--input-dir $$INPUT_DIR}; \
		OUTPUT_ARG=$${OUTPUT_DIR:+--output-dir $$OUTPUT_DIR}; \
		FULL_REFRESH_ARG=$${FULL_REFRESH:+--full-refresh}; \
		conda run --no-capture-output -n $(ENV_NAME) python src/usf_fabric_monitoring/scripts/build_star_schema.py $$INPUT_ARG $$OUTPUT_ARG $$FULL_REFRESH_ARG; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

star-schema-ddl:
	@echo "$(GREEN)Generating Star Schema DDL$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) python src/usf_fabric_monitoring/scripts/build_star_schema.py --ddl-only; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi

star-schema-describe:
	@echo "$(GREEN)Describing Star Schema$(NC)"
	@if conda env list | grep -q "^$(ENV_NAME) "; then \
		conda run --no-capture-output -n $(ENV_NAME) python src/usf_fabric_monitoring/scripts/build_star_schema.py --describe; \
	else \
		echo "$(RED)❌ Environment $(ENV_NAME) does not exist$(NC)"; \
		echo "$(YELLOW)Create it first with: make create$(NC)"; \
	fi