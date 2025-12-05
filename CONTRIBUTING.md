# Contributing to USF Fabric Monitoring

Thank you for your interest in contributing to the USF Fabric Monitoring project! This document provides guidelines and best practices for contributing.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Workflow](#development-workflow)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Documentation Standards](#documentation-standards)
- [Pull Request Process](#pull-request-process)
- [Release Process](#release-process)
- [Project-Specific Guidelines](#project-specific-guidelines)

---

## Code of Conduct

### Our Pledge

We are committed to providing a welcoming and inclusive environment for all contributors, regardless of experience level, background, or identity.

### Expected Behavior

- Use welcoming and inclusive language
- Be respectful of differing viewpoints and experiences
- Gracefully accept constructive criticism
- Focus on what is best for the project and community
- Show empathy towards other community members

### Unacceptable Behavior

- Trolling, insulting/derogatory comments, and personal or political attacks
- Public or private harassment
- Publishing others' private information without explicit permission
- Other conduct which could reasonably be considered inappropriate

### Enforcement

Violations of the Code of Conduct should be reported to the project maintainers. All complaints will be reviewed and investigated promptly and fairly.

---

## Getting Started

### Prerequisites

- Python 3.11 or higher
- Conda (for environment management)
- Git
- Microsoft Fabric access (for testing)

### Initial Setup

1. **Fork the repository** on GitHub

2. **Clone your fork**:
   ```bash
   git clone https://github.com/YOUR_USERNAME/usf_fabric_monitoring.git
   cd usf_fabric_monitoring
   ```

3. **Add upstream remote**:
   ```bash
   git remote add upstream https://github.com/BralaBee-LEIT/usf_fabric_cicd_codebase.git
   ```

4. **Create the development environment**:
   ```bash
   make create
   ```

5. **Configure environment variables**:
   ```bash
   cp .env.template .env
   # Edit .env with your credentials
   ```

6. **Verify setup**:
   ```bash
   make test
   ```

---

## Development Workflow

We follow the **Git Flow** branching strategy.

### Branch Naming Convention

- **Feature branches**: `feature/<short-description>`
- **Bug fixes**: `fix/<issue-number>-<short-description>`
- **Documentation**: `docs/<short-description>`
- **Hotfixes**: `hotfix/<version>-<short-description>`
- **Releases**: `release/<version>`

Examples:
```bash
feature/smart-merge-optimization
fix/123-pandas-warning
docs/api-documentation
hotfix/0.2.1-critical-auth-bug
release/0.3.0
```

### Workflow Steps

1. **Sync with upstream**:
   ```bash
   git checkout main
   git pull upstream main
   ```

2. **Create a feature branch**:
   ```bash
   git checkout -b feature/your-feature-name
   ```

3. **Make your changes**:
   - Write code following [Coding Standards](#coding-standards)
   - Add tests for new functionality
   - Update documentation as needed

4. **Run tests and linting**:
   ```bash
   make test
   make lint
   make format
   ```

5. **Commit your changes** (see [Commit Message Guidelines](#commit-message-guidelines)):
   ```bash
   git add .
   git commit -m "feat: add smart merge optimization"
   ```

6. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

7. **Create a Pull Request** on GitHub

### Keeping Your Branch Updated

```bash
# While on your feature branch
git fetch upstream
git rebase upstream/main

# If there are conflicts, resolve them and continue
git rebase --continue
```

---

## Coding Standards

### Python Style Guide

We follow **PEP 8** with some project-specific conventions:

- **Line Length**: Maximum 120 characters (not 79)
- **Indentation**: 4 spaces (no tabs)
- **Quotes**: Double quotes for strings (`"example"`)
- **Import Order**: Standard library → Third-party → Local imports

### Type Hints

**All new functions must include type hints**:

```python
# ✅ GOOD
def calculate_duration(start_time: pd.Timestamp, end_time: pd.Timestamp) -> float:
    """Calculate duration in seconds between two timestamps."""
    return (end_time - start_time).total_seconds()

# ❌ BAD
def calculate_duration(start_time, end_time):
    return (end_time - start_time).total_seconds()
```

### Docstring Format

Use **Google-style docstrings**:

```python
def merge_activities(
    activities: pd.DataFrame,
    job_details: pd.DataFrame,
    tolerance_minutes: int = 5
) -> pd.DataFrame:
    """Merge activities with job details using Smart Merge algorithm.
    
    This function correlates activity logs with detailed job execution data
    to restore missing timing information. Uses temporal proximity matching
    with configurable tolerance.
    
    Args:
        activities: DataFrame containing activity logs with columns:
            - activity_id (str): Unique activity identifier
            - start_time (pd.Timestamp): Activity start time
            - item_id (str): Associated Fabric item ID
        job_details: DataFrame containing job execution details with columns:
            - job_id (str): Unique job identifier
            - start_time (pd.Timestamp): Job start time
            - end_time (pd.Timestamp): Job end time
            - item_id (str): Associated Fabric item ID
        tolerance_minutes: Maximum time difference (in minutes) for matching
            activities to jobs. Default is 5 minutes.
    
    Returns:
        Enhanced DataFrame with restored timing data. Includes all original
        activity columns plus:
            - end_time (pd.Timestamp): Restored end time from job details
            - duration_seconds (float): Calculated duration
            - job_id (str): Matched job identifier
    
    Raises:
        ValueError: If required columns are missing from input DataFrames
        TypeError: If inputs are not pandas DataFrames
    
    Example:
        >>> activities = pd.DataFrame({
        ...     'activity_id': ['a1', 'a2'],
        ...     'start_time': [pd.Timestamp('2024-12-01 10:00:00'),
        ...                    pd.Timestamp('2024-12-01 11:00:00')],
        ...     'item_id': ['item1', 'item2']
        ... })
        >>> jobs = pd.DataFrame({
        ...     'job_id': ['j1', 'j2'],
        ...     'start_time': [pd.Timestamp('2024-12-01 10:00:30'),
        ...                    pd.Timestamp('2024-12-01 11:01:00')],
        ...     'end_time': [pd.Timestamp('2024-12-01 10:15:00'),
        ...                  pd.Timestamp('2024-12-01 11:30:00')],
        ...     'item_id': ['item1', 'item2']
        ... })
        >>> result = merge_activities(activities, jobs)
        >>> result['duration_seconds'].tolist()
        [900.0, 1740.0]
    
    Note:
        The Smart Merge algorithm uses pandas merge_asof for temporal matching,
        which requires sorted timestamps. This function handles sorting internally.
    """
    # Implementation...
```

### Naming Conventions

- **Functions/Variables**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private methods**: `_leading_underscore`

```python
# ✅ GOOD
MAX_RETRY_ATTEMPTS = 3
class MonitorHubPipeline:
    def extract_activities(self, workspace_id: str) -> pd.DataFrame:
        return self._fetch_from_api(workspace_id)
    
    def _fetch_from_api(self, workspace_id: str) -> dict:
        # Private helper method

# ❌ BAD
maxRetryAttempts = 3
class monitor_hub_pipeline:
    def ExtractActivities(self, workspaceId):
        return self.FetchFromAPI(workspaceId)
```

### Code Organization

- **One class per file** (unless tightly coupled)
- **Related functions grouped together**
- **Imports at the top**
- **Constants after imports**

```python
# Standard library imports
import logging
from datetime import datetime
from typing import Dict, List, Optional

# Third-party imports
import pandas as pd
import numpy as np
from azure.identity import DefaultAzureCredential

# Local imports
from usf_fabric_monitoring.core.data_loader import DataLoader
from usf_fabric_monitoring.core.enrichment import EnrichmentEngine

# Constants
MAX_HISTORICAL_DAYS = 28
DEFAULT_TOLERANCE_MINUTES = 5
API_RATE_LIMIT = 180

# Logger setup
logger = logging.getLogger(__name__)

# Classes and functions...
```

### Error Handling

- **Use specific exceptions**
- **Provide context in error messages**
- **Log errors appropriately**

```python
# ✅ GOOD
def validate_workspace_id(workspace_id: str) -> None:
    """Validate workspace ID format."""
    if not re.match(r'^[a-f0-9\-]{36}$', workspace_id):
        raise ValueError(
            f"Invalid workspace ID format: {workspace_id}. "
            "Expected a valid GUID (e.g., '12345678-1234-1234-1234-123456789abc')"
        )
    logger.debug(f"Validated workspace ID: {workspace_id}")

# ❌ BAD
def validate_workspace_id(workspace_id):
    if not re.match(r'^[a-f0-9\-]{36}$', workspace_id):
        raise Exception("Bad ID")
```

### Logging

Use the standard logging module with appropriate levels:

```python
import logging

logger = logging.getLogger(__name__)

# DEBUG: Detailed diagnostic information
logger.debug(f"Processing {len(activities)} activities")

# INFO: General informational messages
logger.info("Smart Merge completed successfully")

# WARNING: Something unexpected but not critical
logger.warning(f"No job details found for {activity_id}, using fallback")

# ERROR: Error that prevented completion
logger.error(f"Failed to extract activities: {str(e)}")

# CRITICAL: Serious error requiring immediate attention
logger.critical("Authentication failed: invalid credentials")
```

---

## Testing Guidelines

### Test Coverage Requirements

- **Minimum coverage**: 70% for pull requests to be accepted
- **Target coverage**: 80%+ for the project
- **Critical modules**: 90%+ coverage (pipeline, Smart Merge, enforcement)

### Test Types

#### 1. Unit Tests

Test individual functions and methods in isolation:

```python
# tests/test_duration_fix.py
import pytest
import pandas as pd
from usf_fabric_monitoring.core.pipeline import MonitorHubPipeline

def test_calculate_duration_with_valid_times():
    """Test duration calculation with valid start and end times."""
    start = pd.Timestamp('2024-12-01 10:00:00')
    end = pd.Timestamp('2024-12-01 10:15:00')
    
    duration = MonitorHubPipeline._calculate_duration(start, end)
    
    assert duration == 900.0  # 15 minutes = 900 seconds

def test_calculate_duration_with_missing_end_time():
    """Test duration calculation gracefully handles missing end time."""
    start = pd.Timestamp('2024-12-01 10:00:00')
    end = pd.NaT
    
    duration = MonitorHubPipeline._calculate_duration(start, end)
    
    assert pd.isna(duration)

def test_calculate_duration_negative_protection():
    """Test that duration is never negative even with reversed times."""
    start = pd.Timestamp('2024-12-01 10:15:00')
    end = pd.Timestamp('2024-12-01 10:00:00')  # End before start
    
    duration = MonitorHubPipeline._calculate_duration(start, end)
    
    assert duration >= 0
```

#### 2. Integration Tests

Test how components work together:

```python
# tests/test_complete_pipeline.py
def test_end_to_end_pipeline_with_duration_fix():
    """Test complete pipeline from data loading to report generation."""
    # Setup
    pipeline = MonitorHubPipeline()
    test_data_path = "tests/fixtures/sample_activities.csv"
    
    # Execute
    pipeline.load_data(test_data_path)
    pipeline.apply_smart_merge()
    reports = pipeline.generate_reports()
    
    # Verify
    assert reports['summary'] is not None
    assert 'duration_seconds' in reports['summary'].columns
    assert reports['summary']['duration_seconds'].notna().sum() > 0
```

#### 3. End-to-End Tests

Test the full system with realistic scenarios:

```python
# tests/test_offline_analysis.py
def test_offline_analysis_complete_workflow():
    """Test complete workflow using existing CSV data (no API calls)."""
    # This test validates the full analysis without requiring Fabric API access
    # Useful for CI/CD pipelines and local development
    
    # Load existing data
    activities = pd.read_csv("exports/test_data/activities.csv")
    jobs = pd.read_csv("exports/test_data/job_details.csv")
    
    # Run analysis
    pipeline = MonitorHubPipeline()
    pipeline.load_from_dataframes(activities, jobs)
    results = pipeline.run_analysis()
    
    # Validate results
    assert len(results['failed_activities']) > 0
    assert results['duration_recovery_rate'] >= 0.95  # 95%+ recovery
```

### Test Fixtures

Store reusable test data in `tests/fixtures/`:

```python
# tests/fixtures/__init__.py
import pandas as pd

def get_sample_activities() -> pd.DataFrame:
    """Return sample activity data for testing."""
    return pd.DataFrame({
        'activity_id': ['a1', 'a2', 'a3'],
        'start_time': pd.to_datetime([
            '2024-12-01 10:00:00',
            '2024-12-01 11:00:00',
            '2024-12-01 12:00:00'
        ]),
        'item_id': ['item1', 'item2', 'item3'],
        'status': ['Success', 'Failed', 'Success']
    })
```

### Running Tests

```bash
# Run all tests
make test

# Run specific test file
pytest tests/test_duration_fix.py -v

# Run with coverage report
pytest --cov=src/usf_fabric_monitoring --cov-report=html

# Run only unit tests
pytest tests/unit/ -v

# Run only integration tests
pytest tests/integration/ -v
```

### Test Naming Convention

- Test files: `test_<module_name>.py`
- Test functions: `test_<function_name>_<scenario>`

```python
# ✅ GOOD
def test_merge_activities_with_valid_data()
def test_merge_activities_with_missing_job_details()
def test_merge_activities_raises_error_on_invalid_input()

# ❌ BAD
def test1()
def test_merge()
def my_test()
```

---

## Documentation Standards

### README Updates

Update `README.md` when:
- Adding new features or commands
- Changing configuration options
- Modifying installation steps
- Adding new dependencies

### Inline Documentation

- **All public functions must have docstrings**
- **Complex logic should have explanatory comments**
- **Avoid obvious comments**

```python
# ✅ GOOD
# Use merge_asof for temporal proximity matching since activities
# and jobs don't have exact timestamp alignment. The Smart Merge
# algorithm allows up to 5 minutes tolerance.
merged = pd.merge_asof(
    activities.sort_values('start_time'),
    jobs.sort_values('start_time'),
    on='start_time',
    by='item_id',
    tolerance=pd.Timedelta(minutes=5),
    direction='nearest'
)

# ❌ BAD
# Merge dataframes
merged = pd.merge_asof(activities, jobs, on='start_time')  # What? Why merge_asof?
```

### CHANGELOG

Update `CHANGELOG.md` with every PR using the format:

```markdown
## [Unreleased]

### Added
- Smart Merge optimization for large datasets (#123)
- New `--batch-size` parameter for memory efficiency

### Changed
- Updated pandas requirement to 2.1.0 for performance improvements
- Improved error messages in authentication module

### Fixed
- Fixed pandas SettingWithCopyWarning in historical_analyzer.py (#125)
- Resolved test failure in test_inference_config.py

### Deprecated
- `load_legacy_format()` will be removed in v0.3.0

### Security
- Updated Azure SDK to patch CVE-2024-XXXX
```

---

## Pull Request Process

### Before Submitting

1. **Ensure all tests pass**: `make test`
2. **Check code quality**: `make lint`
3. **Format code**: `make format`
4. **Update documentation**: README, docstrings, CHANGELOG
5. **Verify test coverage**: `pytest --cov`

### PR Title Format

Use **Conventional Commits** format:

```
<type>(<scope>): <description>

Examples:
feat(smart-merge): add batch processing for large datasets
fix(auth): resolve token refresh issue in long-running jobs
docs(readme): update installation instructions for Python 3.11
test(pipeline): add integration tests for Smart Merge
refactor(analyzer): simplify duration calculation logic
chore(deps): update pandas to 2.1.0
```

**Types**:
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, no logic change)
- `refactor`: Code refactoring (no behavior change)
- `test`: Adding or updating tests
- `chore`: Maintenance tasks (dependencies, build config)
- `perf`: Performance improvements

### PR Description Template

```markdown
## Description
[Brief description of what this PR does]

## Related Issue
Closes #123

## Type of Change
- [ ] Bug fix (non-breaking change which fixes an issue)
- [ ] New feature (non-breaking change which adds functionality)
- [ ] Breaking change (fix or feature that would cause existing functionality to not work as expected)
- [ ] Documentation update

## Changes Made
- Added Smart Merge batch processing
- Updated duration calculation logic
- Added integration tests for large datasets

## Testing
- [ ] Unit tests added/updated
- [ ] Integration tests added/updated
- [ ] All tests pass locally
- [ ] Test coverage maintained/improved

## Documentation
- [ ] README updated
- [ ] Docstrings added/updated
- [ ] CHANGELOG updated
- [ ] Inline comments added where necessary

## Screenshots (if applicable)
[Add screenshots for UI changes]

## Additional Notes
[Any additional context, implementation decisions, or considerations]
```

### Review Process

1. **Automated Checks**: CI/CD pipeline runs tests and linting
2. **Code Review**: At least one maintainer must approve
3. **Address Feedback**: Make requested changes in new commits
4. **Final Approval**: Maintainer approves and merges

### Commit Message Guidelines

Follow the **Conventional Commits** specification:

```bash
# Format
<type>(<scope>): <subject>

<body>

<footer>

# Examples
feat(smart-merge): add tolerance parameter for temporal matching

Allows users to configure the maximum time difference for matching
activities to jobs. Default remains 5 minutes but can be adjusted
based on data characteristics.

Closes #123

---

fix(analyzer): resolve pandas SettingWithCopyWarning

Changed direct DataFrame column assignment to use .loc accessor
following pandas best practices.

Fixes #125

---

docs(contributing): add comprehensive contribution guidelines

Added detailed sections covering:
- Code of Conduct
- Development Workflow
- Coding Standards
- Testing Guidelines
- Documentation Standards
- Pull Request Process

Related to #130
```

---

## Release Process

We follow **Semantic Versioning** (SemVer): `MAJOR.MINOR.PATCH`

- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

### Release Checklist

1. **Create release branch**:
   ```bash
   git checkout -b release/0.3.0
   ```

2. **Update version numbers**:
   - `pyproject.toml`: `version = "0.3.0"`
   - `src/usf_fabric_monitoring/__init__.py`: `__version__ = "0.3.0"`

3. **Update CHANGELOG.md**:
   - Move items from `[Unreleased]` to `[0.3.0] - YYYY-MM-DD`
   - Add comparison links

4. **Run full test suite**:
   ```bash
   make test-all
   make test-complete-pipeline
   ```

5. **Build package**:
   ```bash
   make build
   ```

6. **Create PR** to `main` with title: `Release v0.3.0`

7. **After merge, create Git tag**:
   ```bash
   git tag -a v0.3.0 -m "Release v0.3.0"
   git push origin v0.3.0
   ```

8. **Create GitHub Release**:
   - Use tag `v0.3.0`
   - Title: `v0.3.0 - <Release Name>`
   - Copy CHANGELOG content to release notes
   - Attach `.whl` file

---

## Project-Specific Guidelines

### Smart Merge Algorithm

When modifying the Smart Merge algorithm:

1. **Preserve backward compatibility**: Ensure existing functionality still works
2. **Add configuration options**: Don't hardcode new parameters
3. **Update tests**: Add tests for new scenarios
4. **Document behavior**: Explain algorithm decisions in comments
5. **Validate edge cases**: Test with missing data, large datasets, edge timestamps

### Configuration Files

When adding/modifying configuration files in `config/`:

1. **Use JSON format**: For compatibility with notebooks
2. **Validate schema**: Add JSON schema if complex
3. **Document structure**: Add README in `config/` explaining each file
4. **Provide examples**: Include sample values with comments
5. **Update .env.template**: If new environment variables are needed

### Jupyter Notebooks

When creating/modifying notebooks in `notebooks/`:

1. **Clear all output**: Before committing (`Cell → All Output → Clear`)
2. **Use markdown cells**: Explain each section
3. **Cell execution order**: Ensure cells can run sequentially
4. **Import from package**: Use `from usf_fabric_monitoring import ...`
5. **Environment references**: Document required environment setup
6. **Test offline**: Verify notebook works with sample data

**Naming Convention**:
- `Monitor_Hub_Analysis.ipynb`: Main analysis notebook
- `Monitor_Hub_Analysis_Advanced.ipynb`: Advanced features
- `Workspace_Access_Audit.ipynb`: Specific use case

### API Changes

When modifying public APIs:

1. **Deprecation period**: Deprecate old API for one major version before removal
2. **Add warnings**: Use `warnings.warn()` for deprecated functions
3. **Provide migration path**: Document how to use new API
4. **Update examples**: Ensure all examples use new API

```python
import warnings

def load_activities_old(path: str) -> pd.DataFrame:
    """Load activities from CSV (DEPRECATED).
    
    .. deprecated:: 0.2.0
        Use :func:`load_activities_v2` instead. This function will be
        removed in v0.3.0.
    """
    warnings.warn(
        "load_activities_old() is deprecated and will be removed in v0.3.0. "
        "Use load_activities_v2() instead.",
        DeprecationWarning,
        stacklevel=2
    )
    return load_activities_v2(path)
```

---

## Questions or Need Help?

- **GitHub Discussions**: Ask questions in the Discussions tab
- **Issues**: Report bugs or request features
- **Email**: Contact maintainers at [email]

---

**Thank you for contributing to USF Fabric Monitoring!** 🚀
