import os
from pathlib import Path

def is_fabric_environment() -> bool:
    """
    Check if the code is running in a Microsoft Fabric environment.
    
    Returns:
        bool: True if running in Fabric, False otherwise.
    """
    # Check for common Fabric/Spark environment variables or paths
    # /lakehouse/default is a standard path in Fabric Notebooks attached to a Lakehouse
    return Path("/lakehouse/default").exists() or os.getenv("IS_FABRIC") == "true"


def _find_project_root() -> Path:
    """
    Find the project root directory by looking for marker files.
    
    Searches upward from current directory for pyproject.toml or Makefile.
    This handles cases where code runs from subdirectories (e.g., notebooks/).
    
    Returns:
        Path: Project root directory, or current directory if not found.
    """
    cwd = Path(os.getcwd()).resolve()

    # Search up to 3 levels for project markers
    for check_dir in [cwd, cwd.parent, cwd.parent.parent]:
        if (check_dir / "pyproject.toml").exists() or (check_dir / "Makefile").exists():
            return check_dir

    # Fallback to current directory
    return cwd


def get_base_output_path() -> Path:
    """
    Get the base output path for data persistence.
    
    Returns:
        Path: '/lakehouse/default/Files' (or FABRIC_BASE_PATH) if in Fabric,
              else project root directory for local development.
    """
    if is_fabric_environment():
        return Path(os.getenv("FABRIC_BASE_PATH", "/lakehouse/default/Files"))
    return _find_project_root()

def resolve_path(relative_path: str) -> Path:
    """
    Resolve a relative path to the correct absolute path based on the environment.
    
    Args:
        relative_path: The relative path (e.g., 'exports/data')
        
    Returns:
        Path: Absolute path rooted in Lakehouse if in Fabric, else project root.
    """
    return get_base_output_path() / relative_path
