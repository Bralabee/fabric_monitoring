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

def get_base_output_path() -> Path:
    """
    Get the base output path for data persistence.
    
    Returns:
        Path: '/lakehouse/default/Files' (or FABRIC_BASE_PATH) if in Fabric, else current working directory.
    """
    if is_fabric_environment():
        return Path(os.getenv("FABRIC_BASE_PATH", "/lakehouse/default/Files"))
    return Path(".")

def resolve_path(relative_path: str) -> Path:
    """
    Resolve a relative path to the correct absolute path based on the environment.
    
    Args:
        relative_path: The relative path (e.g., 'exports/data')
        
    Returns:
        Path: Absolute path rooted in Lakehouse if in Fabric, else local path.
    """
    return get_base_output_path() / relative_path
