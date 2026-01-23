from __future__ import annotations

import sys
from pathlib import Path
import importlib.util


# Add src and scripts to path for testing
repo_root = Path(__file__).parents[1]
sys.path.insert(0, str(repo_root / "src"))
sys.path.insert(0, str(repo_root / "scripts"))


def _import_script_function(script_name: str, func_name: str):
    """Dynamically import a function from a script in the scripts directory."""
    script_path = repo_root / "scripts" / f"{script_name}.py"
    if not script_path.exists():
        raise ImportError(f"Script not found: {script_path}")
    spec = importlib.util.spec_from_file_location(script_name, script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return getattr(module, func_name)


def test_validate_config_cli_returns_zero_for_repo_config():
    main = _import_script_function("validate_config", "main")

    config_dir = repo_root / "config"

    exit_code = main([str(config_dir), "--json"])
    assert exit_code == 0
