from pathlib import Path

from usf_fabric_monitoring.core.config_validation import validate_config_dir


def test_repo_config_json_validates():
    repo_root = Path(__file__).resolve().parents[1]
    results = validate_config_dir(repo_root / "config", only_known_files=True)
    assert results == {}
