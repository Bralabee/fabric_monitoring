import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from usf_fabric_monitoring.core.enrichment import _load_inference_rules, infer_domain, infer_location


def test_load_inference_rules_returns_dict():
    rules = _load_inference_rules()
    assert isinstance(rules, dict)


def test_infer_domain_basic_keywords():
    assert infer_domain("HR Report") == "Human Resources"
    assert infer_domain("Sales Dashboard") == "Sales"


def test_infer_location_basic_keywords():
    assert infer_location({"name": "US Sales"}) == "Americas"
    assert infer_location({"name": "EMEA Operations"}) == "EMEA"
