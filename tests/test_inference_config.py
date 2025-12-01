import sys
import json
from pathlib import Path
import unittest

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from usf_fabric_monitoring.core.enrichment import infer_domain, infer_location, _load_inference_rules

class TestInferenceConfig(unittest.TestCase):
    def test_load_config(self):
        rules = _load_inference_rules()
        self.assertIsInstance(rules, dict)
        self.assertIn("domains", rules)
        self.assertIn("locations", rules)
        
    def test_infer_domain_from_config(self):
        # Test a standard one
        self.assertEqual(infer_domain("HR Report"), "Human Resources")
        
        # Test one that relies on the config (assuming config matches default for now)
        self.assertEqual(infer_domain("Sales Dashboard"), "Sales")
        
    def test_infer_location_from_config(self):
        # Test a standard one
        workspace = {"name": "US Sales"}
        self.assertEqual(infer_location(workspace), "Americas")
        
        workspace_emea = {"name": "EMEA Operations"}
        self.assertEqual(infer_location(workspace_emea), "EMEA")

if __name__ == "__main__":
    unittest.main()
