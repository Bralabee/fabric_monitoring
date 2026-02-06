"""
Simple test of the Monitor Hub pipeline
"""

import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from usf_fabric_monitoring.core.auth import create_authenticator_from_env
from usf_fabric_monitoring.core.extractor import FabricDataExtractor
from usf_fabric_monitoring.core.monitor_hub_reporter_clean import MonitorHubCSVReporter


@pytest.mark.integration
def test_pipeline():
    if not (os.getenv("AZURE_TENANT_ID") and os.getenv("AZURE_CLIENT_ID") and os.getenv("AZURE_CLIENT_SECRET")):
        pytest.skip("Missing AZURE_* service principal env vars; skipping integration test.")

    print("🚀 Testing Monitor Hub Pipeline Components")

    # Test 1: Authentication
    print("\n1. Testing authentication...")
    try:
        auth = create_authenticator_from_env()
        if auth.validate_credentials():
            print("   ✅ Authentication successful")
        else:
            pytest.fail("Authentication failed")
    except Exception as e:
        pytest.fail(f"Authentication error: {str(e)}")

    # Test 2: Data extraction (small test)
    print("\n2. Testing data extraction...")
    try:
        extractor = FabricDataExtractor(auth)
        print("   ✅ FabricDataExtractor initialized")

        # Quick connectivity test
        print("   🔍 Testing API connectivity...")
        connectivity = extractor.test_api_connectivity()

        if all(connectivity.values()):
            print(f"   ✅ All API tests passed: {connectivity}")
        else:
            print(f"   ⚠️  Some API tests failed: {connectivity}")

        # Test workspace enumeration (member-only for quick test)
        print("   🔍 Testing workspace enumeration...")
        workspaces = extractor.get_workspaces(tenant_wide=False, exclude_personal=True)
        print(f"   ✅ Found {len(workspaces)} member workspaces")

    except Exception as e:
        pytest.fail(f"Extraction failed: {str(e)}")

    # Test 3: Report generation
    print("\n3. Testing report generation...")
    try:
        reporter = MonitorHubCSVReporter("exports/test_reports")

        # Create comprehensive test data for demonstration
        # Load max days from environment
        max_days = int(os.getenv("MAX_HISTORICAL_DAYS", "28"))
        default_days = int(os.getenv("DEFAULT_ANALYSIS_DAYS", "7"))

        data = {
            "analysis_period": {
                "days": default_days,
                "start_date": "2024-11-08",
                "end_date": "2024-11-15",
                "description": f"{default_days}-day test period (API compliant)",
            },
            "activities": [
                {
                    "activity_id": "act_001",
                    "workspace_id": "ws_001",
                    "item_id": "item_001",
                    "item_name": "Sales Lakehouse",
                    "item_type": "Lakehouse",
                    "activity_type": "DataRefresh",
                    "status": "Succeeded",
                    "start_time": "2024-11-15T10:00:00Z",
                    "end_time": "2024-11-15T10:05:00Z",
                    "duration_seconds": 300,
                    "submitted_by": "analyst@contoso.com",
                    "created_by": "admin@contoso.com",
                    "domain": "contoso.com",
                    "location": "East US",
                    "is_simulated": True,
                },
                {
                    "activity_id": "act_002",
                    "workspace_id": "ws_001",
                    "item_id": "item_002",
                    "item_name": "Marketing Dashboard",
                    "item_type": "Report",
                    "activity_type": "ViewReport",
                    "status": "Failed",
                    "start_time": "2024-11-15T11:00:00Z",
                    "end_time": "2024-11-15T11:02:00Z",
                    "duration_seconds": 120,
                    "submitted_by": "user@contoso.com",
                    "created_by": "admin@contoso.com",
                    "domain": "contoso.com",
                    "location": "West US",
                    "is_simulated": True,
                },
                {
                    "activity_id": "act_003",
                    "workspace_id": "ws_002",
                    "item_id": "item_003",
                    "item_name": "Data Pipeline",
                    "item_type": "DataPipeline",
                    "activity_type": "PipelineRun",
                    "status": "Succeeded",
                    "start_time": "2024-11-14T15:30:00Z",
                    "end_time": "2024-11-14T16:00:00Z",
                    "duration_seconds": 1800,
                    "submitted_by": "engineer@fabrikam.com",
                    "created_by": "engineer@fabrikam.com",
                    "domain": "fabrikam.com",
                    "location": "Central US",
                    "is_simulated": True,
                },
            ],
            "workspaces": [
                {"id": "ws_001", "name": "Sales Analytics Workspace"},
                {"id": "ws_002", "name": "Data Engineering Workspace"},
            ],
            "items": [
                {"id": "item_001", "name": "Sales Lakehouse"},
                {"id": "item_002", "name": "Marketing Dashboard"},
                {"id": "item_003", "name": "Data Pipeline"},
            ],
        }

        reports = reporter.generate_comprehensive_reports(data)
        print(f"   ✅ Generated {len(reports)} reports")

        # List generated files
        for report_name, file_path in reports.items():
            file_name = Path(file_path).name
            print(f"   📄 {report_name}: {file_name}")

    except Exception as e:
        pytest.fail(f"Report generation failed: {str(e)}")

    print("\n✅ All tests passed!")
    print("\n📁 Check the exports/test_reports directory for generated files.")

    assert True
