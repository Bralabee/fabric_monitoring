"""
USF Fabric Monitoring - Power BI Report Deployment Script
=========================================================
Version: 0.3.8
Author: Sanmi Ibitoye
Email: Sanmi.Ibitoye@leit.ltd
Last Updated: 18-12-2025

This script creates a Power BI report programmatically using the Fabric REST APIs.
It can be run locally or in a Microsoft Fabric notebook.

Prerequisites:
- Service Principal with Power BI Admin permissions OR user authentication
- Semantic model already created from star schema Delta tables
- Environment variables: AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID

Usage:
    # Local (with .env file)
    python deploy_powerbi_report.py --workspace "Your Workspace" --dataset "USF Fabric Monitoring Model"
    
    # In Fabric notebook
    %run deploy_powerbi_report.py
"""

import os
import sys
import json
import requests
import argparse
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any, List

# Try to import azure.identity for authentication
try:
    from azure.identity import ClientSecretCredential, DefaultAzureCredential
    AZURE_IDENTITY_AVAILABLE = True
except ImportError:
    AZURE_IDENTITY_AVAILABLE = False
    print("Warning: azure.identity not available. Install with: pip install azure-identity")

# Try to import dotenv for local development
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass


# =============================================================================
# CONFIGURATION
# =============================================================================

# Power BI REST API endpoints
POWERBI_API_BASE = "https://api.powerbi.com/v1.0/myorg"
FABRIC_API_BASE = "https://api.fabric.microsoft.com/v1"

# Default report configuration
DEFAULT_REPORT_NAME = "USF Fabric Monitoring Dashboard"
DEFAULT_DATASET_NAME = "USF Fabric Monitoring Model"

# Load report layout from JSON file
SCRIPT_DIR = Path(__file__).parent
REPORT_LAYOUT_FILE = SCRIPT_DIR / "report_layout.json"
MEASURES_FILE = SCRIPT_DIR / "measures" / "fabric_monitoring_measures.dax"


# =============================================================================
# AUTHENTICATION
# =============================================================================

class PowerBIAuth:
    """Handle authentication to Power BI / Fabric APIs."""
    
    def __init__(self):
        self.token: Optional[str] = None
        self.credential = None
        
    def get_token(self) -> str:
        """Get an access token for Power BI API."""
        if self.token:
            return self.token
            
        # Try Service Principal first
        client_id = os.getenv("AZURE_CLIENT_ID")
        client_secret = os.getenv("AZURE_CLIENT_SECRET")
        tenant_id = os.getenv("AZURE_TENANT_ID")
        
        if client_id and client_secret and tenant_id and AZURE_IDENTITY_AVAILABLE:
            print("Authenticating with Service Principal...")
            self.credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
        elif AZURE_IDENTITY_AVAILABLE:
            print("Authenticating with DefaultAzureCredential...")
            self.credential = DefaultAzureCredential()
        else:
            raise RuntimeError("No authentication method available")
            
        # Get token for Power BI scope
        token = self.credential.get_token("https://analysis.windows.net/powerbi/api/.default")
        self.token = token.token
        return self.token
        
    def get_headers(self) -> Dict[str, str]:
        """Get HTTP headers with authorization."""
        return {
            "Authorization": f"Bearer {self.get_token()}",
            "Content-Type": "application/json"
        }


# =============================================================================
# POWER BI API CLIENT
# =============================================================================

class PowerBIClient:
    """Client for Power BI REST API operations."""
    
    def __init__(self, auth: PowerBIAuth):
        self.auth = auth
        
    def _request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """Make an API request."""
        url = f"{POWERBI_API_BASE}/{endpoint}"
        headers = self.auth.get_headers()
        
        if method == "GET":
            response = requests.get(url, headers=headers)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=data)
        elif method == "PUT":
            response = requests.put(url, headers=headers, json=data)
        elif method == "DELETE":
            response = requests.delete(url, headers=headers)
        elif method == "PATCH":
            response = requests.patch(url, headers=headers, json=data)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
            
        if response.status_code >= 400:
            print(f"API Error: {response.status_code}")
            print(f"Response: {response.text}")
            response.raise_for_status()
            
        if response.text:
            return response.json()
        return {}
        
    def get_workspaces(self) -> List[Dict]:
        """Get list of workspaces."""
        result = self._request("GET", "groups")
        return result.get("value", [])
        
    def get_workspace_by_name(self, name: str) -> Optional[Dict]:
        """Get workspace by name."""
        workspaces = self.get_workspaces()
        for ws in workspaces:
            if ws.get("name", "").lower() == name.lower():
                return ws
        return None
        
    def get_datasets_in_workspace(self, workspace_id: str) -> List[Dict]:
        """Get datasets in a workspace."""
        result = self._request("GET", f"groups/{workspace_id}/datasets")
        return result.get("value", [])
        
    def get_dataset_by_name(self, workspace_id: str, name: str) -> Optional[Dict]:
        """Get dataset by name."""
        datasets = self.get_datasets_in_workspace(workspace_id)
        for ds in datasets:
            if ds.get("name", "").lower() == name.lower():
                return ds
        return None
        
    def get_reports_in_workspace(self, workspace_id: str) -> List[Dict]:
        """Get reports in a workspace."""
        result = self._request("GET", f"groups/{workspace_id}/reports")
        return result.get("value", [])
        
    def create_report(self, workspace_id: str, report_name: str, dataset_id: str) -> Dict:
        """Create a new report bound to a dataset."""
        # Note: Power BI REST API doesn't directly support creating reports with visuals
        # This creates an empty report - visuals need to be added via Power BI Desktop or Fabric
        data = {
            "name": report_name,
            "datasetId": dataset_id
        }
        return self._request("POST", f"groups/{workspace_id}/reports", data)
        
    def clone_report(self, workspace_id: str, report_id: str, new_name: str, 
                     target_workspace_id: Optional[str] = None,
                     target_dataset_id: Optional[str] = None) -> Dict:
        """Clone an existing report."""
        data = {
            "name": new_name,
            "targetWorkspaceId": target_workspace_id or workspace_id
        }
        if target_dataset_id:
            data["targetModelId"] = target_dataset_id
        return self._request("POST", f"groups/{workspace_id}/reports/{report_id}/Clone", data)
        
    def update_report_content(self, workspace_id: str, report_id: str, 
                              report_definition: Dict) -> Dict:
        """Update report content (requires specific format)."""
        return self._request("POST", 
                            f"groups/{workspace_id}/reports/{report_id}/UpdateReportContent",
                            report_definition)
                            
    def add_measure_to_dataset(self, workspace_id: str, dataset_id: str,
                               table_name: str, measure_name: str, 
                               expression: str) -> Dict:
        """Add a DAX measure to a dataset."""
        data = {
            "name": measure_name,
            "expression": expression
        }
        return self._request("POST", 
                            f"groups/{workspace_id}/datasets/{dataset_id}/tables/{table_name}/measures",
                            data)


# =============================================================================
# FABRIC API CLIENT (for semantic model operations)
# =============================================================================

class FabricClient:
    """Client for Microsoft Fabric REST API operations."""
    
    def __init__(self, auth: PowerBIAuth):
        self.auth = auth
        
    def _request(self, method: str, endpoint: str, data: Optional[Dict] = None) -> Dict:
        """Make an API request to Fabric API."""
        url = f"{FABRIC_API_BASE}/{endpoint}"
        headers = self.auth.get_headers()
        
        if method == "GET":
            response = requests.get(url, headers=headers)
        elif method == "POST":
            response = requests.post(url, headers=headers, json=data)
        elif method == "PATCH":
            response = requests.patch(url, headers=headers, json=data)
        elif method == "DELETE":
            response = requests.delete(url, headers=headers)
        else:
            raise ValueError(f"Unsupported HTTP method: {method}")
            
        if response.status_code >= 400:
            print(f"Fabric API Error: {response.status_code}")
            print(f"Response: {response.text}")
            response.raise_for_status()
            
        if response.text:
            return response.json()
        return {}
        
    def get_workspaces(self) -> List[Dict]:
        """Get list of Fabric workspaces."""
        result = self._request("GET", "workspaces")
        return result.get("value", [])
        
    def get_items_in_workspace(self, workspace_id: str, item_type: Optional[str] = None) -> List[Dict]:
        """Get items in a workspace, optionally filtered by type."""
        endpoint = f"workspaces/{workspace_id}/items"
        if item_type:
            endpoint += f"?type={item_type}"
        result = self._request("GET", endpoint)
        return result.get("value", [])
        
    def create_report(self, workspace_id: str, report_name: str, 
                      definition: Optional[Dict] = None) -> Dict:
        """Create a Power BI report in Fabric."""
        data = {
            "displayName": report_name,
            "type": "Report"
        }
        if definition:
            data["definition"] = definition
        return self._request("POST", f"workspaces/{workspace_id}/items", data)


# =============================================================================
# REPORT BUILDER
# =============================================================================

class ReportBuilder:
    """Build Power BI report configuration."""
    
    def __init__(self):
        self.report_layout = self._load_report_layout()
        self.measures = self._load_measures()
        
    def _load_report_layout(self) -> Dict:
        """Load report layout from JSON file."""
        if REPORT_LAYOUT_FILE.exists():
            with open(REPORT_LAYOUT_FILE, "r") as f:
                return json.load(f)
        return {}
        
    def _load_measures(self) -> List[Dict]:
        """Parse DAX measures from file."""
        measures = []
        if not MEASURES_FILE.exists():
            return measures
            
        with open(MEASURES_FILE, "r") as f:
            content = f.read()
            
        # Simple parser for DAX measures
        # Format: // Comment\nMeasure Name = \nDAX Expression
        lines = content.split("\n")
        current_measure = None
        current_expression = []
        
        for line in lines:
            # Skip section comments
            if line.startswith("// ===") or line.startswith("// SECTION"):
                continue
            # Skip regular comments
            if line.startswith("//"):
                continue
            # Check for measure definition start
            if "=" in line and not line.strip().startswith("//"):
                if current_measure:
                    # Save previous measure
                    measures.append({
                        "name": current_measure,
                        "expression": "\n".join(current_expression).strip()
                    })
                # Start new measure
                parts = line.split("=", 1)
                current_measure = parts[0].strip()
                if len(parts) > 1:
                    current_expression = [parts[1].strip()]
                else:
                    current_expression = []
            elif current_measure:
                # Continue expression
                if line.strip():
                    current_expression.append(line)
                elif current_expression:
                    # Empty line ends the measure
                    measures.append({
                        "name": current_measure,
                        "expression": "\n".join(current_expression).strip()
                    })
                    current_measure = None
                    current_expression = []
                    
        # Don't forget the last measure
        if current_measure:
            measures.append({
                "name": current_measure,
                "expression": "\n".join(current_expression).strip()
            })
            
        return measures
        
    def get_report_config(self) -> Dict:
        """Get full report configuration."""
        return self.report_layout
        
    def get_measures(self) -> List[Dict]:
        """Get list of DAX measures."""
        return self.measures
        
    def generate_deployment_script(self, workspace_name: str, dataset_name: str) -> str:
        """Generate a deployment script for Power BI Desktop."""
        script = f"""
// =============================================================================
// Power BI Desktop Deployment Script
// Generated: {datetime.now().isoformat()}
// =============================================================================

/*
INSTRUCTIONS:
1. Open Power BI Desktop
2. Connect to your semantic model: "{dataset_name}" in workspace "{workspace_name}"
3. Go to Model View
4. For each measure below:
   - Right-click the fact_activity table
   - Select "New Measure"
   - Paste the DAX expression
5. Save and publish to workspace

MEASURES TO ADD:
*/

"""
        for measure in self.measures:
            script += f"\n// Measure: {measure['name']}\n"
            script += f"{measure['name']} = \n{measure['expression']}\n"
            script += "\n"
            
        return script


# =============================================================================
# DEPLOYMENT ORCHESTRATOR
# =============================================================================

class ReportDeployer:
    """Orchestrate Power BI report deployment."""
    
    def __init__(self):
        self.auth = PowerBIAuth()
        self.pbi_client = PowerBIClient(self.auth)
        self.fabric_client = FabricClient(self.auth)
        self.builder = ReportBuilder()
        
    def deploy(self, workspace_name: str, dataset_name: str, 
               report_name: str = DEFAULT_REPORT_NAME) -> Dict:
        """Deploy the Power BI report."""
        print(f"\n{'='*60}")
        print("USF Fabric Monitoring - Power BI Report Deployment")
        print(f"{'='*60}\n")
        
        # Step 1: Find workspace
        print(f"Step 1: Finding workspace '{workspace_name}'...")
        workspace = self.pbi_client.get_workspace_by_name(workspace_name)
        if not workspace:
            raise ValueError(f"Workspace '{workspace_name}' not found")
        workspace_id = workspace["id"]
        print(f"  ✓ Found workspace: {workspace_id}")
        
        # Step 2: Find dataset
        print(f"\nStep 2: Finding dataset '{dataset_name}'...")
        dataset = self.pbi_client.get_dataset_by_name(workspace_id, dataset_name)
        if not dataset:
            print(f"  ⚠ Dataset not found. Available datasets:")
            datasets = self.pbi_client.get_datasets_in_workspace(workspace_id)
            for ds in datasets:
                print(f"    - {ds['name']}")
            raise ValueError(f"Dataset '{dataset_name}' not found in workspace")
        dataset_id = dataset["id"]
        print(f"  ✓ Found dataset: {dataset_id}")
        
        # Step 3: Check for existing report
        print(f"\nStep 3: Checking for existing report '{report_name}'...")
        reports = self.pbi_client.get_reports_in_workspace(workspace_id)
        existing_report = None
        for r in reports:
            if r.get("name", "").lower() == report_name.lower():
                existing_report = r
                break
                
        if existing_report:
            print(f"  ⚠ Report already exists: {existing_report['id']}")
            print("    To update, delete the existing report first.")
            return {"status": "exists", "report": existing_report}
            
        # Step 4: Create report
        print(f"\nStep 4: Creating report '{report_name}'...")
        # Note: Direct report creation with visuals requires Power BI Desktop
        # This creates a blank report bound to the dataset
        try:
            report = self.pbi_client.create_report(workspace_id, report_name, dataset_id)
            print(f"  ✓ Created report: {report.get('id', 'N/A')}")
        except Exception as e:
            print(f"  ⚠ Could not create report via API: {e}")
            print("    Use Power BI Desktop to create the report manually.")
            report = None
            
        # Step 5: Generate deployment instructions
        print("\nStep 5: Generating deployment instructions...")
        instructions = self._generate_instructions(workspace_name, dataset_name, report_name)
        print(instructions)
        
        # Step 6: Save measures to file
        print("\nStep 6: Saving DAX measures file...")
        measures_output = SCRIPT_DIR / "deployment" / f"measures_{datetime.now().strftime('%Y%m%d_%H%M%S')}.dax"
        measures_output.parent.mkdir(parents=True, exist_ok=True)
        
        with open(measures_output, "w") as f:
            f.write(self.builder.generate_deployment_script(workspace_name, dataset_name))
        print(f"  ✓ Saved to: {measures_output}")
        
        return {
            "status": "success",
            "workspace_id": workspace_id,
            "dataset_id": dataset_id,
            "report": report,
            "instructions": instructions
        }
        
    def _generate_instructions(self, workspace_name: str, dataset_name: str, 
                               report_name: str) -> str:
        """Generate manual deployment instructions."""
        return f"""
╔══════════════════════════════════════════════════════════════════════════════╗
║                    POWER BI REPORT DEPLOYMENT INSTRUCTIONS                    ║
╠══════════════════════════════════════════════════════════════════════════════╣
║                                                                              ║
║  Since Power BI reports with visuals cannot be created via API alone,        ║
║  please follow these steps in Power BI Desktop:                              ║
║                                                                              ║
║  OPTION 1: Power BI Desktop                                                  ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  1. Open Power BI Desktop                                                    ║
║  2. Get Data → Power BI datasets                                            ║
║  3. Connect to: "{dataset_name}"                                  ║
║  4. Go to Model View → Add measures from the .dax file                       ║
║  5. Go to Report View → Create visuals as per report_layout.json             ║
║  6. Publish to workspace: "{workspace_name}"                      ║
║                                                                              ║
║  OPTION 2: Fabric Direct Lake (Recommended)                                  ║
║  ─────────────────────────────────────────────────────────────────────────  ║
║  1. Go to your Fabric Lakehouse SQL Endpoint                                 ║
║  2. Click "New semantic model"                                               ║
║  3. Select all dim_* and fact_* tables                                       ║
║  4. Name it: "{dataset_name}"                                     ║
║  5. In semantic model, add relationships (see report_layout.json)            ║
║  6. Add DAX measures from the .dax file                                      ║
║  7. Click "Create report" to open in Power BI web                            ║
║  8. Build visuals using the layout guide                                     ║
║                                                                              ║
║  FILES GENERATED:                                                            ║
║  • powerbi/measures/fabric_monitoring_measures.dax - All DAX measures        ║
║  • powerbi/report_layout.json - Visual layout specification                  ║
║  • powerbi/deployment/*.dax - Timestamped deployment script                  ║
║                                                                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

    def list_measures(self) -> None:
        """List all available DAX measures."""
        print("\n" + "="*60)
        print("Available DAX Measures")
        print("="*60 + "\n")
        
        for i, measure in enumerate(self.builder.get_measures(), 1):
            print(f"{i:3}. {measure['name']}")
            
        print(f"\nTotal: {len(self.builder.get_measures())} measures")
        print(f"File: {MEASURES_FILE}")


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point for the deployment script."""
    parser = argparse.ArgumentParser(
        description="Deploy USF Fabric Monitoring Power BI Report"
    )
    parser.add_argument(
        "--workspace", "-w",
        required=True,
        help="Name of the Power BI / Fabric workspace"
    )
    parser.add_argument(
        "--dataset", "-d",
        default=DEFAULT_DATASET_NAME,
        help=f"Name of the semantic model/dataset (default: {DEFAULT_DATASET_NAME})"
    )
    parser.add_argument(
        "--report", "-r",
        default=DEFAULT_REPORT_NAME,
        help=f"Name for the report (default: {DEFAULT_REPORT_NAME})"
    )
    parser.add_argument(
        "--list-measures",
        action="store_true",
        help="List all available DAX measures and exit"
    )
    
    args = parser.parse_args()
    
    deployer = ReportDeployer()
    
    if args.list_measures:
        deployer.list_measures()
        return
        
    try:
        result = deployer.deploy(
            workspace_name=args.workspace,
            dataset_name=args.dataset,
            report_name=args.report
        )
        print("\n✓ Deployment process completed!")
        return result
    except Exception as e:
        print(f"\n✗ Deployment failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
