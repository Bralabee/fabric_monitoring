"""
Extract Lineage for Mirrored Databases
"""
import os
import sys
import json
import base64
import logging
import argparse
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

# Add src to path
sys.path.insert(0, str(Path(__file__).parents[2]))

from dotenv import load_dotenv
from usf_fabric_monitoring.core.auth import create_authenticator_from_env

def setup_logging():
    """Setup logging configuration."""
    Path("logs").mkdir(exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            TimedRotatingFileHandler(
                'logs/lineage_extraction.log',
                when='midnight',
                interval=1,
                backupCount=30,
                encoding='utf-8'
            )
        ]
    )
    return logging.getLogger(__name__)

class LineageExtractor:
    def __init__(self):
        self.logger = setup_logging()
        load_dotenv()
        self.authenticator = create_authenticator_from_env()
        if not self.authenticator.validate_credentials():
            raise Exception("Authentication failed")
        self.token = self.authenticator.get_fabric_token()
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self.api_base = "https://api.fabric.microsoft.com/v1"

    def make_request_with_retry(self, method, url, **kwargs):
        """Make HTTP request with retry logic for 429s."""
        max_retries = 5
        base_delay = 2
        
        for attempt in range(max_retries):
            try:
                response = requests.request(method, url, headers=self.headers, **kwargs)
                
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", base_delay * (2 ** attempt)))
                    self.logger.warning(f"Rate limited. Waiting {retry_after}s before retry {attempt + 1}/{max_retries}...")
                    import time
                    time.sleep(retry_after)
                    continue
                    
                return response
                
            except Exception as e:
                self.logger.error(f"Request failed: {str(e)}")
                if attempt == max_retries - 1:
                    raise
                import time
                time.sleep(base_delay)
                
        return None

    def get_workspaces(self):
        """Fetch all workspaces."""
        url = f"{self.api_base}/workspaces"
        workspaces = []
        
        while url:
            response = self.make_request_with_retry("GET", url)
            if not response or response.status_code != 200:
                self.logger.error(f"Failed to fetch workspaces: {response.text if response else 'No response'}")
                break
                
            data = response.json()
            workspaces.extend(data.get("value", []))
            url = data.get("continuationUri")
            
        self.logger.info(f"Found {len(workspaces)} workspaces")
        return workspaces

    def get_mirrored_databases(self, workspace_id):
        """Fetch Mirrored Databases in a workspace."""
        url = f"{self.api_base}/workspaces/{workspace_id}/mirroredDatabases"
        items = []
        
        try:
            response = self.make_request_with_retry("GET", url)
            if response and response.status_code == 200:
                items = response.json().get("value", [])
            elif response and response.status_code == 403:
                self.logger.warning(f"Access denied for workspace {workspace_id}")
            else:
                code = response.status_code if response else "Unknown"
                self.logger.warning(f"Failed to fetch mirrored DBs for {workspace_id}: {code}")
        except Exception as e:
            self.logger.error(f"Error fetching items for {workspace_id}: {str(e)}")
            
        return items

    def get_lakehouses(self, workspace_id):
        """Fetch Lakehouses in a workspace."""
        url = f"{self.api_base}/workspaces/{workspace_id}/items?type=Lakehouse"
        items = []
        
        try:
            while url:
                response = self.make_request_with_retry("GET", url)
                if not response or response.status_code != 200:
                    code = response.status_code if response else "Unknown"
                    if code != 403: 
                        self.logger.warning(f"Failed to fetch Lakehouses for {workspace_id}: {code}")
                    break
                    
                data = response.json()
                items.extend(data.get("value", []))
                url = data.get("continuationUri")
                
        except Exception as e:
            self.logger.error(f"Error fetching Lakehouses for {workspace_id}: {str(e)}")
            
        return items
    def get_lakehouses(self, workspace_id):
        """Fetch Lakehouses in a workspace."""
        url = f"{self.api_base}/workspaces/{workspace_id}/items?type=Lakehouse"
        items = []
        
        try:
            while url:
                response = self.make_request_with_retry("GET", url)
                if not response or response.status_code != 200:
                    code = response.status_code if response else "Unknown"
                    if code != 403: 
                        self.logger.warning(f"Failed to fetch Lakehouses for {workspace_id}: {code}")
                    break
                    
                data = response.json()
                items.extend(data.get("value", []))
                url = data.get("continuationUri")
                
        except Exception as e:
            self.logger.error(f"Error fetching Lakehouses for {workspace_id}: {str(e)}")
            
        return items

    def get_shortcuts(self, workspace_id, item_id):
        """Fetch shortcuts for a specific item (Lakehouse)."""
        url = f"{self.api_base}/workspaces/{workspace_id}/items/{item_id}/shortcuts"
        shortcuts = []
        
        try:
            response = self.make_request_with_retry("GET", url)
            if response and response.status_code == 200:
                shortcuts = response.json().get("value", [])
        except Exception as e:
            self.logger.error(f"Error fetching shortcuts for {item_id}: {str(e)}")
            
        return shortcuts

    def get_definition(self, workspace_id, item_id):
        """Get definition for a Mirrored Database."""
        url = f"{self.api_base}/workspaces/{workspace_id}/mirroredDatabases/{item_id}/getDefinition"
        
        try:
            response = self.make_request_with_retry("POST", url)
            if response and response.status_code == 200:
                return response.json()
            else:
                self.logger.error(f"Failed to get definition for {item_id}: {response.text if response else 'No response'}")
                return None
        except Exception as e:
            self.logger.error(f"Error getting definition for {item_id}: {str(e)}")
            return None

    def decode_payload(self, payload_base64):
        """Decode Base64 payload."""
        try:
            decoded_bytes = base64.b64decode(payload_base64)
            return json.loads(decoded_bytes.decode('utf-8'))
        except Exception as e:
            self.logger.error(f"Failed to decode payload: {str(e)}")
            return None

    def extract_lineage(self, output_dir="exports/lineage"):
        """Main extraction logic."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        workspaces = self.get_workspaces()
        lineage_data = []
        
        for ws in workspaces:
            ws_id = ws["id"]
            ws_name = ws.get("displayName", "Unknown")
            
            self.logger.info(f"Scanning workspace: {ws_name}")
            
            # Proactive delay to avoid rate limits
            import time
            time.sleep(0.2)
            
            # --- 1. Mirrored Databases ---
            try:
                mirrored_dbs = self.get_mirrored_databases(ws_id)
                for db in mirrored_dbs:
                    db_id = db["id"]
                    db_name = db.get("displayName", "Unknown")
                    
                    self.logger.info(f"  Found Mirrored DB: {db_name}")
                    
                    definition = self.get_definition(ws_id, db_id)
                    if definition:
                        parts = definition.get("definition", {}).get("parts", [])
                        for part in parts:
                            payload = part.get("payload")
                            if payload:
                                decoded = self.decode_payload(payload)
                                if decoded:
                                    # Extract Source Properties
                                    source_props = decoded.get("SourceProperties", {})
                                    source_type_props = decoded.get("SourceTypeProperties", {})
                                    
                                    lineage_data.append({
                                        "Workspace Name": ws_name,
                                        "Workspace ID": ws_id,
                                        "Item Name": db_name,
                                        "Item ID": db_id,
                                        "Item Type": "MirroredDatabase",
                                        "Shortcut Name": None,
                                        "Shortcut Path": None,
                                        "Source Type": source_props.get("sourceType", "Unknown"),
                                        "Source Connection": source_props.get("connection", "Unknown"),
                                        "Source Database": source_props.get("database", "Unknown"),
                                        "Connection ID": source_type_props.get("connectionIdentifier", "Unknown"),
                                        "Full Definition": json.dumps(decoded) # For debugging
                                    })
            except Exception as e:
                 self.logger.error(f"Error processing Mirrored DBs in {ws_name}: {e}")

            # --- 2. Lakehouse Shortcuts ---
            try:
                lakehouses = self.get_lakehouses(ws_id)
                if lakehouses:
                     self.logger.info(f"  Found {len(lakehouses)} Lakehouses, checking shortcuts...")

                for lh in lakehouses:
                    lh_id = lh["id"]
                    lh_name = lh.get("displayName", "Unknown")
                    
                    shortcuts = self.get_shortcuts(ws_id, lh_id)
                    
                    for shortcut in shortcuts:
                        try:
                            sc_name = shortcut.get("name", "Unknown")
                            sc_path = shortcut.get("path", "Unknown")
                            target = shortcut.get("target", {})
                            
                            self.logger.info(f"    Found Shortcut: {sc_name} in {lh_name}")
                            
                            lineage_data.append({
                                "Workspace Name": ws_name,
                                "Workspace ID": ws_id,
                                "Item Name": lh_name,
                                "Item ID": lh_id,
                                "Item Type": "Lakehouse Shortcut",
                                "Shortcut Name": sc_name,
                                "Shortcut Path": sc_path,
                                "Source Type": target.get("type", "Unknown"),
                                "Source Connection": target.get("path") or target.get("location") or str(target),
                                "Source Database": None,
                                "Connection ID": None,
                                "Full Definition": json.dumps(shortcut)
                            })
                        except Exception as e:
                             self.logger.warning(f"Error parsing shortcut {sc_name} in {lh_name}: {e}")

            except Exception as e:
                self.logger.error(f"Error processing Shortcuts in {ws_name}: {e}")
        
        if lineage_data:
            df = pd.DataFrame(lineage_data)
            filename = f"mirrored_lineage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            filepath = output_path / filename
            df.to_csv(filepath, index=False)
            self.logger.info(f"✅ Lineage exported to {filepath}")
            
            # Print Summary
            print("\n" + "="*40)
            print("🔗 LINEAGE SUMMARY")
            print("="*40)
            print(f"Total Items Found: {len(df)}")
            print(f"Mirrored Databases: {len(df[df['Item Type'] == 'MirroredDatabase'])}")
            print(f"Shortcuts: {len(df[df['Item Type'] == 'Lakehouse Shortcut'])}")
            if "Source Type" in df.columns:
                print("\nTop Source Types:")
                print(df["Source Type"].value_counts().head().to_string())
            print("\n" + "="*40 + "\n")
            
        else:
            self.logger.warning("No lineage data found.")

def main():
    parser = argparse.ArgumentParser(description="Extract Lineage for Mirrored Databases and Shortcuts")
    parser.add_argument("--output-dir", default="exports/lineage", help="Output directory")
    args = parser.parse_args()
    
    extractor = LineageExtractor()
    extractor.extract_lineage(args.output_dir)

if __name__ == "__main__":
    main()
