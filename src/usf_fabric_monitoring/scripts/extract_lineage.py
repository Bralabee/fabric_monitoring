"""
Extract Lineage for Mirrored Databases
"""
import json
import base64
import logging
import argparse
import requests
from pathlib import Path
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

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

    def get_kql_databases(self, workspace_id):
        """Fetch KQL Databases in a workspace."""
        url = f"{self.api_base}/workspaces/{workspace_id}/items?type=KQLDatabase"
        items = []

        try:
            while url:
                response = self.make_request_with_retry("GET", url)
                if not response or response.status_code != 200:
                    code = response.status_code if response else "Unknown"
                    if code != 403:
                        self.logger.warning(f"Failed to fetch KQL Databases for {workspace_id}: {code}")
                    break

                data = response.json()
                items.extend(data.get("value", []))
                url = data.get("continuationUri")

        except Exception as e:
            self.logger.error(f"Error fetching KQL Databases for {workspace_id}: {str(e)}")

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

            # --- 3. KQL Database Shortcuts ---
            try:
                kql_dbs = self.get_kql_databases(ws_id)
                if kql_dbs:
                     self.logger.info(f"  Found {len(kql_dbs)} KQL Databases, checking shortcuts...")

                for db in kql_dbs:
                    db_id = db["id"]
                    db_name = db.get("displayName", "Unknown")

                    shortcuts = self.get_shortcuts(ws_id, db_id)

                    for shortcut in shortcuts:
                        try:
                            sc_name = shortcut.get("name", "Unknown")
                            sc_path = shortcut.get("path", "Unknown")
                            target = shortcut.get("target", {})

                            self.logger.info(f"    Found Shortcut: {sc_name} in {db_name}")

                            lineage_data.append({
                                "Workspace Name": ws_name,
                                "Workspace ID": ws_id,
                                "Item Name": db_name,
                                "Item ID": db_id,
                                "Item Type": "KQLDatabase Shortcut",
                                "Shortcut Name": sc_name,
                                "Shortcut Path": sc_path,
                                "Source Type": target.get("type", "Unknown"),
                                "Source Connection": target.get("path") or target.get("location") or str(target),
                                "Source Database": None,
                                "Connection ID": None,
                                "Full Definition": json.dumps(shortcut)
                            })
                        except Exception as e:
                             self.logger.warning(f"Error parsing shortcut {sc_name} in {db_name}: {e}")

            except Exception as e:
                self.logger.error(f"Error processing KQL Shortcuts in {ws_name}: {e}")

        if lineage_data:
            # Output as JSON (preserves structure, no CSV flattening)
            output_data = {
                "extracted_at": datetime.now().isoformat(),
                "total_items": len(lineage_data),
                "lineage": lineage_data
            }

            filename = f"lineage_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            filepath = output_path / filename

            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)

            self.logger.info(f"✅ Lineage exported to {filepath}")

            # Print Summary
            mirrored_count = sum(1 for item in lineage_data if item.get('Item Type') == 'MirroredDatabase')
            lakehouse_count = sum(1 for item in lineage_data if item.get('Item Type') == 'Lakehouse Shortcut')
            kql_count = sum(1 for item in lineage_data if item.get('Item Type') == 'KQLDatabase Shortcut')

            # Count source types
            source_types = {}
            for item in lineage_data:
                st = item.get('Source Type', 'Unknown')
                source_types[st] = source_types.get(st, 0) + 1

            print("\n" + "="*40)
            print("🔗 LINEAGE SUMMARY")
            print("="*40)
            print(f"Total Items Found: {len(lineage_data)}")
            print(f"Mirrored Databases: {mirrored_count}")
            print(f"Lakehouse Shortcuts: {lakehouse_count}")
            print(f"KQL Shortcuts: {kql_count}")
            print("\nTop Source Types:")
            for st, count in sorted(source_types.items(), key=lambda x: -x[1])[:5]:
                print(f"  {st}: {count}")
            print("\n" + "="*40 + "\n")

            return filepath  # Return path for chaining

        else:
            self.logger.warning("No lineage data found.")
            return None


class HybridLineageExtractor:
    """
    Hybrid lineage extractor that intelligently chooses between
    iterative extraction and Admin Scanner API based on workspace count.
    """

    DEFAULT_THRESHOLD = 50  # Workspaces threshold for auto mode

    def __init__(self, mode: str = "auto", threshold: int = None):
        """
        Initialize hybrid extractor.
        
        Args:
            mode: "auto", "iterative", or "scanner"
            threshold: Workspace count threshold for auto mode (default 50)
        """
        self.logger = setup_logging()
        self.mode = mode.lower()
        self.threshold = threshold or self.DEFAULT_THRESHOLD

        load_dotenv()
        self.authenticator = create_authenticator_from_env()
        if not self.authenticator.validate_credentials():
            raise Exception("Authentication failed")
        self.token = self.authenticator.get_fabric_token()

    def extract(self, output_dir: str = "exports/lineage") -> Path:
        """
        Extract lineage using the configured mode.
        
        Args:
            output_dir: Output directory for results
            
        Returns:
            Path to output file
        """
        if self.mode == "iterative":
            self.logger.info("Mode: ITERATIVE (forced)")
            return self._run_iterative(output_dir)

        elif self.mode == "scanner":
            self.logger.info("Mode: SCANNER (forced)")
            return self._run_scanner(output_dir)

        else:  # auto mode
            # Get workspace count to decide mode
            workspace_count = self._count_workspaces()

            if workspace_count >= self.threshold:
                self.logger.info(f"Mode: AUTO → SCANNER ({workspace_count} workspaces >= {self.threshold} threshold)")
                return self._run_scanner(output_dir)
            else:
                self.logger.info(f"Mode: AUTO → ITERATIVE ({workspace_count} workspaces < {self.threshold} threshold)")
                return self._run_iterative(output_dir)

    def _count_workspaces(self) -> int:
        """Count accessible workspaces to inform mode selection."""
        headers = {"Authorization": f"Bearer {self.token}"}
        url = "https://api.fabric.microsoft.com/v1/workspaces"

        try:
            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                count = len(response.json().get("value", []))
                self.logger.info(f"Found {count} accessible workspaces")
                return count
        except Exception as e:
            self.logger.warning(f"Failed to count workspaces: {e}")

        return 0

    def _run_iterative(self, output_dir: str) -> Path:
        """Run existing iterative extraction."""
        extractor = LineageExtractor()
        return extractor.extract_lineage(output_dir)

    def _run_scanner(self, output_dir: str) -> Path:
        """Run Admin Scanner API extraction."""
        try:
            from usf_fabric_monitoring.core.admin_scanner import AdminScannerClient, AdminScannerError
        except ImportError:
            self.logger.error("Admin Scanner module not available. Falling back to iterative.")
            return self._run_iterative(output_dir)

        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        # Get all workspace IDs
        workspace_ids = self._get_all_workspace_ids()

        if not workspace_ids:
            self.logger.warning("No workspaces found for scanning")
            return None

        self.logger.info(f"Scanning {len(workspace_ids)} workspaces via Admin Scanner API...")

        try:
            client = AdminScannerClient(self.token)
            scan_result = client.scan_workspaces(
                workspace_ids=workspace_ids,
                lineage=True,
                datasource_details=True
            )

            # Normalize results to match iterative format
            lineage_data = client.normalize_lineage_results(scan_result)

            if lineage_data:
                output_data = {
                    "extracted_at": datetime.now().isoformat(),
                    "extraction_mode": "scanner",
                    "total_items": len(lineage_data),
                    "lineage": lineage_data
                }

                filename = f"lineage_scanner_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
                filepath = output_path / filename

                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(output_data, f, indent=2, ensure_ascii=False)

                self.logger.info(f"✅ Scanner lineage exported to {filepath}")
                self._print_summary(lineage_data)
                return filepath
            else:
                self.logger.warning("No lineage data returned from scanner")
                return None

        except AdminScannerError as e:
            self.logger.error(f"Admin Scanner failed: {e}")
            self.logger.info("Falling back to iterative extraction...")
            return self._run_iterative(output_dir)
        except Exception as e:
            self.logger.error(f"Unexpected error in scanner mode: {e}")
            self.logger.info("Falling back to iterative extraction...")
            return self._run_iterative(output_dir)

    def _get_all_workspace_ids(self) -> list:
        """Get all accessible workspace IDs."""
        headers = {"Authorization": f"Bearer {self.token}"}
        url = "https://api.fabric.microsoft.com/v1/workspaces"
        workspace_ids = []

        while url:
            try:
                response = requests.get(url, headers=headers)
                if response.status_code != 200:
                    break
                data = response.json()
                workspace_ids.extend([ws["id"] for ws in data.get("value", [])])
                url = data.get("continuationUri")
            except Exception as e:
                self.logger.error(f"Failed to get workspaces: {e}")
                break

        return workspace_ids

    def _print_summary(self, lineage_data: list):
        """Print extraction summary."""
        item_types = {}
        for item in lineage_data:
            it = item.get('Item Type', 'Unknown')
            item_types[it] = item_types.get(it, 0) + 1

        print("\n" + "="*40)
        print("🔗 LINEAGE SUMMARY (Scanner Mode)")
        print("="*40)
        print(f"Total Items Found: {len(lineage_data)}")
        for it, count in sorted(item_types.items(), key=lambda x: -x[1]):
            print(f"  {it}: {count}")
        print("="*40 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description="Extract Lineage for Mirrored Databases and Shortcuts",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Extraction Modes:
  auto       Automatically select mode based on workspace count (default)
  iterative  Force iterative extraction (granular, more API calls)
  scanner    Force Admin Scanner API (batch, requires admin permissions)

Examples:
  python extract_lineage.py --mode auto
  python extract_lineage.py --mode scanner --output-dir exports/scanner_lineage
  python extract_lineage.py --mode iterative --threshold 100
        """
    )
    parser.add_argument("--output-dir", default="exports/lineage", help="Output directory")
    parser.add_argument("--mode", choices=["auto", "iterative", "scanner"], default="auto",
                       help="Extraction mode: auto (default), iterative, or scanner")
    parser.add_argument("--threshold", type=int, default=50,
                       help="Workspace count threshold for auto mode (default: 50)")
    args = parser.parse_args()

    if args.mode == "auto" or args.mode == "scanner":
        # Use hybrid extractor
        extractor = HybridLineageExtractor(mode=args.mode, threshold=args.threshold)
        extractor.extract(args.output_dir)
    else:
        # Direct iterative mode (legacy behavior)
        extractor = LineageExtractor()
        extractor.extract_lineage(args.output_dir)


if __name__ == "__main__":
    main()

