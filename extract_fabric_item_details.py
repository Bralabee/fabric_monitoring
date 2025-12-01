"""
Fabric Item Details Extraction Script

Extracts detailed information about Fabric items including:
- Pipeline runs (start/end time, status, errors)
- Notebook runs (execution status)
- Lakehouse tables (maintenance status)

Usage:
    python extract_fabric_item_details.py
    python extract_fabric_item_details.py --workspace <workspace_id>
"""

import argparse
import json
import logging
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any

from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, str(Path(__file__).parent / "src"))

from usf_fabric_monitoring.core.auth import create_authenticator_from_env
from usf_fabric_monitoring.core.extractor import FabricDataExtractor
from usf_fabric_monitoring.core.fabric_item_details import FabricItemDetailExtractor
from usf_fabric_monitoring.core.logger import setup_logging

def parse_args():
    parser = argparse.ArgumentParser(description="Extract Fabric item details")
    parser.add_argument("--workspace", help="Filter by specific workspace ID")
    parser.add_argument("--output-dir", default="exports/fabric_item_details", help="Output directory")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args()

def save_json(data: Any, filepath: Path):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)

def main():
    load_dotenv()
    args = parse_args()
    
    logger = setup_logging(name="fabric_item_details", level=getattr(logging, args.log_level.upper()))
    
    try:
        authenticator = create_authenticator_from_env()
        extractor = FabricDataExtractor(authenticator)
        detail_extractor = FabricItemDetailExtractor(authenticator)
        
        # Get workspaces
        logger.info("Fetching workspaces...")
        workspaces = extractor.get_workspaces(tenant_wide=False, exclude_personal=True)
        
        if args.workspace:
            workspaces = [ws for ws in workspaces if ws.get("id") == args.workspace]
            logger.info(f"Filtered to workspace: {args.workspace}")
            
        logger.info(f"Found {len(workspaces)} workspaces to process")
        
        all_details = {
            "pipelines": [],
            "notebooks": [],
            "lakehouses": []
        }
        
        for ws in workspaces:
            workspace_id = ws.get("id")
            workspace_name = ws.get("displayName")
            logger.info(f"Processing workspace: {workspace_name} ({workspace_id})")
            
            items = extractor.get_workspace_items(workspace_id)
            
            for item in items:
                item_id = item.get("id")
                item_type = item.get("type")
                item_name = item.get("displayName")
                
                if item_type == "DataPipeline":
                    logger.info(f"  Fetching jobs for Pipeline: {item_name}")
                    jobs = detail_extractor.get_item_job_instances(workspace_id, item_id)
                    for job in jobs:
                        job["_workspace_name"] = workspace_name
                        job["_item_name"] = item_name
                        job["_item_type"] = item_type
                        all_details["pipelines"].append(job)
                        
                elif item_type == "Notebook":
                    logger.info(f"  Fetching jobs for Notebook: {item_name}")
                    jobs = detail_extractor.get_item_job_instances(workspace_id, item_id)
                    for job in jobs:
                        job["_workspace_name"] = workspace_name
                        job["_item_name"] = item_name
                        job["_item_type"] = item_type
                        all_details["notebooks"].append(job)
                        
                elif item_type == "Lakehouse":
                    logger.info(f"  Fetching tables for Lakehouse: {item_name}")
                    tables = detail_extractor.get_lakehouse_tables(workspace_id, item_id)
                    for table in tables:
                        table["_workspace_name"] = workspace_name
                        table["_item_name"] = item_name
                        table["_item_type"] = item_type
                        all_details["lakehouses"].append(table)

        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_dir = Path(args.output_dir)
        
        if all_details["pipelines"]:
            save_json(all_details["pipelines"], output_dir / f"pipelines_{timestamp}.json")
            logger.info(f"Saved {len(all_details['pipelines'])} pipeline jobs")
            
        if all_details["notebooks"]:
            save_json(all_details["notebooks"], output_dir / f"notebooks_{timestamp}.json")
            logger.info(f"Saved {len(all_details['notebooks'])} notebook jobs")
            
        if all_details["lakehouses"]:
            save_json(all_details["lakehouses"], output_dir / f"lakehouses_{timestamp}.json")
            logger.info(f"Saved {len(all_details['lakehouses'])} lakehouse tables")
            
        logger.info("Extraction complete")
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        return 1
        
    return 0

if __name__ == "__main__":
    sys.exit(main())
