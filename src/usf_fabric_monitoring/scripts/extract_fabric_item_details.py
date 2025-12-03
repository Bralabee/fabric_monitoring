"""
Fabric Item Details Extraction Script

Extracts detailed information about Fabric items including:
- Job instances for ALL supported item types (Pipelines, Notebooks, Dataflows, etc.)
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

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parents[2]))

from usf_fabric_monitoring.core.auth import create_authenticator_from_env
from usf_fabric_monitoring.core.extractor import FabricDataExtractor
from usf_fabric_monitoring.core.fabric_item_details import FabricItemDetailExtractor
from usf_fabric_monitoring.core.logger import setup_logging
from usf_fabric_monitoring.core.utils import resolve_path

def parse_args(args=None):
    parser = argparse.ArgumentParser(description="Extract Fabric item details")
    parser.add_argument("--workspace", help="Filter by specific workspace ID")
    # Default to resolved path (Lakehouse in Fabric, local otherwise)
    default_output = str(resolve_path("exports/fabric_item_details"))
    parser.add_argument("--output-dir", default=default_output, help="Output directory")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args(args)

def save_json(data: Any, filepath: Path):
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, default=str)

def get_last_processed_time(output_dir: Path) -> datetime:
    """Finds the latest endTimeUtc from existing job JSON files."""
    max_time = None
    if not output_dir.exists():
        return None
    
    # Check only the most recent files to avoid scanning everything
    # Sort files by modification time, newest first
    files = sorted(output_dir.glob("jobs_*.json"), key=os.path.getmtime, reverse=True)
    
    # Scan up to 5 most recent files
    for file_path in files[:5]:
        try:
            with open(file_path, 'r') as f:
                data = json.load(f)
                for job in data:
                    end_time_str = job.get("endTimeUtc")
                    if end_time_str:
                        # Handle ISO format with 'Z'
                        end_time_str = end_time_str.replace('Z', '+00:00')
                        try:
                            dt = datetime.fromisoformat(end_time_str)
                            if max_time is None or dt > max_time:
                                max_time = dt
                        except ValueError:
                            pass
        except Exception:
            continue
            
    return max_time

def run_item_details_extraction(workspace_id: str = None, output_dir: str = "exports/fabric_item_details", log_level: str = "INFO") -> Dict[str, Any]:
    """
    Run the extraction of Fabric item details (jobs and lakehouse tables).
    
    Args:
        workspace_id: Optional workspace ID to filter by.
        output_dir: Directory to save JSON exports.
        log_level: Logging level.
        
    Returns:
        Dictionary containing status and summary of extraction.
    """
    logger = setup_logging(name="fabric_item_details", level=getattr(logging, log_level.upper()))
    
    try:
        authenticator = create_authenticator_from_env()
        extractor = FabricDataExtractor(authenticator)
        detail_extractor = FabricItemDetailExtractor(authenticator)
        
        output_path = Path(output_dir)
        last_processed_time = get_last_processed_time(output_path)
        
        if last_processed_time:
            logger.info(f"Incremental run: Fetching jobs completed after {last_processed_time}")
        else:
            logger.info("Full run: No previous job data found or unable to determine last run time")

        # Get workspaces
        logger.info("Fetching workspaces...")
        workspaces = extractor.get_workspaces(tenant_wide=False, exclude_personal=True)
        
        if workspace_id:
            workspaces = [ws for ws in workspaces if ws.get("id") == workspace_id]
            logger.info(f"Filtered to workspace: {workspace_id}")
            
        logger.info(f"Found {len(workspaces)} workspaces to process")
        
        all_details = {
            "jobs": [],
            "lakehouses": []
        }
        
        # List of item types that support job instances
        SUPPORTED_JOB_ITEM_TYPES = {
            "DataPipeline", "Notebook", "SparkJobDefinition", 
            "Dataflow", "Datamart", "SemanticModel"
        }
        
        for ws in workspaces:
            ws_id = ws.get("id")
            workspace_name = ws.get("displayName")
            logger.info(f"Processing workspace: {workspace_name} ({ws_id})")
            
            items = extractor.get_workspace_items(ws_id)
            
            for item in items:
                item_id = item.get("id")
                item_type = item.get("type")
                item_name = item.get("displayName")
                
                if item_type == "Lakehouse":
                    logger.info(f"  Fetching tables for Lakehouse: {item_name}")
                    tables = detail_extractor.get_lakehouse_tables(ws_id, item_id)
                    for table in tables:
                        table["_workspace_name"] = workspace_name
                        table["_item_name"] = item_name
                        table["_item_type"] = item_type
                        all_details["lakehouses"].append(table)
                elif item_type in SUPPORTED_JOB_ITEM_TYPES:
                    # Try to fetch jobs for supported item types
                    try:
                        jobs = detail_extractor.get_item_job_instances(ws_id, item_id)
                        if jobs:
                            # Filter jobs if incremental run
                            if last_processed_time:
                                new_jobs = []
                                for job in jobs:
                                    end_time_str = job.get("endTimeUtc")
                                    if end_time_str:
                                        try:
                                            dt = datetime.fromisoformat(end_time_str.replace('Z', '+00:00'))
                                            if dt > last_processed_time:
                                                new_jobs.append(job)
                                        except ValueError:
                                            new_jobs.append(job)
                                    else:
                                        new_jobs.append(job)
                                jobs = new_jobs

                            if jobs:
                                logger.info(f"  Found {len(jobs)} new jobs for {item_type}: {item_name}")
                                for job in jobs:
                                    job["_workspace_name"] = workspace_name
                                    job["_item_name"] = item_name
                                    job["_item_type"] = item_type
                                    all_details["jobs"].append(job)
                    except Exception as e:
                        logger.debug(f"  Could not fetch jobs for {item_type} {item_name}: {str(e)}")

        # Save results
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        saved_files = []
        if all_details["jobs"]:
            job_file = output_path / f"jobs_{timestamp}.json"
            save_json(all_details["jobs"], job_file)
            saved_files.append(str(job_file))
            logger.info(f"Saved {len(all_details['jobs'])} jobs (all types)")
            
        if all_details["lakehouses"]:
            lake_file = output_path / f"lakehouses_{timestamp}.json"
            save_json(all_details["lakehouses"], lake_file)
            saved_files.append(str(lake_file))
            logger.info(f"Saved {len(all_details['lakehouses'])} lakehouse tables")
            
        logger.info("Extraction complete")
        
        return {
            "status": "success",
            "jobs_count": len(all_details["jobs"]),
            "lakehouses_count": len(all_details["lakehouses"]),
            "files": saved_files
        }
        
    except Exception as e:
        logger.error(f"Extraction failed: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        }

def main(argv=None):
    load_dotenv()
    args = parse_args(argv)
    
    result = run_item_details_extraction(
        workspace_id=args.workspace,
        output_dir=args.output_dir,
        log_level=args.log_level
    )
    
    if result["status"] == "success":
        return 0
    else:
        return 1

if __name__ == "__main__":
    sys.exit(main())
