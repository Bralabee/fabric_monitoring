#!/usr/bin/env python3
"""
Fabric Workspace Inventory and Monitoring Report

⚠️ Note: This script uses member-only workspace access (not tenant-wide).
    It returns only workspaces where the service principal is a member (~139 workspaces).
    For tenant-wide monitoring, use monitor_hub_pipeline.py instead.

This script generates comprehensive monitoring data from available Fabric APIs
when admin permissions for activity logs are not available.

Usage:
    python fabric_workspace_report.py [--output-format csv|excel|json]
    
Examples:
    # Generate CSV report (member workspaces only)
    python fabric_workspace_report.py
    
    # Generate Excel report with charts
    python fabric_workspace_report.py --output-format excel
    
    # Generate JSON for API consumption
    python fabric_workspace_report.py --output-format json
    
For tenant-wide monitoring:
    python monitor_hub_pipeline.py --days 7
"""

import os
import sys
import argparse
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
import json
import requests
import pandas as pd

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parents[2]))

from dotenv import load_dotenv
from usf_fabric_monitoring.core.auth import create_authenticator_from_env


class FabricWorkspaceMonitor:
    """
    Monitor Fabric workspaces and items using available APIs
    """
    
    def __init__(self, authenticator):
        self.auth = authenticator
        self.logger = logging.getLogger(__name__)
        
    def get_workspace_inventory(self) -> List[Dict[str, Any]]:
        """
        Get comprehensive workspace inventory
        """
        self.logger.info("Fetching workspace inventory...")
        
        token = self.auth.get_fabric_token()
        headers = {'Authorization': f'Bearer {token}'}
        
        # Get all workspaces
        response = requests.get(
            'https://api.fabric.microsoft.com/v1/workspaces',
            headers=headers,
            timeout=30
        )
        response.raise_for_status()
        
        workspaces = response.json()['value']
        inventory = []
        
        for ws in workspaces:
            ws_data = {
                'workspace_id': ws['id'],
                'workspace_name': ws['displayName'],
                'workspace_type': ws.get('type', 'Unknown'),
                'capacity_id': ws.get('capacityId'),
                'scan_timestamp': datetime.utcnow().isoformat(),
                'items': []
            }
            
            # Get items in workspace
            try:
                items_response = requests.get(
                    f'https://api.fabric.microsoft.com/v1/workspaces/{ws["id"]}/items',
                    headers=headers,
                    timeout=15
                )
                
                if items_response.status_code == 200:
                    items = items_response.json()['value']
                    
                    for item in items:
                        item_data = {
                            'item_id': item['id'],
                            'item_name': item['displayName'],
                            'item_type': item['type'],
                            'created_date': item.get('createdDate'),
                            'modified_date': item.get('lastModifiedDate'),
                            'description': item.get('description'),
                            'job_instances_available': False,
                            'recent_job_count': 0
                        }
                        
                        # Check for job instances if applicable
                        if item['type'] in ['Lakehouse', 'Warehouse', 'SemanticModel', 'Notebook']:
                            try:
                                jobs_response = requests.get(
                                    f'https://api.fabric.microsoft.com/v1/workspaces/{ws["id"]}/items/{item["id"]}/jobs/instances',
                                    headers=headers,
                                    timeout=5
                                )
                                
                                if jobs_response.status_code == 200:
                                    item_data['job_instances_available'] = True
                                    jobs = jobs_response.json().get('value', [])
                                    item_data['recent_job_count'] = len(jobs)
                                    
                                    # Add job details if any exist
                                    if jobs:
                                        latest_job = jobs[0]  # Jobs are typically sorted by date
                                        item_data['latest_job_status'] = latest_job.get('status')
                                        item_data['latest_job_start'] = latest_job.get('startTimeUtc')
                                        item_data['latest_job_end'] = latest_job.get('endTimeUtc')
                                        item_data['latest_job_type'] = latest_job.get('jobType')
                            
                            except Exception as e:
                                self.logger.debug(f"Could not fetch jobs for {item['displayName']}: {e}")
                        
                        ws_data['items'].append(item_data)
                
                else:
                    self.logger.warning(f"Could not fetch items for workspace {ws['displayName']}")
            
            except Exception as e:
                self.logger.error(f"Error processing workspace {ws['displayName']}: {e}")
            
            # Add workspace-level statistics
            ws_data['total_items'] = len(ws_data['items'])
            ws_data['items_by_type'] = {}
            ws_data['items_with_jobs'] = 0
            ws_data['total_recent_jobs'] = 0
            
            for item in ws_data['items']:
                item_type = item['item_type']
                ws_data['items_by_type'][item_type] = ws_data['items_by_type'].get(item_type, 0) + 1
                
                if item['job_instances_available']:
                    ws_data['items_with_jobs'] += 1
                    ws_data['total_recent_jobs'] += item['recent_job_count']
            
            inventory.append(ws_data)
            
        self.logger.info(f"Completed inventory for {len(inventory)} workspaces")
        return inventory
    
    def get_capacity_information(self) -> Dict[str, Any]:
        """
        Get capacity information where available
        """
        self.logger.info("Fetching capacity information...")
        
        # This would require additional API calls to capacity management endpoints
        # For now, we'll return basic capacity info from workspaces
        capacity_info = {
            'scan_timestamp': datetime.utcnow().isoformat(),
            'capacities_found': [],
            'workspace_capacity_mapping': {}
        }
        
        return capacity_info
    
    def generate_monitoring_summary(self, inventory: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Generate summary statistics for monitoring dashboard
        """
        summary = {
            'report_timestamp': datetime.utcnow().isoformat(),
            'total_workspaces': len(inventory),
            'total_items': sum(ws['total_items'] for ws in inventory),
            'workspaces_with_activity': sum(1 for ws in inventory if ws['total_recent_jobs'] > 0),
            'total_recent_jobs': sum(ws['total_recent_jobs'] for ws in inventory),
            'items_by_type': {},
            'items_with_monitoring': 0,
            'workspace_health': [],
            'recommendations': []
        }
        
        # Aggregate item types across all workspaces
        for ws in inventory:
            for item_type, count in ws['items_by_type'].items():
                summary['items_by_type'][item_type] = summary['items_by_type'].get(item_type, 0) + count
            
            summary['items_with_monitoring'] += ws['items_with_jobs']
            
            # Workspace health assessment
            health_score = 0
            health_issues = []
            
            if ws['total_items'] == 0:
                health_issues.append("No items in workspace")
            elif ws['items_with_jobs'] == 0:
                health_issues.append("No monitorable items found")
                health_score += 1
            else:
                health_score += 2
                
            if ws['total_recent_jobs'] > 0:
                health_score += 1
                
            workspace_health = {
                'workspace_name': ws['workspace_name'],
                'workspace_id': ws['workspace_id'],
                'health_score': health_score,
                'max_score': 3,
                'health_percentage': (health_score / 3) * 100,
                'issues': health_issues,
                'total_items': ws['total_items'],
                'monitorable_items': ws['items_with_jobs']
            }
            
            summary['workspace_health'].append(workspace_health)
        
        # Generate recommendations
        if summary['total_recent_jobs'] == 0:
            summary['recommendations'].append({
                'type': 'activity',
                'priority': 'high',
                'message': 'No recent job activity detected across all workspaces. Consider running some data operations to test monitoring.',
                'action': 'Execute notebooks, refresh datasets, or run data pipelines to generate monitorable activity.'
            })
        
        if summary['items_with_monitoring'] < summary['total_items'] * 0.5:
            summary['recommendations'].append({
                'type': 'coverage',
                'priority': 'medium',
                'message': f'Only {summary["items_with_monitoring"]} out of {summary["total_items"]} items support job monitoring.',
                'action': 'Focus monitoring on Lakehouses, Warehouses, and Semantic Models which provide job instance data.'
            })
        
        low_health_workspaces = [ws for ws in summary['workspace_health'] if ws['health_percentage'] < 50]
        if low_health_workspaces:
            summary['recommendations'].append({
                'type': 'health',
                'priority': 'medium',
                'message': f'{len(low_health_workspaces)} workspaces have low health scores.',
                'action': 'Review workspaces with no items or no monitorable activity.',
                'affected_workspaces': [ws['workspace_name'] for ws in low_health_workspaces]
            })
        
        return summary


def export_to_csv(inventory: List[Dict], summary: Dict, output_dir: Path):
    """Export data to CSV files"""
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Flatten inventory data for CSV
    rows = []
    for ws in inventory:
        for item in ws['items']:
            row = {
                'workspace_id': ws['workspace_id'],
                'workspace_name': ws['workspace_name'],
                'workspace_type': ws['workspace_type'],
                'capacity_id': ws['capacity_id'],
                'item_id': item['item_id'],
                'item_name': item['item_name'],
                'item_type': item['item_type'],
                'created_date': item['created_date'],
                'modified_date': item['modified_date'],
                'job_instances_available': item['job_instances_available'],
                'recent_job_count': item['recent_job_count'],
                'latest_job_status': item.get('latest_job_status'),
                'latest_job_start': item.get('latest_job_start'),
                'scan_timestamp': ws['scan_timestamp']
            }
            rows.append(row)
    
    # Export inventory
    if rows:
        df_inventory = pd.DataFrame(rows)
        inventory_file = output_dir / f"fabric_inventory_{timestamp}.csv"
        df_inventory.to_csv(inventory_file, index=False)
        print(f"📄 Inventory exported to: {inventory_file}")
    
    # Export workspace summary
    ws_summary_rows = []
    for ws in inventory:
        row = {
            'workspace_name': ws['workspace_name'],
            'workspace_id': ws['workspace_id'],
            'workspace_type': ws['workspace_type'],
            'capacity_id': ws['capacity_id'],
            'total_items': ws['total_items'],
            'items_with_jobs': ws['items_with_jobs'],
            'total_recent_jobs': ws['total_recent_jobs'],
            'scan_timestamp': ws['scan_timestamp']
        }
        # Add item type counts
        for item_type, count in ws['items_by_type'].items():
            row[f'{item_type.lower()}_count'] = count
        
        ws_summary_rows.append(row)
    
    if ws_summary_rows:
        df_summary = pd.DataFrame(ws_summary_rows)
        summary_file = output_dir / f"workspace_summary_{timestamp}.csv"
        df_summary.to_csv(summary_file, index=False)
        print(f"📊 Workspace summary exported to: {summary_file}")
    
    # Export health report
    health_rows = summary['workspace_health']
    if health_rows:
        df_health = pd.DataFrame(health_rows)
        health_file = output_dir / f"workspace_health_{timestamp}.csv"
        df_health.to_csv(health_file, index=False)
        print(f"🏥 Health report exported to: {health_file}")
    
    return inventory_file if rows else None


def export_to_json(inventory: List[Dict], summary: Dict, output_dir: Path):
    """Export data to JSON files"""
    output_dir.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Complete report
    complete_report = {
        'inventory': inventory,
        'summary': summary,
        'metadata': {
            'export_timestamp': datetime.utcnow().isoformat(),
            'format_version': '1.0',
            'source': 'fabric-workspace-monitor'
        }
    }
    
    report_file = output_dir / f"fabric_monitoring_report_{timestamp}.json"
    with open(report_file, 'w') as f:
        json.dump(complete_report, f, indent=2, default=str)
    
    print(f"📋 Complete report exported to: {report_file}")
    return report_file


def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description="Generate Fabric workspace monitoring report")
    parser.add_argument(
        '--output-format',
        choices=['csv', 'json', 'both'],
        default='csv',
        help='Output format for the report (default: csv)'
    )
    parser.add_argument(
        '--output-dir',
        type=str,
        default='./exports/workspace_reports',
        help='Output directory for report files'
    )
    parser.add_argument(
        '--log-level',
        choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
        default='INFO',
        help='Logging level'
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # Load environment
    load_dotenv()
    
    logger.info("Starting Fabric workspace monitoring report generation")
    
    try:
        # Initialize authenticator and monitor
        authenticator = create_authenticator_from_env()
        monitor = FabricWorkspaceMonitor(authenticator)
        
        # Generate inventory and summary
        print("🔍 Scanning Fabric workspaces...")
        inventory = monitor.get_workspace_inventory()
        
        print("📊 Generating monitoring summary...")
        summary = monitor.generate_monitoring_summary(inventory)
        
        # Create output directory
        output_dir = Path(args.output_dir)
        
        # Export based on format
        if args.output_format in ['csv', 'both']:
            print("📤 Exporting to CSV format...")
            export_to_csv(inventory, summary, output_dir)
        
        if args.output_format in ['json', 'both']:
            print("📤 Exporting to JSON format...")
            export_to_json(inventory, summary, output_dir)
        
        # Print summary to console
        print(f"\n📋 Monitoring Report Summary:")
        print(f"  • Total Workspaces: {summary['total_workspaces']}")
        print(f"  • Total Items: {summary['total_items']}")
        print(f"  • Items with Monitoring: {summary['items_with_monitoring']}")
        print(f"  • Recent Job Activity: {summary['total_recent_jobs']} jobs")
        print(f"  • Workspaces with Activity: {summary['workspaces_with_activity']}")
        
        if summary['items_by_type']:
            print(f"\n📊 Items by Type:")
            for item_type, count in summary['items_by_type'].items():
                print(f"    {item_type}: {count}")
        
        if summary['recommendations']:
            print(f"\n💡 Recommendations:")
            for rec in summary['recommendations']:
                print(f"    [{rec['priority'].upper()}] {rec['message']}")
        
        print(f"\n🎉 Report generation completed successfully!")
        
    except Exception as e:
        logger.error(f"Report generation failed: {e}", exc_info=True)
        print(f"❌ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()