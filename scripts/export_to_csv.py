#!/usr/bin/env python3
"""
Export JSON measurement files to CSV format for Google Sheets import.

Creates two CSV files:
1. Summary CSV - one row per measurement run with aggregate statistics
2. Deployment CSV - one row per deployment with individual metrics
"""

import argparse
import csv
import json
import logging
from pathlib import Path
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def parse_cluster_info(cluster_context: str) -> tuple[str, str]:
    """Parse region and cluster name from cluster context ARN.
    
    Example: arn:aws:eks:us-east-1:906324658258:cluster/prod-live-main
    Returns: ('us-east-1', 'prod-live-main')
    """
    if cluster_context.startswith('arn:aws:eks:'):
        parts = cluster_context.split(':')
        region = parts[3] if len(parts) > 3 else ''
        cluster_name = parts[5].split('/')[-1] if len(parts) > 5 else ''
        return region, cluster_name
    else:
        # Fallback if it's just a cluster name
        return '', cluster_context


def export_summary_csv(json_files: List[Path], output_file: Path):
    """Export summary-level statistics to CSV."""
    
    rows = []
    for json_file in json_files:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Handle both old (cluster_context ARN) and new (cluster name) formats
        cluster_context = data.get('args', {}).get('cluster_context', '')
        if cluster_context:
            region, cluster_name = parse_cluster_info(cluster_context)
        else:
            # New format: cluster is a direct field
            cluster_name = data.get('cluster', '')
            region = ''  # Region not available in new format
        
        # Handle both timestamp formats
        timestamp = data.get('timestamp', '') or data.get('start_time', '')
        
        # Handle both install_time and elapsed_time
        elapsed = data.get('install_time', '') or data.get('elapsed_time', '')
        
        # Get scenario and action if available (new format)
        args = data.get('args', {})
        scenario = args.get('scenario', '')
        action = args.get('action', '')
        
        # Handle namespaces - can be in args or as 'namespace'
        namespaces = args.get('namespaces', [])
        if not namespaces and args.get('namespace'):
            namespaces = [args.get('namespace')]
            
        row = {
            'region': region,
            'cluster': cluster_name,
            'timestamp': timestamp,
            'scenario': scenario,
            'action': action,
            'namespaces': ','.join(namespaces) if namespaces else '',
            'elapsed_time': elapsed,
        }
        
        # Add postprocessed statistics (handle both 'postprocessed' and 'postprocessed_data')
        postprocessed = data.get('postprocessed', {}) or data.get('postprocessed_data', {})
        row.update({
            'scale_direction': postprocessed.get('scale_direction', ''),
            'scale_amount': postprocessed.get('scale_amount', ''),
            'scale_percentage': postprocessed.get('scale_percentage', ''),
            'jain_fairness_index_mean': postprocessed.get('jain_fairness_index_mean', ''),
            'jain_fairness_index_median': postprocessed.get('jain_fairness_index_median', ''),
            'coefficient_of_variation_mean': postprocessed.get('coefficient_of_variation_mean', ''),
            'coefficient_of_variation_median': postprocessed.get('coefficient_of_variation_median', ''),
            'gini_coefficient_mean': postprocessed.get('gini_coefficient_mean', ''),
            'gini_coefficient_median': postprocessed.get('gini_coefficient_median', ''),
            'node_skew_mean': postprocessed.get('node_skew_mean', ''),
            'node_skew_median': postprocessed.get('node_skew_median', ''),
            'node_skew_max': postprocessed.get('node_skew_max', ''),
            'node_skew_percentage_mean': postprocessed.get('node_skew_percentage_mean', ''),
            'node_skew_percentage_median': postprocessed.get('node_skew_percentage_median', ''),
            'node_skew_percentage_max': postprocessed.get('node_skew_percentage_max', ''),
            # Note: the source has a typo 'nosed_used' instead of 'nodes_used'
            'nodes_used_avg': postprocessed.get('nosed_used_avg', ''),
            'nodes_used_median': postprocessed.get('nosed_used_median', ''),
            'nodes_used_max': postprocessed.get('nosed_used_max', ''),
            'nodes_used_min': postprocessed.get('nosed_used_min', ''),
        })
        
        # Add cluster info from first measurement (handle multiple formats)
        measurements = data.get('measurements', []) or data.get('measurements_taken', [])
        cluster_data = {}
        deployment_count = 0
        
        if measurements:
            cluster_data = measurements[0].get('cluster', {})
            deployment_count = len(measurements[0].get('deployments', {}))
        else:
            # Try old format with measurements_pre/post
            measurements_pre = data.get('measurements_pre', {})
            measurements_post = data.get('measurements_post', {})
            # Prefer post measurements for cluster data
            cluster_data = measurements_post.get('cluster', {}) or measurements_pre.get('cluster', {})
            deployment_count = len(measurements_post.get('deployments', {}))
        
        row.update({
            'node_count': cluster_data.get('node_count', ''),
            'eligible_node_count': cluster_data.get('eligible_node_count', ''),
            'deployment_count': deployment_count if deployment_count else '',
        })
        
        rows.append(row)
    
    # Write CSV
    if rows:
        fieldnames = list(rows[0].keys())
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"Wrote {len(rows)} summary rows to {output_file}")
    else:
        logger.warning("No data to write to summary CSV")


def export_deployments_csv(json_files: List[Path], output_file: Path):
    """Export deployment-level statistics to CSV."""
    
    rows = []
    for json_file in json_files:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Handle both timestamp formats
        timestamp = data.get('timestamp', '') or data.get('start_time', '')
        
        # Handle both old (cluster_context ARN) and new (cluster name) formats
        cluster_context = data.get('args', {}).get('cluster_context', '')
        if cluster_context:
            region, cluster_name = parse_cluster_info(cluster_context)
        else:
            cluster_name = data.get('cluster', '')
            region = ''
        
        # Get scenario and action if available (new format)
        args = data.get('args', {})
        scenario = args.get('scenario', '')
        action = args.get('action', '')
        
        # Handle namespaces - can be in args as 'namespaces' (list) or 'namespace' (string)
        namespaces = args.get('namespaces', [])
        if not namespaces and args.get('namespace'):
            namespaces = [args.get('namespace')]
        namespace_str = ','.join(namespaces) if namespaces else ''
        
        # Get measurements (handle multiple formats)
        measurements = data.get('measurements', []) or data.get('measurements_taken', [])
        if not measurements:
            # Try old format with measurements_pre/post
            measurements_post = data.get('measurements_post')
            if measurements_post:
                measurements = [measurements_post]
        
        for measurement in measurements:
            deployments = measurement.get('deployments', {})
            measurement_timestamp = measurement.get('timestamp', timestamp)
            
            for deployment_name, deployment_data in deployments.items():
                # Handle deployment name - can be in 'name' field or as the dict key
                name = deployment_data.get('name', deployment_name)
                row = {
                    'region': region,
                    'cluster': cluster_name,
                    'namespace': namespace_str,
                    'timestamp': timestamp,
                    'measurement_timestamp': measurement_timestamp,
                    'scenario': scenario,
                    'action': action,
                    'deployment_name': name,
                    'total_pods': deployment_data.get('total_pods', ''),
                    'nodes_used': deployment_data.get('nodes_used', ''),
                    'max_pods': deployment_data.get('max_pods', ''),
                    'min_pods': deployment_data.get('min_pods', ''),
                    'node_skew': deployment_data.get('node_skew', ''),
                    'node_skew_percentage': deployment_data.get('node_skew_percentage', ''),
                    'mean_pods': deployment_data.get('mean_pods', ''),
                    'median_pods': deployment_data.get('median_pods', ''),
                    'coefficient_of_variation': deployment_data.get('coefficient_of_variation', ''),
                    'gini_coefficient': deployment_data.get('gini_coefficient', ''),
                    'jain_fairness_index': deployment_data.get('jain_fairness_index', ''),
                }
                rows.append(row)
    
    # Write CSV
    if rows:
        fieldnames = list(rows[0].keys())
        with open(output_file, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
        logger.info(f"Wrote {len(rows)} deployment rows to {output_file}")
    else:
        logger.warning("No data to write to deployments CSV")


def find_json_files(path: Path) -> List[Path]:
    """Find all JSON files in a path (file or directory)."""
    if path.is_file():
        return [path]
    elif path.is_dir():
        return sorted(path.rglob('*.json'))
    else:
        logger.error(f"Path not found: {path}")
        return []


def main():
    parser = argparse.ArgumentParser(
        description="Export JSON measurement files to CSV format for Google Sheets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export a single JSON file
  ./export_to_csv.py output/prod-live-main/prod-live-main-2025-11-18T12:39:35.json
  
  # Export all JSON files in a directory
  ./export_to_csv.py output/prod-live-main/
  
  # Export all JSON files in output directory
  ./export_to_csv.py output/
  
  # Specify custom output location
  ./export_to_csv.py output/C2/ --output-dir exports/
        """
    )
    
    parser.add_argument(
        'input_path',
        type=Path,
        help='Path to JSON file or directory containing JSON files'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        help='Output directory for CSV files (default: same as input directory)'
    )
    parser.add_argument(
        '--summary-only',
        action='store_true',
        help='Export only summary CSV'
    )
    parser.add_argument(
        '--deployments-only',
        action='store_true',
        help='Export only deployments CSV'
    )
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s %(levelname)s: %(message)s'
    )
    
    # Find JSON files
    json_files = find_json_files(args.input_path)
    if not json_files:
        logger.error("No JSON files found")
        return 1
    
    logger.info(f"Found {len(json_files)} JSON file(s)")
    
    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
    elif args.input_path.is_file():
        output_dir = args.input_path.parent
    else:
        output_dir = args.input_path
    
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate base filename from input path
    if args.input_path.is_file():
        base_name = args.input_path.stem
    else:
        base_name = args.input_path.name or 'measurements'
    
    # Export CSVs
    export_both = not args.summary_only and not args.deployments_only
    
    if args.summary_only or export_both:
        summary_file = output_dir / f"{base_name}-summary.csv"
        export_summary_csv(json_files, summary_file)
    
    if args.deployments_only or export_both:
        deployments_file = output_dir / f"{base_name}-deployments.csv"
        export_deployments_csv(json_files, deployments_file)
    
    logger.info("Export complete!")
    return 0


if __name__ == '__main__':
    exit(main())

