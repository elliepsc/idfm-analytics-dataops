"""
Extract ticket validations data from IDFM rail network

Dataset: validations-sur-le-reseau-ferre-nombre-de-validations-par-jour-1er-semestre
(Validations on rail network - number of validations per day)

This script:
1. Connects to IDFM Opendatasoft API
2. Fetches validation records for a date range
3. Saves raw data to JSON files (bronze layer)
"""

import argparse
import logging
import json
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import yaml

from odsv2_client import ODSv2Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


def load_config():
    """Load API configuration from YAML file"""
    config_path = Path(__file__).parent.parent / 'config' / 'apis.yml'
    with open(config_path) as f:
        return yaml.safe_load(f)


def extract_validations(start_date: str, end_date: str, output_dir: str = 'data/bronze/validations'):
    """
    Extract rail network ticket validations between two dates
    
    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_dir: Output directory for JSON files
    """
    # Load configuration
    config = load_config()
    idfm_config = config['idfm']
    dataset_config = idfm_config['datasets']['validations']
    
    # Initialize ODS client
    client = ODSv2Client(
        base_url=idfm_config['base_url'],
        dataset_id=dataset_config['id']
    )
    
    # Build WHERE clause for date filtering
    date_field = dataset_config['filters']['date_field']
    where_clause = f"{date_field} >= '{start_date}' AND {date_field} <= '{end_date}'"
    
    # Build SELECT clause with fields to extract
    fields = dataset_config['fields']
    select_clause = ', '.join(fields.values())
    
    logger.info(f"Extracting validations from {start_date} to {end_date}")
    
    # Fetch all records from API
    records = client.get_all_records(
        where=where_clause,
        select=select_clause,
        order_by=f"{date_field} ASC"
    )
    
    if not records:
        logger.warning("No records found for the specified date range")
        return
    
    # Extract and rename fields according to mapping
    field_mapping = {target: source for target, source in fields.items()}
    extracted = client.extract_fields(records, field_mapping)
    
    # Add metadata to each record
    for record in extracted:
        record['ingestion_ts'] = datetime.utcnow().isoformat()
        record['source'] = 'idfm_validations'
    
    # Save to JSON file
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    filename = f"validations_{start_date}_{end_date}.json"
    filepath = output_path / filename
    
    with open(filepath, 'w') as f:
        json.dump(extracted, f, indent=2, ensure_ascii=False)
    
    logger.info(f"✅ Saved {len(extracted)} records to {filepath}")


def main():
    """CLI entry point"""
    parser = argparse.ArgumentParser(
        description='Extract IDFM rail network ticket validations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python extract_validations.py --start 2024-01-01 --end 2024-01-31
  python extract_validations.py --start 2024-01-01 --end 2024-01-07 --output /tmp/data
        """
    )
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', default='data/bronze/validations', help='Output directory')
    
    args = parser.parse_args()
    
    extract_validations(args.start, args.end, args.output)


if __name__ == '__main__':
    main()
