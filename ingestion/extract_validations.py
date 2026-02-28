"""
Extract ticket validations data from IDFM rail network.

Dataset: validations-reseau-ferre-nombre-validations-par-jour-1er-trimestre
API structure confirmed 2026-02-27: fields at ROOT level of each record.
Example: {"jour": "2025-03-12", "code_stif_arret": "401", "nb_vald": 12, ...}

Usage:
    python ingestion/extract_validations.py --start 2025-01-01 --end 2025-01-31
    python ingestion/extract_validations.py --start 2025-01-01 --end 2025-01-01 --output /tmp/data
"""

import argparse
import logging
import json
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import os
import yaml

from odsv2_client import ODSv2Client

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()

# FIX V2: resolve project root from env var first, fall back to script location.
# - PROJECT_ROOT in .env → used in Airflow, CI/CD, or any non-standard layout
# - Path(__file__).parent.parent → automatic fallback for local dev (no config needed)
# This pattern ensures output always lands in data/bronze/validations/ at project root,
# regardless of which directory you call the script from.
PROJECT_ROOT = Path(os.getenv('PROJECT_ROOT', Path(__file__).parent.parent))


def load_config():
    """Load API configuration from YAML file."""
    config_path = PROJECT_ROOT / 'config' / 'apis.yml'
    with open(config_path) as f:
        return yaml.safe_load(f)


def extract_validations(start_date: str, end_date: str, output_dir: Path = None):
    """
    Extract rail network ticket validations between two dates.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
        output_dir: Output directory (default: PROJECT_ROOT/data/bronze/validations)
    """
    config = load_config()
    idfm_config = config['idfm']
    # FIXED V2: key is 'validations_rail' (not 'validations')
    dataset_config = idfm_config['datasets']['validations_rail']

    client = ODSv2Client(
        base_url=idfm_config['base_url'],
        dataset_id=dataset_config['id']
    )

    date_field = dataset_config['filters']['date_field']
    where_clause = f"{date_field} >= '{start_date}' AND {date_field} <= '{end_date}'"

    fields = dataset_config['fields']
    select_clause = ', '.join(fields.values())

    logger.info(f"Extracting validations from {start_date} to {end_date}")
    logger.info(f"Dataset: {dataset_config['id']}")

    records = client.get_all_records(
        where=where_clause,
        select=select_clause,
        order_by=f"{date_field} ASC"
    )

    if not records:
        logger.warning("No records found for the specified date range")
        return

    # FIXED V2: fields at ROOT level — do not use client.extract_fields()
    # which expects nested record['record']['fields'] (old ODS v1 structure).
    extracted = []
    for record in records:
        extracted_record = {target: record.get(source) for target, source in fields.items()}
        # FIXED V2: datetime.utcnow() deprecated in Python 3.12+
        extracted_record['ingestion_ts'] = datetime.now(timezone.utc).isoformat()
        extracted_record['source'] = 'idfm_validations_rail'
        extracted.append(extracted_record)

    # FIX V2: default output path anchored to PROJECT_ROOT, not working directory
    output_path = Path(output_dir) if output_dir else PROJECT_ROOT / 'data/bronze/validations'
    output_path.mkdir(parents=True, exist_ok=True)

    filename = f"validations_{start_date}_{end_date}.json"
    filepath = output_path / filename

    with open(filepath, 'w') as f:
        json.dump(extracted, f, indent=2, ensure_ascii=False)

    logger.info(f"✅ Saved {len(extracted)} records to {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description='Extract IDFM rail network ticket validations',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python extract_validations.py --start 2025-01-01 --end 2025-01-31
  python extract_validations.py --start 2025-01-01 --end 2025-01-01 --output /tmp/data
        """
    )
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', default=None, help='Output directory (default: PROJECT_ROOT/data/bronze/validations)')

    args = parser.parse_args()
    extract_validations(args.start, args.end, args.output)


if __name__ == '__main__':
    main()
