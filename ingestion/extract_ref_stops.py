"""
Extract reference data: stops (arrets-lignes).

This dataset is denormalized: 1 stop × N lines = N rows.
Deduplication to unique stops happens in dbt (stg_ref_stops → dim_stop).

Usage:
    python ingestion/extract_ref_stops.py
    python ingestion/extract_ref_stops.py --output /tmp/data
"""

import argparse
import logging
import json
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import os
import yaml

# FIX V2: removed parasitic imports from V1 that shadowed local variables:
# - 'from http import client' overwrote the ODSv2Client instance named 'client'
# - 'from attrs import fields' overwrote the 'fields' variable from config
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
PROJECT_ROOT = Path(os.getenv('PROJECT_ROOT', Path(__file__).parent.parent))


def load_config():
    """Load API configuration from YAML file."""
    # FIX V2: use PROJECT_ROOT instead of Path(__file__).parent.parent
    config_path = PROJECT_ROOT / 'config' / 'apis.yml'
    with open(config_path) as f:
        return yaml.safe_load(f)


def extract_ref_stops(output_dir: Path = None):
    """Extract full stops referential from IDFM API."""

    config = load_config()
    idfm_config = config['idfm']
    dataset_config = idfm_config['datasets']['ref_stops']

    client = ODSv2Client(
        base_url=idfm_config['base_url'],
        dataset_id=dataset_config['id']
    )

    fields = dataset_config['fields']
    select_clause = ', '.join(fields.values())

    logger.info("Extracting reference data: stops")

    records = client.get_all_records(select=select_clause)

    if not records:
        logger.warning("No records found")
        return

    # FIX V2: fields at ROOT level — do not use client.extract_fields()
    # which expects nested record['record']['fields'] (old ODS v1 structure).
    # FIX V2: removed duplicated ingestion_ts loop that existed in V1.
    extracted = []
    for record in records:
        extracted_record = {target: record.get(source) for target, source in fields.items()}
        # FIX V2: datetime.utcnow() deprecated in Python 3.12+
        extracted_record['ingestion_ts'] = datetime.now(timezone.utc).isoformat()
        extracted_record['source'] = 'idfm_ref_stops'
        extracted.append(extracted_record)

    # FIX V2: default output path anchored to PROJECT_ROOT, not working directory
    output_path = Path(output_dir) if output_dir else PROJECT_ROOT / 'data/bronze/referentials'
    output_path.mkdir(parents=True, exist_ok=True)

    filename = f"ref_stops_{datetime.now().strftime('%Y%m%d')}.json"
    filepath = output_path / filename

    with open(filepath, 'w') as f:
        json.dump(extracted, f, indent=2, ensure_ascii=False)

    logger.info(f"✅ Saved {len(extracted)} stops to {filepath}")


def main():
    parser = argparse.ArgumentParser(description='Extract IDFM stops referential')
    parser.add_argument('--output', default=None,
                        help='Output directory (default: PROJECT_ROOT/data/bronze/referentials)')
    args = parser.parse_args()
    extract_ref_stops(args.output)


if __name__ == '__main__':
    main()
