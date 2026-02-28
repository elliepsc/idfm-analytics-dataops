"""
Extract reference data: lines (referentiel-des-lignes).

All transport lines for Ile-de-France: RER, Metro, Transilien, Bus, Tram.
This is a full snapshot — always overwrites previous extraction.

Usage:
    python ingestion/extract_ref_lines.py
    python ingestion/extract_ref_lines.py --output /tmp/data
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

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
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


def extract_ref_lines(output_dir: Path = None):
    """Extract full lines referential from IDFM API."""

    config = load_config()
    idfm_config = config['idfm']
    dataset_config = idfm_config['datasets']['ref_lines']

    client = ODSv2Client(
        base_url=idfm_config['base_url'],
        dataset_id=dataset_config['id']
    )

    fields = dataset_config['fields']
    select_clause = ', '.join(fields.values())

    logger.info("Extracting reference data: lines")

    records = client.get_all_records(select=select_clause)

    if not records:
        logger.warning("No records found")
        return

    # FIX V2: fields at ROOT level — do not use client.extract_fields()
    # which expects nested record['record']['fields'] (old ODS v1 structure).
    extracted = []
    for record in records:
        extracted_record = {target: record.get(source) for target, source in fields.items()}
        # FIX V2: datetime.utcnow() deprecated in Python 3.12+
        extracted_record['ingestion_ts'] = datetime.now(timezone.utc).isoformat()
        extracted_record['source'] = 'idfm_ref_lines'
        extracted.append(extracted_record)

    # FIX V2: default output path anchored to PROJECT_ROOT, not working directory
    output_path = Path(output_dir) if output_dir else PROJECT_ROOT / 'data/bronze/referentials'
    output_path.mkdir(parents=True, exist_ok=True)

    filename = f"ref_lines_{datetime.now().strftime('%Y%m%d')}.json"
    filepath = output_path / filename

    with open(filepath, 'w') as f:
        json.dump(extracted, f, indent=2, ensure_ascii=False)

    logger.info(f"✅ Saved {len(extracted)} lines to {filepath}")


def main():
    parser = argparse.ArgumentParser(description='Extract IDFM lines referential')
    parser.add_argument('--output', default=None,
                        help='Output directory (default: PROJECT_ROOT/data/bronze/referentials)')
    args = parser.parse_args()
    extract_ref_lines(args.output)


if __name__ == '__main__':
    main()
