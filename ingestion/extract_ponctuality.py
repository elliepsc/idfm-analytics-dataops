"""
Extract monthly punctuality data from Transilien (SNCF)

Dataset: ponctualite-mensuelle-transilien
API structure confirmed 2026-02-27:
  Fields at ROOT level. Format date = YYYY-MM (not YYYY-MM-DD).
  Example: {"date": "2013-01", "ligne": "A", "taux_de_ponctualite": 83.6, ...}

⚠️  Fields that do NOT exist in this dataset (wrong assumptions in V1):
    - taux_regularite       → correct field is taux_de_ponctualite
    - nombre_de_trains_prevu
    - nombre_de_trains_partis
"""

import argparse
import logging
import json
from datetime import datetime, timezone
from pathlib import Path
from dotenv import load_dotenv
import yaml

from odsv2_client import ODSv2Client

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


def extract_punctuality(start_date: str, end_date: str, output_dir: str = 'data/bronze/punctuality'):
    """
    Extract Transilien monthly punctuality data between two dates.

    Args:
        start_date: Start date (YYYY-MM-DD or YYYY-MM)
        end_date: End date (YYYY-MM-DD or YYYY-MM)
        output_dir: Output directory for JSON files
    """
    config = load_config()
    transilien_config = config['transilien']
    dataset_config = transilien_config['datasets']['punctuality']

    client = ODSv2Client(
        base_url=transilien_config['base_url'],
        dataset_id=dataset_config['id']
    )

    # Date field is YYYY-MM format — filter with >= and <= on string works correctly
    date_field = dataset_config['filters']['date_field']
    # Truncate to YYYY-MM if full date passed
    start_month = start_date[:7]
    end_month = end_date[:7]
    where_clause = f"{date_field} >= '{start_month}' AND {date_field} <= '{end_month}'"

    fields = dataset_config['fields']
    select_clause = ', '.join(fields.values())

    logger.info(f"Extracting punctuality from {start_month} to {end_month}")
    logger.info(f"Dataset: {dataset_config['id']}")

    records = client.get_all_records(
        where=where_clause,
        select=select_clause,
        order_by=f"{date_field} ASC"
    )

    if not records:
        logger.warning("No records found for the specified date range")
        return

    # FIXED V2: fields at ROOT level, not nested
    extracted = []
    for record in records:
        extracted_record = {}
        for target_field, source_field in fields.items():
            extracted_record[target_field] = record.get(source_field)

        # FIXED V2: datetime.utcnow() deprecated in Python 3.12+
        extracted_record['ingestion_ts'] = datetime.now(timezone.utc).isoformat()
        extracted_record['source'] = 'transilien_punctuality'
        extracted.append(extracted_record)

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = f"punctuality_{start_month}_{end_month}.json"
    filepath = output_path / filename

    with open(filepath, 'w') as f:
        json.dump(extracted, f, indent=2, ensure_ascii=False)

    logger.info(f"✅ Saved {len(extracted)} records to {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description='Extract Transilien monthly punctuality data',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python extract_ponctuality.py --start 2024-01 --end 2024-12
  python extract_ponctuality.py --start 2024-01-01 --end 2024-12-31
        """
    )
    parser.add_argument('--start', required=True, help='Start month (YYYY-MM or YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End month (YYYY-MM or YYYY-MM-DD)')
    parser.add_argument('--output', default='data/bronze/punctuality', help='Output directory')

    args = parser.parse_args()
    extract_punctuality(args.start, args.end, args.output)


if __name__ == '__main__':
    main()
