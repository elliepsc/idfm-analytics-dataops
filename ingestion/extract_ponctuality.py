import argparse
import logging
import json
from datetime import datetime
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
    """Charge la configuration des APIs"""
    config_path = Path(__file__).parent.parent / 'config' / 'apis.yml'
    with open(config_path) as f:
        return yaml.safe_load(f)


def extract_punctuality(start_date: str, end_date: str, output_dir: str = 'data/bronze/punctuality'):
    """
    Extrait la ponctualité Transilien entre deux dates

    Args:
        start_date: Date de début (YYYY-MM-DD)
        end_date: Date de fin (YYYY-MM-DD)
        output_dir: Répertoire de sortie
    """
    config = load_config()
    transilien_config = config['transilien']
    dataset_config = transilien_config['datasets']['punctuality']

    # Client ODS
    client = ODSv2Client(
        base_url=transilien_config['base_url'],
        dataset_id=dataset_config['id']
    )

    # Filtres
    date_field = dataset_config['filters']['date_field']
    where_clause = f"{date_field} >= '{start_date}' AND {date_field} <= '{end_date}'"

    # Champs à extraire
    fields = dataset_config['fields']
    select_clause = ', '.join(fields.values())

    logger.info(f"Extracting punctuality from {start_date} to {end_date}")

    # Récupération
    records = client.get_all_records(
        where=where_clause,
        select=select_clause,
        order_by=f"{date_field} ASC"
    )

    if not records:
        logger.warning("No records found")
        return

    # Mapping des champs
    field_mapping = {target: source for target, source in fields.items()}
    extracted = client.extract_fields(records, field_mapping)

    # Ajout metadata
    for record in extracted:
        record['ingestion_ts'] = datetime.utcnow().isoformat()
        record['source'] = 'transilien_punctuality'

    # Sauvegarde
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = f"punctuality_{start_date}_{end_date}.json"
    filepath = output_path / filename

    with open(filepath, 'w') as f:
        json.dump(extracted, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved {len(extracted)} records to {filepath}")


def main():
    parser = argparse.ArgumentParser(description='Extract Transilien punctuality data')
    parser.add_argument('--start', required=True, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end', required=True, help='End date (YYYY-MM-DD)')
    parser.add_argument('--output', default='data/bronze/punctuality', help='Output directory')

    args = parser.parse_args()

    extract_punctuality(args.start, args.end, args.output)


if __name__ == '__main__':
    main()
