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
    config_path = Path(__file__).parent.parent / 'config' / 'apis.yml'
    with open(config_path) as f:
        return yaml.safe_load(f)


def extract_ref_stops(output_dir: str = 'data/bronze/referentials'):
    """Extrait le référentiel complet des arrêts"""

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

    field_mapping = {target: source for target, source in fields.items()}
    extracted = client.extract_fields(records, field_mapping)

    for record in extracted:
        record['ingestion_ts'] = datetime.utcnow().isoformat()
        record['source'] = 'idfm_ref_stops'

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = f"ref_stops_{datetime.now().strftime('%Y%m%d')}.json"
    filepath = output_path / filename

    with open(filepath, 'w') as f:
        json.dump(extracted, f, indent=2, ensure_ascii=False)

    logger.info(f"Saved {len(extracted)} stops to {filepath}")


if __name__ == '__main__':
    extract_ref_stops()
