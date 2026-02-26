# Création des datasets et tables BigQuery
import logging
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import os
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


def setup_bigquery():
    """Crée les datasets et tables BigQuery nécessaires"""

    project_id = os.getenv('GCP_PROJECT_ID')
    region = os.getenv('GCP_REGION', 'europe-west1')

    client = bigquery.Client(project=project_id)

    # Datasets à créer
    datasets = [
        os.getenv('BQ_DATASET_RAW', 'transport_raw'),
        os.getenv('BQ_DATASET_STAGING', 'transport_staging'),
        os.getenv('BQ_DATASET_CORE', 'transport_core'),
        os.getenv('BQ_DATASET_ANALYTICS', 'transport_analytics')
    ]

    for dataset_id in datasets:
        dataset_ref = f"{project_id}.{dataset_id}"

        try:
            client.get_dataset(dataset_ref)
            logger.info(f"Dataset {dataset_ref} already exists")
        except NotFound:
            # Créer le dataset
            dataset = bigquery.Dataset(dataset_ref)
            dataset.location = region
            dataset.description = f"Transport scorecard - {dataset_id}"

            dataset = client.create_dataset(dataset)
            logger.info(f"Created dataset {dataset_ref}")

    # Tables RAW avec schéma simple (auto-detect gérera le reste)
    raw_dataset = os.getenv('BQ_DATASET_RAW', 'transport_raw')

    raw_tables = {
        'raw_validations': [
            bigquery.SchemaField('date', 'DATE'),
            bigquery.SchemaField('stop_id', 'STRING'),
            bigquery.SchemaField('stop_name', 'STRING'),
            bigquery.SchemaField('line_id', 'STRING'),
            bigquery.SchemaField('line_name', 'STRING'),
            bigquery.SchemaField('ticket_type', 'STRING'),
            bigquery.SchemaField('validation_count', 'INTEGER'),
            bigquery.SchemaField('ingestion_ts', 'TIMESTAMP'),
            bigquery.SchemaField('source', 'STRING')
        ],
        'raw_punctuality': [
            bigquery.SchemaField('month', 'DATE'),
            bigquery.SchemaField('line_id', 'STRING'),
            bigquery.SchemaField('line_name', 'STRING'),
            bigquery.SchemaField('punctuality_rate', 'FLOAT64'),
            bigquery.SchemaField('trains_planned', 'INTEGER'),
            bigquery.SchemaField('trains_departed', 'INTEGER'),
            bigquery.SchemaField('ingestion_ts', 'TIMESTAMP'),
            bigquery.SchemaField('source', 'STRING')
        ],
        'raw_ref_stops': [
            bigquery.SchemaField('stop_id', 'STRING'),
            bigquery.SchemaField('stop_name', 'STRING'),
            bigquery.SchemaField('latitude', 'FLOAT64'),
            bigquery.SchemaField('longitude', 'FLOAT64'),
            bigquery.SchemaField('town', 'STRING'),
            bigquery.SchemaField('ingestion_ts', 'TIMESTAMP'),
            bigquery.SchemaField('source', 'STRING')
        ],
        'raw_ref_lines': [
            bigquery.SchemaField('line_id', 'STRING'),
            bigquery.SchemaField('line_name', 'STRING'),
            bigquery.SchemaField('transport_mode', 'STRING'),
            bigquery.SchemaField('operator', 'STRING'),
            bigquery.SchemaField('ingestion_ts', 'TIMESTAMP'),
            bigquery.SchemaField('source', 'STRING')
        ],
        'raw_ref_stop_lines': [
            bigquery.SchemaField('stop_id', 'STRING'),
            bigquery.SchemaField('line_id', 'STRING'),
            bigquery.SchemaField('ingestion_ts', 'TIMESTAMP'),
            bigquery.SchemaField('source', 'STRING')
        ]
    }

    for table_name, schema in raw_tables.items():
        table_id = f"{project_id}.{raw_dataset}.{table_name}"

        try:
            client.get_table(table_id)
            logger.info(f"Table {table_id} already exists")
        except NotFound:
            table = bigquery.Table(table_id, schema=schema)
            table = client.create_table(table)
            logger.info(f"Created table {table_id}")

    logger.info("BigQuery setup complete")


if __name__ == '__main__':
    setup_bigquery()
