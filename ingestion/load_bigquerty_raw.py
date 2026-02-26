import logging
import json
from pathlib import Path
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


class BigQueryLoader:
    """Charge les données JSON dans BigQuery RAW"""

    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID')
        self.dataset_raw = os.getenv('BQ_DATASET_RAW', 'transport_raw')
        self.client = bigquery.Client(project=self.project_id)

        logger.info(f"Initialized loader for {self.project_id}.{self.dataset_raw}")

    def load_json_to_table(
        self,
        json_file: Path,
        table_name: str,
        schema: list = None,
        write_disposition: str = 'WRITE_APPEND'
    ):
        """
        Charge un fichier JSON dans une table BigQuery

        Args:
            json_file: Chemin vers le fichier JSON
            table_name: Nom de la table (sans dataset)
            schema: Schéma BigQuery (None = auto-detect)
            write_disposition: WRITE_APPEND ou WRITE_TRUNCATE
        """
        table_id = f"{self.project_id}.{self.dataset_raw}.{table_name}"

        logger.info(f"Loading {json_file} to {table_id}")

        # Configuration du job
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition,
            autodetect=(schema is None),
            schema=schema
        )

        # Chargement
        with open(json_file, 'rb') as f:
            load_job = self.client.load_table_from_file(
                f,
                table_id,
                job_config=job_config
            )

        # Attendre la fin
        load_job.result()

        # Stats
        table = self.client.get_table(table_id)
        logger.info(f"Loaded {load_job.output_rows} rows to {table_id}")
        logger.info(f"Table now has {table.num_rows} total rows")

    def load_validations(self, data_dir: str = 'data/bronze/validations'):
        """Charge tous les fichiers de validations"""
        data_path = Path(data_dir)

        if not data_path.exists():
            logger.warning(f"Directory not found: {data_dir}")
            return

        json_files = list(data_path.glob('validations_*.json'))

        if not json_files:
            logger.warning(f"No validation files found in {data_dir}")
            return

        logger.info(f"Found {len(json_files)} validation files")

        for json_file in sorted(json_files):
            self.load_json_to_table(
                json_file=json_file,
                table_name='raw_validations',
                write_disposition='WRITE_APPEND'
            )

    def load_punctuality(self, data_dir: str = 'data/bronze/punctuality'):
        """Charge tous les fichiers de ponctualité"""
        data_path = Path(data_dir)

        if not data_path.exists():
            logger.warning(f"Directory not found: {data_dir}")
            return

        json_files = list(data_path.glob('punctuality_*.json'))

        if not json_files:
            logger.warning(f"No punctuality files found in {data_dir}")
            return

        logger.info(f"Found {len(json_files)} punctuality files")

        for json_file in sorted(json_files):
            self.load_json_to_table(
                json_file=json_file,
                table_name='raw_punctuality',
                write_disposition='WRITE_APPEND'
            )

    def load_referentials(self, data_dir: str = 'data/bronze/referentials'):
        """Charge les référentiels (TRUNCATE car snapshot complet)"""
        data_path = Path(data_dir)

        if not data_path.exists():
            logger.warning(f"Directory not found: {data_dir}")
            return

        # Tables référentielles
        ref_tables = {
            'ref_stops': 'raw_ref_stops',
            'ref_lines': 'raw_ref_lines',
            'ref_stop_lines': 'raw_ref_stop_lines'
        }

        for pattern, table_name in ref_tables.items():
            json_files = list(data_path.glob(f'{pattern}_*.json'))

            if not json_files:
                logger.warning(f"No {pattern} files found")
                continue

            # Prendre le plus récent
            latest_file = sorted(json_files)[-1]

            logger.info(f"Loading {latest_file} (latest {pattern})")

            self.load_json_to_table(
                json_file=latest_file,
                table_name=table_name,
                write_disposition='WRITE_TRUNCATE'  # Remplace complètement
            )

    def load_all(self):
        """Charge toutes les données"""
        logger.info("Starting full data load")

        self.load_validations()
        self.load_punctuality()
        self.load_referentials()

        logger.info("Data load complete")


def main():
    loader = BigQueryLoader()
    loader.load_all()


if __name__ == '__main__':
    main()
