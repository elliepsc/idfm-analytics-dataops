"""
Load JSON files from Bronze layer into BigQuery RAW tables.

Replaces load_bigquerty_raw.py (V1) with three key fixes:
  1. Filename typo fixed (bigquerty → bigquery)
  2. JSON array → NDJSON conversion added (BigQuery requires one object per line)
  3. Paths anchored to script location, not working directory

Usage:
    python ingestion/load_bigquery_raw.py              # Load all datasets
    python ingestion/load_bigquery_raw.py --validations
    python ingestion/load_bigquery_raw.py --punctuality
    python ingestion/load_bigquery_raw.py --referentials
"""

import argparse
import logging
import json
import io                          # FIX V2: needed for in-memory NDJSON buffer
from pathlib import Path
from google.cloud import bigquery
# FIX V2: removed unused 'from google.cloud.exceptions import NotFound' (V1 leftover)
import os
from dotenv import load_dotenv

# FIX V2: anchor paths to the script location, not the working directory.
# Path(__file__) = absolute path of this script (ingestion/load_bigquery_raw.py)
# .parent        = ingestion/
# .parent.parent = project root (idfm-analytics-dataops/)
# V1 used '../data/bronze/' which broke when called from project root.
PROJECT_ROOT = Path(__file__).parent.parent

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()


class BigQueryLoader:
    """Load Bronze JSON files into BigQuery RAW tables."""

    def __init__(self):
        self.project_id = os.getenv('GCP_PROJECT_ID')
        self.dataset_raw = os.getenv('BQ_DATASET_RAW', 'transport_raw')
        self.client = bigquery.Client(project=self.project_id)
        logger.info(f"Initialized loader for {self.project_id}.{self.dataset_raw}")

    # FIX V2: new method — did not exist in V1.
    # V1 opened JSON files in binary mode ('rb') and sent them directly to BigQuery.
    # This worked by chance for small referential files but would fail on large files
    # because BigQuery's NEWLINE_DELIMITED_JSON format requires one object per line,
    # not a JSON array [{...}, {...}].
    # This method converts in memory (no temp file written to disk).
    def _json_array_to_ndjson(self, json_file: Path) -> io.BytesIO:
        """
        Convert a JSON array file to NDJSON (newline-delimited JSON).

        BigQuery's load API requires one JSON object per line.
        Our Bronze files are standard JSON arrays: [{...}, {...}].

        Args:
            json_file: Path to JSON array file

        Returns:
            BytesIO buffer containing NDJSON data
        """
        with open(json_file, 'r', encoding='utf-8') as f:
            records = json.load(f)

        ndjson_lines = '\n'.join(json.dumps(record, ensure_ascii=False) for record in records)
        return io.BytesIO(ndjson_lines.encode('utf-8'))

    def load_json_to_table(
        self,
        json_file: Path,
        table_name: str,
        schema: list = None,
        write_disposition: str = 'WRITE_APPEND'
    ):
        """
        Load a JSON file into a BigQuery table.

        Args:
            json_file: Path to JSON array file (Bronze layer)
            table_name: Target table name (without dataset prefix)
            schema: BigQuery schema (None = autodetect)
            write_disposition: WRITE_APPEND or WRITE_TRUNCATE
        """
        table_id = f"{self.project_id}.{self.dataset_raw}.{table_name}"
        logger.info(f"Loading {json_file.name} → {table_id}")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition,
            autodetect=(schema is None),
            schema=schema
        )

        # FIX V2: convert JSON array to NDJSON before loading.
        # V1 did: with open(json_file, 'rb') as f: client.load_table_from_file(f, ...)
        # That sent raw JSON array bytes directly — fragile and format-incorrect.
        ndjson_buffer = self._json_array_to_ndjson(json_file)

        load_job = self.client.load_table_from_file(
            ndjson_buffer,
            table_id,
            job_config=job_config
        )

        load_job.result()  # Wait for completion

        table = self.client.get_table(table_id)
        logger.info(f"✅ Loaded {load_job.output_rows} rows → {table_id} ({table.num_rows} total)")

    # FIX V2: signature changed from (self, data_dir: str = '../data/bronze/validations')
    # to     (self, data_dir: Path = None) with PROJECT_ROOT as fallback.
    # '../data/...' is relative to the working directory — breaks when called from project root.
    # PROJECT_ROOT / 'data/...' is always absolute, anchored to the script location.
    def load_validations(self, data_dir: Path = None, truncate: bool = False):
        """Load all validation JSON files.

        Args:
            data_dir: Override default Bronze directory.
            truncate: If True, reset table before loading (use after schema change).
                      Default False = APPEND mode for daily increments.
        """
        data_path = Path(data_dir) if data_dir else PROJECT_ROOT / 'data/bronze/validations'

        if not data_path.exists():
            logger.warning(f"Directory not found: {data_path}")
            return

        json_files = sorted(data_path.glob('validations_*.json'))

        if not json_files:
            logger.warning(f"No validation files found in {data_path}")
            return

        logger.info(f"Found {len(json_files)} validation file(s)")

        # --truncate: WRITE_TRUNCATE on the first file to reset schema,
        # then WRITE_APPEND for the rest to stack all files.
        for i, json_file in enumerate(json_files):
            disposition = 'WRITE_TRUNCATE' if (truncate and i == 0) else 'WRITE_APPEND'
            if truncate and i == 0:
                logger.info("Truncate mode: first file will reset the table schema")
            self.load_json_to_table(
                json_file=json_file,
                table_name='raw_validations',
                write_disposition=disposition
            )

    # FIX V2: same path fix as load_validations — '../data/bronze/punctuality' → PROJECT_ROOT
    def load_punctuality(self, data_dir: Path = None, truncate: bool = False):
        """Load all punctuality JSON files.

        Args:
            data_dir: Override default Bronze directory.
            truncate: If True, reset table before loading (use after schema change).
        """
        data_path = Path(data_dir) if data_dir else PROJECT_ROOT / 'data/bronze/punctuality'

        if not data_path.exists():
            logger.warning(f"Directory not found: {data_path}")
            return

        json_files = sorted(data_path.glob('punctuality_*.json'))

        if not json_files:
            logger.warning(f"No punctuality files found in {data_path}")
            return

        logger.info(f"Found {len(json_files)} punctuality file(s)")

        for i, json_file in enumerate(json_files):
            disposition = 'WRITE_TRUNCATE' if (truncate and i == 0) else 'WRITE_APPEND'
            self.load_json_to_table(
                json_file=json_file,
                table_name='raw_punctuality',
                write_disposition=disposition
            )

    # FIX V2: same path fix as load_validations — '../data/bronze/referentials' → PROJECT_ROOT
    def load_referentials(self, data_dir: Path = None):
        """
        Load reference data files (TRUNCATE mode — full snapshot each time).

        Always uses WRITE_TRUNCATE since referentials are complete snapshots,
        not incremental data.
        """
        data_path = Path(data_dir) if data_dir else PROJECT_ROOT / 'data/bronze/referentials'

        if not data_path.exists():
            logger.warning(f"Directory not found: {data_path}")
            return

        # Map filename prefix → target table
        ref_tables = {
            'ref_stops': 'raw_ref_stops',
            'ref_lines': 'raw_ref_lines',
            'ref_stop_lines': 'raw_ref_stop_lines'
        }

        for pattern, table_name in ref_tables.items():
            json_files = sorted(data_path.glob(f'{pattern}_*.json'))

            if not json_files:
                logger.warning(f"No {pattern} files found in {data_path}")
                continue

            # Use the most recent file
            latest_file = json_files[-1]
            logger.info(f"Using latest {pattern}: {latest_file.name}")

            self.load_json_to_table(
                json_file=latest_file,
                table_name=table_name,
                write_disposition='WRITE_TRUNCATE'
            )

    def load_all(self, truncate: bool = False):
        """Load all datasets: validations, punctuality, referentials.

        Args:
            truncate: If True, reset transactional tables before loading.
                      Use after a schema change (e.g. new fields added).
                      Referentials are always truncated regardless.
        """
        logger.info("Starting full data load to BigQuery RAW")

        self.load_validations(truncate=truncate)
        self.load_punctuality(truncate=truncate)
        self.load_referentials()  # Always WRITE_TRUNCATE — no flag needed

        logger.info("✅ Full data load complete")


# FIX V2: added CLI argument parser — V1 had no arguments, always ran load_all().
# Now you can target a specific dataset without reloading everything.
def main():
    parser = argparse.ArgumentParser(description='Load Bronze JSON files into BigQuery RAW tables')
    parser.add_argument('--validations', action='store_true', help='Load validations only')
    parser.add_argument('--punctuality', action='store_true', help='Load punctuality only')
    parser.add_argument('--referentials', action='store_true', help='Load referentials only')
    # --truncate: use when schema has changed and existing RAW table must be reset.
    # Safe on RAW/Bronze — data is always reproducible from source API.
    # Never use on staging/core/marts without a proper SQL migration.
    parser.add_argument('--truncate', action='store_true',
                        help='Reset tables before loading (use after schema change)')

    args = parser.parse_args()
    loader = BigQueryLoader()

    if args.validations:
        loader.load_validations(truncate=args.truncate)
    elif args.punctuality:
        loader.load_punctuality(truncate=args.truncate)
    elif args.referentials:
        loader.load_referentials()
    else:
        loader.load_all(truncate=args.truncate)


if __name__ == '__main__':
    main()
