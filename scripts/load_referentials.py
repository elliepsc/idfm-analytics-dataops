"""Load referential data from Bronze JSON to BigQuery RAW tables"""

import json
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
RAW_DATASET = os.getenv("BQ_DATASET_RAW", "transport_raw")


def load_referentials():
    """Load ref_stops and ref_lines into BigQuery"""

    project_id = os.getenv("GCP_PROJECT_ID")
    client = bigquery.Client(project=project_id)

    # Load ref_stops
    stops_file = "data/bronze/referentials/ref_stops_20260226.json"
    if Path(stops_file).exists():
        logger.info(f"Loading {stops_file} into BigQuery...")

        with open(stops_file) as f:
            data = json.load(f)

        table_id = f"{project_id}.{RAW_DATASET}.raw_ref_stops"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        # Convert list to newline-delimited JSON

        job = client.load_table_from_json(data, table_id, job_config=job_config)

        job.result()  # Wait for completion
        logger.info(f"✅ Loaded {len(data)} stops into {table_id}")

    # Load ref_lines
    lines_file = "data/bronze/referentials/ref_lines_20260226.json"
    if Path(lines_file).exists():
        logger.info(f"Loading {lines_file} into BigQuery...")

        with open(lines_file) as f:
            data = json.load(f)

        table_id = f"{project_id}.{RAW_DATASET}.raw_ref_lines"

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        )

        job = client.load_table_from_json(data, table_id, job_config=job_config)

        job.result()
        logger.info(f"✅ Loaded {len(data)} lines into {table_id}")

    logger.info("✅ All referentials loaded!")


if __name__ == "__main__":
    load_referentials()
