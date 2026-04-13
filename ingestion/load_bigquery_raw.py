"""
Load JSON files from GCS landing zone into BigQuery RAW tables.

Architecture: API → GCS (NDJSON) → BigQuery RAW → dbt

GCS layout:
  gs://{GCS_BUCKET_RAW}/validations/validations_*.json
  gs://{GCS_BUCKET_RAW}/punctuality/punctuality_*.json
  gs://{GCS_BUCKET_RAW}/referentials/ref_stops_*.json
  gs://{GCS_BUCKET_RAW}/referentials/ref_lines_*.json
  gs://{GCS_BUCKET_RAW}/referentials/ref_stop_lines_*.json
  gs://{GCS_BUCKET_RAW}/referentials/ref_stations_*.json
  gs://{GCS_BUCKET_RAW}/referentials/ref_stop_id_mapping_*.json

Usage:
    python ingestion/load_bigquery_raw.py              # Load all datasets
    python ingestion/load_bigquery_raw.py --validations
    python ingestion/load_bigquery_raw.py --punctuality
    python ingestion/load_bigquery_raw.py --referentials
"""

import argparse
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from google.cloud import bigquery, storage

PROJECT_ROOT = Path(__file__).parent.parent

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv(PROJECT_ROOT / ".env")


class BigQueryLoader:
    """Load NDJSON files from GCS into BigQuery RAW tables."""

    def __init__(self):
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.dataset_raw = os.getenv("BQ_DATASET_RAW", "transport_raw")
        self.gcs_bucket = os.getenv("GCS_BUCKET_RAW")
        self.bq_client = bigquery.Client(project=self.project_id)
        self.gcs_client = storage.Client()
        logger.info(
            f"Initialized loader for {self.project_id}.{self.dataset_raw} "
            f"← gs://{self.gcs_bucket}"
        )

    def _list_gcs_uris(self, prefix: str) -> list[str]:
        """List GCS objects matching a prefix, sorted by name."""
        blobs = sorted(
            self.gcs_client.list_blobs(self.gcs_bucket, prefix=prefix),
            key=lambda b: b.name,
        )
        return [
            f"gs://{self.gcs_bucket}/{b.name}"
            for b in blobs
            if b.name.endswith(".json")
        ]

    def load_from_gcs(
        self,
        gcs_uri: str,
        table_name: str,
        schema: list = None,
        write_disposition: str = "WRITE_APPEND",
    ):
        """
        Load a NDJSON file from GCS into a BigQuery table.

        Args:
            gcs_uri: GCS URI, e.g. gs://bucket/validations/validations_2026-04-13.json
            table_name: Target table name (without dataset prefix)
            schema: BigQuery schema (None = autodetect)
            write_disposition: WRITE_APPEND or WRITE_TRUNCATE
        """
        table_id = f"{self.project_id}.{self.dataset_raw}.{table_name}"
        logger.info(f"Loading {gcs_uri} → {table_id}")

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition,
            autodetect=(schema is None),
            schema=schema,
        )

        load_job = self.bq_client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )
        load_job.result()

        table = self.bq_client.get_table(table_id)
        logger.info(
            f"✅ Loaded {load_job.output_rows} rows → {table_id} ({table.num_rows} total)"
        )

        if write_disposition == "WRITE_TRUNCATE":
            source_count = load_job.output_rows
            if table.num_rows != source_count:
                raise ValueError(
                    f"Row count mismatch after WRITE_TRUNCATE: "
                    f"table has {table.num_rows} rows but job loaded {source_count}. "
                    f"Possible double-load or partial failure — investigate before retrying."
                )

    def load_validations(self, truncate: bool = False):
        """Load all validation NDJSON files from GCS."""
        uris = self._list_gcs_uris("validations/")

        if not uris:
            logger.warning(
                f"No validation files found in gs://{self.gcs_bucket}/validations/"
            )
            return

        logger.info(f"Found {len(uris)} validation file(s)")
        for i, uri in enumerate(uris):
            disposition = "WRITE_TRUNCATE" if (truncate and i == 0) else "WRITE_APPEND"
            if truncate and i == 0:
                logger.info("Truncate mode: first file will reset the table schema")
            self.load_from_gcs(uri, "raw_validations", write_disposition=disposition)

    def load_punctuality(self, truncate: bool = False):
        """Load all punctuality NDJSON files from GCS."""
        uris = self._list_gcs_uris("punctuality/")

        if not uris:
            logger.warning(
                f"No punctuality files found in gs://{self.gcs_bucket}/punctuality/"
            )
            return

        logger.info(f"Found {len(uris)} punctuality file(s)")
        for i, uri in enumerate(uris):
            disposition = "WRITE_TRUNCATE" if (truncate and i == 0) else "WRITE_APPEND"
            self.load_from_gcs(uri, "raw_punctuality", write_disposition=disposition)

    def load_referentials(self):
        """
        Load reference data from GCS (TRUNCATE mode — full snapshot each time).

        Always uses WRITE_TRUNCATE since referentials are complete snapshots.
        Uses the most recent file per referential type.
        """
        ref_tables = {
            "referentials/ref_stops": "raw_ref_stops",
            "referentials/ref_lines": "raw_ref_lines",
            "referentials/ref_stop_lines": "raw_ref_stop_lines",
            "referentials/ref_stations": "raw_ref_stations",
            "referentials/ref_stop_id_mapping": "raw_ref_stop_id_mapping",
        }

        for prefix, table_name in ref_tables.items():
            uris = self._list_gcs_uris(prefix)

            if not uris:
                logger.warning(
                    f"No files found for gs://{self.gcs_bucket}/{prefix}_*.json"
                )
                continue

            latest_uri = uris[-1]
            logger.info(f"Using latest {prefix}: {latest_uri}")
            self.load_from_gcs(
                latest_uri, table_name, write_disposition="WRITE_TRUNCATE"
            )

    def load_all(self, truncate: bool = False):
        """Load all datasets: validations, punctuality, referentials."""
        logger.info("Starting full data load to BigQuery RAW")
        self.load_validations(truncate=truncate)
        self.load_punctuality(truncate=truncate)
        self.load_referentials()
        logger.info("✅ Full data load complete")


def main():
    parser = argparse.ArgumentParser(
        description="Load NDJSON files from GCS into BigQuery RAW tables"
    )
    parser.add_argument(
        "--validations", action="store_true", help="Load validations only"
    )
    parser.add_argument(
        "--punctuality", action="store_true", help="Load punctuality only"
    )
    parser.add_argument(
        "--referentials", action="store_true", help="Load referentials only"
    )
    parser.add_argument(
        "--truncate",
        action="store_true",
        help="Reset tables before loading (use after schema change)",
    )

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


if __name__ == "__main__":
    main()
