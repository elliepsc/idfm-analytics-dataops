"""
Load NDJSON files from GCS or local disk into BigQuery RAW tables.

Architecture (prod):  API → GCS (NDJSON) → BigQuery RAW → dbt
Architecture (local): API → local disk (NDJSON) → BigQuery RAW → dbt

GCS layout (when GCS_BUCKET_RAW is set):
  gs://{GCS_BUCKET_RAW}/validations/validations_*.json
  gs://{GCS_BUCKET_RAW}/punctuality/punctuality_*.json
  gs://{GCS_BUCKET_RAW}/referentials/ref_*.json

Local layout (fallback for reproduction without GCP storage):
  data/bronze/validations/validations_*.json
  data/bronze/punctuality/punctuality_*.json
  data/bronze/referentials/ref_*.json

Usage:
    python ingestion/load_bigquery_raw.py              # Load all (GCS if set, else local)
    python ingestion/load_bigquery_raw.py --local-dir data/bronze  # Force local
    python ingestion/load_bigquery_raw.py --validations
"""

import argparse
import io
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
    """Load NDJSON files from GCS or local disk into BigQuery RAW tables."""

    def __init__(self, local_dir: Path = None):
        self.project_id = os.getenv("GCP_PROJECT_ID")
        self.dataset_raw = os.getenv("BQ_DATASET_RAW", "transport_raw")
        self.gcs_bucket = os.getenv("GCS_BUCKET_RAW")
        self.local_dir = Path(local_dir) if local_dir else None
        self.bq_client = bigquery.Client(project=self.project_id)
        if self.local_dir:
            logger.info(
                f"Initialized loader for {self.project_id}.{self.dataset_raw} ← {self.local_dir} (local)"
            )
        else:
            self.gcs_client = storage.Client()
            logger.info(
                f"Initialized loader for {self.project_id}.{self.dataset_raw} ← gs://{self.gcs_bucket}"
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

    def _list_local_files(self, subdir: str, pattern: str) -> list[Path]:
        """List local NDJSON files matching a glob pattern, sorted by name."""
        base = self.local_dir / subdir
        if not base.exists():
            return []
        return sorted(base.glob(pattern))

    def _load_job_config(self, write_disposition: str, schema: list = None):
        return bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition,
            autodetect=(schema is None),
            schema=schema,
        )

    def load_from_gcs(
        self,
        gcs_uri: str,
        table_name: str,
        schema: list = None,
        write_disposition: str = "WRITE_APPEND",
    ):
        """Load a NDJSON file from GCS into BigQuery."""
        table_id = f"{self.project_id}.{self.dataset_raw}.{table_name}"
        logger.info(f"Loading {gcs_uri} → {table_id}")
        load_job = self.bq_client.load_table_from_uri(
            gcs_uri,
            table_id,
            job_config=self._load_job_config(write_disposition, schema),
        )
        load_job.result()
        table = self.bq_client.get_table(table_id)
        logger.info(
            f"✅ Loaded {load_job.output_rows} rows → {table_id} ({table.num_rows} total)"
        )
        if (
            write_disposition == "WRITE_TRUNCATE"
            and table.num_rows != load_job.output_rows
        ):
            raise ValueError(
                f"Row count mismatch after WRITE_TRUNCATE: "
                f"table has {table.num_rows} rows but job loaded {load_job.output_rows}."
            )

    def load_from_local(
        self,
        local_path: Path,
        table_name: str,
        schema: list = None,
        write_disposition: str = "WRITE_APPEND",
    ):
        """Load a local NDJSON file into BigQuery via in-memory buffer."""
        table_id = f"{self.project_id}.{self.dataset_raw}.{table_name}"
        logger.info(f"Loading {local_path.name} → {table_id}")
        with open(local_path, "rb") as f:
            content = f.read()
        load_job = self.bq_client.load_table_from_file(
            io.BytesIO(content),
            table_id,
            job_config=self._load_job_config(write_disposition, schema),
        )
        load_job.result()
        table = self.bq_client.get_table(table_id)
        logger.info(
            f"✅ Loaded {load_job.output_rows} rows → {table_id} ({table.num_rows} total)"
        )
        if (
            write_disposition == "WRITE_TRUNCATE"
            and table.num_rows != load_job.output_rows
        ):
            raise ValueError(
                f"Row count mismatch after WRITE_TRUNCATE: "
                f"table has {table.num_rows} rows but job loaded {load_job.output_rows}."
            )

    def _load_files(self, uris_or_paths, table_name: str, truncate: bool = False):
        """Generic loader: handles both GCS URIs (str) and local paths (Path)."""
        for i, src in enumerate(uris_or_paths):
            disposition = "WRITE_TRUNCATE" if (truncate and i == 0) else "WRITE_APPEND"
            if isinstance(src, Path):
                self.load_from_local(src, table_name, write_disposition=disposition)
            else:
                self.load_from_gcs(src, table_name, write_disposition=disposition)

    def load_validations(self, truncate: bool = False):
        """Load all validation NDJSON files."""
        if self.local_dir:
            sources = self._list_local_files("validations", "validations_*.json")
        else:
            sources = self._list_gcs_uris("validations/")
        if not sources:
            logger.warning("No validation files found")
            return
        logger.info(f"Found {len(sources)} validation file(s)")
        self._load_files(sources, "raw_validations", truncate=truncate)

    def load_punctuality(self, truncate: bool = False):
        """Load all punctuality NDJSON files."""
        if self.local_dir:
            sources = self._list_local_files("punctuality", "punctuality_*.json")
        else:
            sources = self._list_gcs_uris("punctuality/")
        if not sources:
            logger.warning("No punctuality files found")
            return
        logger.info(f"Found {len(sources)} punctuality file(s)")
        self._load_files(sources, "raw_punctuality", truncate=truncate)

    def load_referentials(self):
        """Load reference data (TRUNCATE — full snapshot each time)."""
        ref_tables = {
            "ref_stops": "raw_ref_stops",
            "ref_lines": "raw_ref_lines",
            "ref_stop_lines": "raw_ref_stop_lines",
            "ref_stations": "raw_ref_stations",
            "ref_stop_id_mapping": "raw_ref_stop_id_mapping",
        }
        for prefix, table_name in ref_tables.items():
            if self.local_dir:
                sources = self._list_local_files("referentials", f"{prefix}_*.json")
            else:
                sources = self._list_gcs_uris(f"referentials/{prefix}")
            if not sources:
                logger.warning(f"No files found for {prefix}")
                continue
            latest = sources[-1]
            logger.info(f"Using latest {prefix}: {latest}")
            if isinstance(latest, Path):
                self.load_from_local(
                    latest, table_name, write_disposition="WRITE_TRUNCATE"
                )
            else:
                self.load_from_gcs(
                    latest, table_name, write_disposition="WRITE_TRUNCATE"
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
        description="Load NDJSON files from GCS or local disk into BigQuery RAW tables"
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
        "--truncate", action="store_true", help="Reset tables before loading"
    )
    parser.add_argument(
        "--local-dir",
        default=None,
        help="Load from local directory instead of GCS (e.g. data/bronze)",
    )

    args = parser.parse_args()
    loader = BigQueryLoader(local_dir=args.local_dir)

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
