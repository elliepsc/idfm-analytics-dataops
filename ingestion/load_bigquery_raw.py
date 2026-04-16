"""
Load NDJSON files from GCS or local disk into BigQuery RAW tables.

Architecture (prod):  API → GCS (NDJSON) → BigQuery RAW → dbt
Architecture (local): API → local disk (NDJSON) → BigQuery RAW → dbt

GCS layout (when GCS_BUCKET_RAW is set):
  gs://{GCS_BUCKET_RAW}/validations/validations_*.json
  gs://{GCS_BUCKET_RAW}/punctuality/punctuality_*.json
  gs://{GCS_BUCKET_RAW}/referentials/ref_*.json
  gs://{GCS_BUCKET_RAW}/incidents/incidents_*.json          ← X2 daily
  gs://{GCS_BUCKET_RAW}/service_quality/service_quality_*.json  ← X1 quarterly
  gs://{GCS_BUCKET_RAW}/hourly_profiles/hourly_profiles_*.json  ← X5 quarterly

Local layout (fallback for reproduction without GCP storage):
  data/bronze/validations/validations_*.json
  data/bronze/punctuality/punctuality_*.json
  data/bronze/referentials/ref_*.json
  data/bronze/incidents/incidents_*.json
  data/bronze/service_quality/service_quality_*.json
  data/bronze/hourly_profiles/hourly_profiles_*.json

Usage:
    python ingestion/load_bigquery_raw.py              # Load all daily (GCS if set, else local)
    python ingestion/load_bigquery_raw.py --local-dir data/bronze  # Force local
    python ingestion/load_bigquery_raw.py --validations
    python ingestion/load_bigquery_raw.py --incidents   # X2: daily incidents only
    python ingestion/load_bigquery_raw.py --service-quality   # X1: quarterly snapshot
    python ingestion/load_bigquery_raw.py --hourly-profiles   # X5: quarterly snapshot
"""

import argparse
import io
import logging
import os
from pathlib import Path

from dotenv import load_dotenv
from google.api_core.exceptions import NotFound
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

    def _ensure_table_exists(self, table_name: str, schema: list[bigquery.SchemaField]):
        """Create an empty RAW table when a loader has nothing to append."""
        table_id = f"{self.project_id}.{self.dataset_raw}.{table_name}"
        try:
            self.bq_client.get_table(table_id)
        except NotFound:
            self.bq_client.create_table(bigquery.Table(table_id, schema=schema))
            logger.info(f"Created empty table {table_id}")

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

    def load_incidents(self, truncate: bool = False):
        """Load incident NDJSON files (WRITE_APPEND - daily new messages).

        X2 reliability layer. Each daily extraction appends new messages.
        Use --truncate only for a full historical reload.
        """
        incident_schema = [
            bigquery.SchemaField("incident_date", "STRING"),
            bigquery.SchemaField("incident_end_date", "STRING"),
            bigquery.SchemaField("line_id", "STRING"),
            bigquery.SchemaField("line_name", "STRING"),
            bigquery.SchemaField("incident_type", "STRING"),
            bigquery.SchemaField("incident_type_raw", "STRING"),
            bigquery.SchemaField("cause", "STRING"),
            bigquery.SchemaField("affected_stops", "STRING"),
            bigquery.SchemaField("transport_mode", "STRING"),
            bigquery.SchemaField("affected_stop_count", "INT64"),
            bigquery.SchemaField("ingestion_ts", "TIMESTAMP"),
            bigquery.SchemaField("source", "STRING"),
        ]
        self._ensure_table_exists("raw_incidents_daily", incident_schema)
        if self.local_dir:
            sources = self._list_local_files("incidents", "incidents_*.json")
        else:
            sources = self._list_gcs_uris("incidents/")
        if not sources:
            logger.warning("No incident files found — skipping (X2 not yet extracted)")
            return
        logger.info(f"Found {len(sources)} incident file(s)")
        self._load_files(sources, "raw_incidents_daily", truncate=truncate)

    def load_service_quality(self):
        """Load service quality snapshot (WRITE_TRUNCATE — full quarterly snapshot).

        X1 reliability layer. Always replaces the table with the latest file.
        Published quarterly by IDFM — run once per quarter after extract_service_quality.py.
        """
        if self.local_dir:
            sources = self._list_local_files(
                "service_quality", "service_quality_*.json"
            )
        else:
            sources = self._list_gcs_uris("service_quality/")
        if not sources:
            logger.warning(
                "No service quality files found — skipping (X1 not yet extracted)"
            )
            return
        latest = sources[-1]
        logger.info(f"Using latest service quality file: {latest}")
        if isinstance(latest, Path):
            self.load_from_local(
                latest, "raw_service_quality", write_disposition="WRITE_TRUNCATE"
            )
        else:
            self.load_from_gcs(
                latest, "raw_service_quality", write_disposition="WRITE_TRUNCATE"
            )

    def load_hourly_profiles(self):
        """Load hourly validation profiles snapshot (WRITE_TRUNCATE — quarterly edition).

        X5 reliability layer. Always replaces the table with the latest quarterly file.
        Published quarterly by IDFM — run once per quarter after extract_hourly_profiles.py.
        """
        if self.local_dir:
            sources = self._list_local_files(
                "hourly_profiles", "hourly_profiles_*.json"
            )
        else:
            sources = self._list_gcs_uris("hourly_profiles/")
        if not sources:
            logger.warning(
                "No hourly profile files found — skipping (X5 not yet extracted)"
            )
            return
        latest = sources[-1]
        logger.info(f"Using latest hourly profiles file: {latest}")
        if isinstance(latest, Path):
            self.load_from_local(
                latest, "raw_hourly_profiles", write_disposition="WRITE_TRUNCATE"
            )
        else:
            self.load_from_gcs(
                latest, "raw_hourly_profiles", write_disposition="WRITE_TRUNCATE"
            )

    def load_all(self, truncate: bool = False):
        """Load daily datasets: validations, punctuality, referentials, incidents.

        NOTE: service_quality and hourly_profiles are quarterly — they are NOT
        included here. Run load_service_quality() and load_hourly_profiles()
        separately from the quarterly DAG (transport_quarterly_pipeline).
        """
        logger.info("Starting full daily data load to BigQuery RAW")
        self.load_validations(truncate=truncate)
        self.load_punctuality(truncate=truncate)
        self.load_referentials()
        self.load_incidents(truncate=truncate)
        logger.info("✅ Full daily data load complete")


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
        "--incidents", action="store_true", help="Load incidents only (X2, daily)"
    )
    parser.add_argument(
        "--service-quality",
        action="store_true",
        help="Load service quality only (X1, quarterly WRITE_TRUNCATE)",
    )
    parser.add_argument(
        "--hourly-profiles",
        action="store_true",
        help="Load hourly profiles only (X5, quarterly WRITE_TRUNCATE)",
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
    elif args.incidents:
        loader.load_incidents(truncate=args.truncate)
    elif args.service_quality:
        loader.load_service_quality()
    elif args.hourly_profiles:
        loader.load_hourly_profiles()
    else:
        loader.load_all(truncate=args.truncate)


if __name__ == "__main__":
    main()
