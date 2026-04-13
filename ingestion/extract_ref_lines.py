"""
Extract reference data: lines (referentiel-des-lignes).

All transport lines for Ile-de-France: RER, Metro, Transilien, Bus, Tram.
This is a full snapshot — always overwrites previous extraction.

Usage:
    python ingestion/extract_ref_lines.py
    python ingestion/extract_ref_lines.py --output /tmp/data
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import yaml
from dotenv import load_dotenv
from google.cloud import storage
from odsv2_client import ODSv2Client

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

# FIX V2: resolve project root from env var first, fall back to script location.
# - PROJECT_ROOT in .env → used in Airflow, CI/CD, or any non-standard layout
# - Path(__file__).parent.parent → automatic fallback for local dev (no config needed)
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))


def load_config():
    """Load API configuration from YAML file."""
    # FIX V2: use PROJECT_ROOT instead of Path(__file__).parent.parent
    config_path = PROJECT_ROOT / "config" / "apis.yml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def extract_ref_lines(gcs_bucket: str = None):
    """Extract full lines referential from IDFM API."""

    config = load_config()
    idfm_config = config["idfm"]
    dataset_config = idfm_config["datasets"]["ref_lines"]

    client = ODSv2Client(
        base_url=idfm_config["base_url"], dataset_id=dataset_config["id"]
    )

    fields = dataset_config["fields"]
    select_clause = ", ".join(fields.values())

    logger.info("Extracting reference data: lines")

    records = client.get_all_records(select=select_clause)

    if not records:
        logger.warning("No records found")
        return

    # FIX V2: fields at ROOT level — do not use client.extract_fields()
    # which expects nested record['record']['fields'] (old ODS v1 structure).
    extracted = []
    for record in records:
        extracted_record = {
            target: record.get(source) for target, source in fields.items()
        }
        # FIX V2: datetime.utcnow() deprecated in Python 3.12+
        extracted_record["ingestion_ts"] = datetime.now(timezone.utc).isoformat()
        extracted_record["source"] = "idfm_ref_lines"
        extracted.append(extracted_record)

    bucket_name = gcs_bucket or os.getenv("GCS_BUCKET_RAW")
    blob_path = f"referentials/ref_lines_{datetime.now().strftime('%Y%m%d')}.json"
    ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in extracted)
    storage.Client().bucket(bucket_name).blob(blob_path).upload_from_string(
        ndjson, content_type="application/json"
    )
    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    logger.info(f"✅ Uploaded {len(extracted)} lines to {gcs_uri}")


def main():
    parser = argparse.ArgumentParser(description="Extract IDFM lines referential")
    parser.add_argument(
        "--bucket",
        default=None,
        help="GCS bucket name (default: GCS_BUCKET_RAW env var)",
    )
    args = parser.parse_args()
    extract_ref_lines(args.bucket)


if __name__ == "__main__":
    main()
