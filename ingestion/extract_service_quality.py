"""
Extract quarterly service quality indicators from IDFM.

Dataset: "Indicateurs de qualité de service du parcours voyageur"
Grain: 1 line × 1 quarter × 1 indicator.
Covers RATP Métro + RER + SNCF Transilien — the main quality KPI replacing
punctuality-only analysis (X1 reliability layer).

This is a full snapshot — overwrites previous extraction.
Published quarterly by IDFM, typically 3-4 weeks after quarter end.

Usage:
    python ingestion/extract_service_quality.py
    python ingestion/extract_service_quality.py --output /tmp/data
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

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))


def load_config():
    config_path = PROJECT_ROOT / "config" / "apis.yml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def extract_service_quality(gcs_bucket: str = None, output_dir: Path = None):
    """Extract quarterly service quality indicators from IDFM ODS API."""

    config = load_config()
    idfm_config = config["idfm"]
    dataset_config = idfm_config["datasets"]["service_quality"]

    client = ODSv2Client(
        base_url=idfm_config["base_url"], dataset_id=dataset_config["id"]
    )

    fields = dataset_config["fields"]
    select_clause = ", ".join(fields.values())

    logger.info("Extracting quarterly service quality indicators")

    records = client.get_all_records(select=select_clause)

    if not records:
        logger.warning(
            "No records found — dataset may not be published yet for this quarter"
        )
        return

    extracted = []
    for record in records:
        extracted_record = {
            target: record.get(source) for target, source in fields.items()
        }
        # Combine trimestre (T1) + annee (2022) → "T1 2022" for staging parsing
        trimestre = extracted_record.pop("trimestre", None) or ""
        annee = extracted_record.pop("annee", None) or ""
        extracted_record["quarter"] = (
            f"{trimestre} {annee}".strip() if trimestre and annee else None
        )
        extracted_record["ingestion_ts"] = datetime.now(timezone.utc).isoformat()
        extracted_record["source"] = "idfm_service_quality"
        extracted.append(extracted_record)

    filename = f"service_quality_{datetime.now().strftime('%Y%m%d')}.json"
    ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in extracted)

    bucket_name = gcs_bucket or os.getenv("GCS_BUCKET_RAW")
    if bucket_name:
        blob_path = f"service_quality/{filename}"
        storage.Client().bucket(bucket_name).blob(blob_path).upload_from_string(
            ndjson, content_type="application/json"
        )
        logger.info(
            f"✅ Uploaded {len(extracted)} records to gs://{bucket_name}/{blob_path}"
        )
    else:
        local_dir = (
            Path(output_dir)
            if output_dir
            else PROJECT_ROOT / "data/bronze/service_quality"
        )
        local_dir.mkdir(parents=True, exist_ok=True)
        filepath = local_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(ndjson)
        logger.info(f"✅ Saved {len(extracted)} records to {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract IDFM quarterly service quality indicators"
    )
    parser.add_argument(
        "--bucket", default=None, help="GCS bucket (default: GCS_BUCKET_RAW env var)"
    )
    parser.add_argument(
        "--output", default=None, help="Local output dir (fallback when no GCS)"
    )
    args = parser.parse_args()
    extract_service_quality(gcs_bucket=args.bucket, output_dir=args.output)


if __name__ == "__main__":
    main()
