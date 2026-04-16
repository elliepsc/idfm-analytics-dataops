"""
Extract hourly validation profiles for the rail network (ferré).

Dataset: "Profils horaires de validations réseau ferré par arrêt et jour type"
Grain: 1 stop × 1 hour × 1 day_type (JOHV / SAM / DIM).
Published quarterly by IDFM — this extractor targets the most recent edition.

Use case:
  - Enriches mart_validations_line_daily with intra-day demand distribution.
  - Supports weighted allocation for off-peak vs peak hour analysis.
  - Prepares data for V4-A pondération horaire (weighted stop allocation).

This is a full snapshot — always overwrites previous extraction (WRITE_TRUNCATE).
The quarter edition is embedded in the filename for lineage tracking.

Usage:
    python ingestion/extract_hourly_profiles.py
    python ingestion/extract_hourly_profiles.py --quarter "T4 2025"
    python ingestion/extract_hourly_profiles.py --output /tmp/data
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

# Canonical day_type mapping → JOHV / SAM / DIM
# ODS dataset uses: JOHV, SAHV, JOVS, SAVS, DIJFP
# Some older editions use French full names
DAY_TYPE_MAP = {
    # Code-based values
    "JOHV": "JOHV",  # Jour Ouvré Hors Vacances Scolaires
    "JOVS": "JOHV",  # Jour Ouvré en Vacances Scolaires — grouped with weekday
    "SAHV": "SAM",  # Samedi Hors Vacances Scolaires
    "SAVS": "SAM",  # Samedi en Vacances Scolaires
    "DIJFP": "DIM",  # Dimanche, Jour Férié, Ponts
    # French full names (legacy editions)
    "LUNDI-JEUDI HORS VACANCES": "JOHV",
    "LUNDI A JEUDI": "JOHV",
    "VENDREDI": "JOHV",
    "SAMEDI": "SAM",
    "DIMANCHE": "DIM",
    "DIMANCHE ET FERIES": "DIM",
    "FERIE": "DIM",
}


def load_config():
    config_path = PROJECT_ROOT / "config" / "apis.yml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def normalise_day_type(raw: str) -> str:
    """Normalise raw day_type string to canonical JOHV / SAM / DIM codes."""
    if not raw:
        return "JOHV"
    cleaned = raw.upper().strip()
    return DAY_TYPE_MAP.get(cleaned, cleaned)


def parse_hour(hour_label: str) -> int | None:
    """Parse hour integer from label like '05H-06H' or '5H-6H' → 5."""
    if not hour_label:
        return None
    import re

    m = re.match(r"^(\d+)H", hour_label.strip().upper())
    if m:
        return int(m.group(1))
    logger.warning(f"Unexpected hour_label format (will be NULL in BQ): {hour_label!r}")
    return None


def extract_hourly_profiles(
    quarter: str = None,
    gcs_bucket: str = None,
    output_dir: Path = None,
):
    """
    Extract hourly validation profiles from IDFM ODS API.

    Args:
        quarter: Quarter label for filename tagging (e.g. "T4 2025").
                 Does not filter the API — the dataset is a single quarterly snapshot.
        gcs_bucket: GCS bucket name. Falls back to GCS_BUCKET_RAW env var.
        output_dir: Local output directory (fallback when no GCS).
    """
    config = load_config()
    idfm_config = config["idfm"]
    dataset_config = idfm_config["datasets"]["hourly_profiles"]

    client = ODSv2Client(
        base_url=idfm_config["base_url"], dataset_id=dataset_config["id"]
    )

    fields = dataset_config["fields"]
    select_clause = ", ".join(fields.values())

    logger.info("Extracting hourly validation profiles (ferré)")

    # Full-snapshot dataset — no date filter
    records = client.get_all_records(select=select_clause)

    if not records:
        logger.warning("No records found — check dataset availability for this quarter")
        return

    # Quarter edition: from arg, then from config, then fallback
    edition = quarter or dataset_config.get("quarter", "unknown")

    ingestion_ts = datetime.now(timezone.utc).isoformat()
    extracted = []
    for record in records:
        extracted_record = {
            target: record.get(source) for target, source in fields.items()
        }

        # Parse hour integer from "05H-06H" text label
        extracted_record["hour"] = parse_hour(extracted_record.pop("hour_label", None))

        # Normalise day_type to JOHV / SAM / DIM
        extracted_record["day_type"] = normalise_day_type(
            extracted_record.get("day_type")
        )

        # Cast validation_share to float
        raw_share = extracted_record.get("validation_share")
        extracted_record["validation_share"] = (
            float(raw_share) if raw_share is not None else None
        )

        extracted_record["quarter"] = edition
        extracted_record["ingestion_ts"] = ingestion_ts
        extracted_record["source"] = "idfm_hourly_profiles"
        extracted.append(extracted_record)
    safe_edition = (edition or "").replace(" ", "_")
    filename = (
        f"hourly_profiles_{safe_edition}_{datetime.now().strftime('%Y%m%d')}.json"
    )
    ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in extracted)

    bucket_name = gcs_bucket or os.getenv("GCS_BUCKET_RAW")
    if bucket_name:
        blob_path = f"hourly_profiles/{filename}"
        storage.Client().bucket(bucket_name).blob(blob_path).upload_from_string(
            ndjson, content_type="application/json"
        )
        logger.info(
            f"✅ Uploaded {len(extracted)} hourly profile rows to gs://{bucket_name}/{blob_path}"
        )
    else:
        local_dir = (
            Path(output_dir)
            if output_dir
            else PROJECT_ROOT / "data/bronze/hourly_profiles"
        )
        local_dir.mkdir(parents=True, exist_ok=True)
        filepath = local_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(ndjson)
        logger.info(f"✅ Saved {len(extracted)} hourly profile rows to {filepath}")


def main():
    parser = argparse.ArgumentParser(
        description="Extract IDFM hourly validation profiles (ferré)"
    )
    parser.add_argument(
        "--quarter",
        default=None,
        help="Quarter edition label (e.g. 'T4 2025') — for filename tagging only",
    )
    parser.add_argument(
        "--bucket", default=None, help="GCS bucket (default: GCS_BUCKET_RAW env var)"
    )
    parser.add_argument(
        "--output", default=None, help="Local output dir (fallback when no GCS)"
    )
    args = parser.parse_args()
    extract_hourly_profiles(
        quarter=args.quarter,
        gcs_bucket=args.bucket,
        output_dir=args.output,
    )


if __name__ == "__main__":
    main()
