"""
Extract reference data: lines (referentiel-des-lignes).

All transport lines for Ile-de-France: RER, Metro, Transilien, Bus, Tram.
This is a full snapshot and overwrites the previous extraction.

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

# Resolve project root from env var first, then fall back to the script location.
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))

TRANSPORT_MODE_PRIORITY = {
    "RER": 1,
    "METRO": 2,
    "TRANSILIEN": 3,
    "TRAM": 4,
    "CABLEWAY": 5,
    "FUNICULAR": 6,
    "BUS": 7,
    "RAIL": 8,
}


def load_config():
    """Load API configuration from the shared YAML file."""
    config_path = PROJECT_ROOT / "config" / "apis.yml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def _clean_text(value):
    """Return a trimmed string value, or None for blank / missing fields."""
    if value is None:
        return None

    if isinstance(value, str):
        value = value.strip()
        return value or None

    value = str(value).strip()
    return value or None


def _line_record_rank(record: dict) -> tuple:
    """Rank candidate duplicates so extraction keeps the most complete record."""
    line_name = record.get("line_name")
    operator = record.get("operator")
    transport_mode = record.get("transport_mode")

    return (
        0 if line_name else 1,
        0 if operator else 1,
        TRANSPORT_MODE_PRIORITY.get(transport_mode or "", 999),
        -(len(line_name) if line_name else 0),
        operator or "",
        transport_mode or "",
    )


def normalize_line_records(records: list[dict], fields: dict[str, str]) -> list[dict]:
    """Normalize raw API rows and keep one canonical record per non-null line_id."""
    ingestion_ts = datetime.now(timezone.utc).isoformat()
    deduped_by_line_id: dict[str, dict] = {}
    records_without_line_id: list[dict] = []
    duplicate_line_ids: set[str] = set()

    for record in records:
        normalized_record = {
            target: _clean_text(record.get(source)) for target, source in fields.items()
        }

        if normalized_record.get("line_id"):
            normalized_record["line_id"] = normalized_record["line_id"].upper()
        if normalized_record.get("transport_mode"):
            normalized_record["transport_mode"] = normalized_record[
                "transport_mode"
            ].upper()

        normalized_record["ingestion_ts"] = ingestion_ts
        normalized_record["source"] = "idfm_ref_lines"

        line_id = normalized_record.get("line_id")
        if not line_id:
            records_without_line_id.append(normalized_record)
            continue

        current_record = deduped_by_line_id.get(line_id)
        if current_record is None:
            deduped_by_line_id[line_id] = normalized_record
            continue

        duplicate_line_ids.add(line_id)
        if _line_record_rank(normalized_record) < _line_record_rank(current_record):
            deduped_by_line_id[line_id] = normalized_record

    normalized_records = list(deduped_by_line_id.values()) + records_without_line_id

    if duplicate_line_ids:
        sample_line_ids = ", ".join(sorted(duplicate_line_ids)[:10])
        suffix = " ..." if len(duplicate_line_ids) > 10 else ""
        logger.warning(
            "Detected %s duplicate line_id values in the IDFM referential: %s%s",
            f"{len(duplicate_line_ids):,}",
            sample_line_ids,
            suffix,
        )

    logger.info(
        "Normalized %s raw line rows into %s output rows (%s rows without line_id)",
        f"{len(records):,}",
        f"{len(normalized_records):,}",
        f"{len(records_without_line_id):,}",
    )
    return normalized_records


def extract_ref_lines(gcs_bucket: str = None, output_dir: Path = None):
    """Extract the full lines referential from the IDFM API."""
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

    extracted = normalize_line_records(records, fields)

    filename = f"ref_lines_{datetime.now().strftime('%Y%m%d')}.json"
    ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in extracted)

    bucket_name = gcs_bucket or os.getenv("GCS_BUCKET_RAW")
    if bucket_name:
        blob_path = f"referentials/{filename}"
        storage.Client().bucket(bucket_name).blob(blob_path).upload_from_string(
            ndjson, content_type="application/json"
        )
        logger.info(
            f"Uploaded {len(extracted)} lines to gs://{bucket_name}/{blob_path}"
        )
    else:
        local_dir = (
            Path(output_dir)
            if output_dir
            else PROJECT_ROOT / "data/bronze/referentials"
        )
        local_dir.mkdir(parents=True, exist_ok=True)
        filepath = local_dir / filename
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(ndjson)
        logger.info(f"Saved {len(extracted)} lines to {filepath}")


def main():
    parser = argparse.ArgumentParser(description="Extract IDFM lines referential")
    parser.add_argument(
        "--bucket", default=None, help="GCS bucket (default: GCS_BUCKET_RAW env var)"
    )
    parser.add_argument(
        "--output", default=None, help="Local output dir (fallback when no GCS)"
    )
    args = parser.parse_args()
    extract_ref_lines(gcs_bucket=args.bucket, output_dir=args.output)


if __name__ == "__main__":
    main()
