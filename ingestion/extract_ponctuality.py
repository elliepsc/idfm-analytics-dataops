"""
Extract monthly punctuality data from Transilien (SNCF).

Dataset: ponctualite-mensuelle-transilien
API structure confirmed 2026-02-27: fields at ROOT level. Date format = YYYY-MM.
Example: {"date": "2013-01", "ligne": "A", "taux_de_ponctualite": 83.6, ...}

Fields that do NOT exist in this dataset (wrong assumptions in V1):
  - taux_regularite       → correct field is taux_de_ponctualite
  - nombre_de_trains_prevu
  - nombre_de_trains_partis

Usage:
    python ingestion/extract_ponctuality.py --start 2024-01 --end 2024-12
    python ingestion/extract_ponctuality.py --start 2024-01-01 --end 2024-12-31
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
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

# FIX V2: resolve project root from env var first, fall back to script location.
# - PROJECT_ROOT in .env → used in Airflow, CI/CD, or any non-standard layout
# - Path(__file__).parent.parent → automatic fallback for local dev (no config needed)
PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "bronze" / "punctuality"


def load_config():
    """Load API configuration from YAML file."""
    config_path = PROJECT_ROOT / "config" / "apis.yml"
    with open(config_path) as f:
        return yaml.safe_load(f)


def build_output_path(output_dir: Path, start_month: str, end_month: str) -> Path:
    """Build the local output path for punctuality extracts."""
    return output_dir / f"punctuality_{start_month}_{end_month}.json"


def write_output(
    records: list[dict],
    output_path: Path,
    gcs_bucket: str | None = None,
) -> Path | str:
    """Write extracted rows to local JSON or upload NDJSON to GCS."""
    if gcs_bucket:
        blob_path = f"punctuality/{output_path.name}"
        ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in records)
        storage.Client().bucket(gcs_bucket).blob(blob_path).upload_from_string(
            ndjson, content_type="application/json"
        )
        return f"gs://{gcs_bucket}/{blob_path}"

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)
    return output_path


def extract_punctuality(
    start_date: str,
    end_date: str,
    output_dir: str | Path | None = None,
    gcs_bucket: str | None = None,
):
    """
    Extract Transilien monthly punctuality data between two dates.

    Args:
        start_date: Start date (YYYY-MM or YYYY-MM-DD — day part is ignored)
        end_date: End date (YYYY-MM or YYYY-MM-DD — day part is ignored)
        output_dir: Local directory for JSON output (used by tests/local fallback)
        gcs_bucket: GCS bucket name (default: GCS_BUCKET_RAW env var if no output_dir)
    """
    config = load_config()
    transilien_config = config["transilien"]
    dataset_config = transilien_config["datasets"]["punctuality"]

    client = ODSv2Client(
        base_url=transilien_config["base_url"], dataset_id=dataset_config["id"]
    )

    # Date field is YYYY-MM format — truncate if full date passed
    date_field = dataset_config["filters"]["date_field"]
    start_month = start_date[:7]
    end_month = end_date[:7]
    where_clause = f"{date_field} >= '{start_month}' AND {date_field} <= '{end_month}'"

    fields = dataset_config["fields"]
    select_clause = ", ".join(fields.values())

    logger.info(f"Extracting punctuality from {start_month} to {end_month}")
    logger.info(f"Dataset: {dataset_config['id']}")

    records = client.get_all_records(
        where=where_clause, select=select_clause, order_by=f"{date_field} ASC"
    )

    if not records:
        logger.warning("No records found for the specified date range")
        return

    # FIXED V2: fields at ROOT level — do not use client.extract_fields()
    extracted = []
    for record in records:
        extracted_record = {
            target: record.get(source) for target, source in fields.items()
        }
        # FIXED V2: datetime.utcnow() deprecated in Python 3.12+
        extracted_record["ingestion_ts"] = datetime.now(timezone.utc).isoformat()
        extracted_record["source"] = "transilien_punctuality"
        extracted.append(extracted_record)

    bucket_name = (
        gcs_bucket
        if gcs_bucket is not None
        else (None if output_dir is not None else os.getenv("GCS_BUCKET_RAW"))
    )
    local_output_dir = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR
    result = write_output(
        extracted,
        build_output_path(local_output_dir, start_month, end_month),
        gcs_bucket=bucket_name,
    )
    logger.info("Saved %d punctuality records to %s", len(extracted), result)


def main():
    parser = argparse.ArgumentParser(
        description="Extract Transilien monthly punctuality data",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python extract_ponctuality.py --start 2024-01 --end 2024-12
  python extract_ponctuality.py --start 2024-01-01 --end 2024-12-31
        """,
    )
    parser.add_argument(
        "--start", required=True, help="Start month (YYYY-MM or YYYY-MM-DD)"
    )
    parser.add_argument(
        "--end", required=True, help="End month (YYYY-MM or YYYY-MM-DD)"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Local output directory (default: PROJECT_ROOT/data/bronze/punctuality)",
    )
    parser.add_argument(
        "--bucket",
        default=None,
        help="GCS bucket name (default: GCS_BUCKET_RAW env var)",
    )

    args = parser.parse_args()
    extract_punctuality(
        args.start,
        args.end,
        output_dir=args.output,
        gcs_bucket=args.bucket,
    )


if __name__ == "__main__":
    main()
