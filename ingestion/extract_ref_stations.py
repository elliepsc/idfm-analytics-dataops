"""
Extract station reference data from IDFM API.

Dataset: emplacement-des-gares-idf
Key field: id_ref_zdc — matches 'ida' field in validations dataset
Provides: station name + coordinates for Looker Studio map

Usage:
    python ingestion/extract_ref_stations.py
"""

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))
BASE_URL = "https://data.iledefrance-mobilites.fr/api/explore/v2.1"
DATASET_ID = "emplacement-des-gares-idf"


def extract_ref_stations(gcs_bucket: str = None) -> str:
    """Extract all stations with id_ref_zdc + coordinates from IDFM API."""
    bucket_name = gcs_bucket or os.getenv("GCS_BUCKET_RAW")

    url = f"{BASE_URL}/catalog/datasets/{DATASET_ID}/records"
    params: dict[str, str | int] = {
        "select": "id_ref_zdc,nom_gares,geo_point_2d,mode,exploitant",
        "limit": 100,
        "offset": 0,
    }

    all_records: list[dict] = []
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    while True:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()

        results = data.get("results", [])
        if not results:
            break

        for r in results:
            geo = r.get("geo_point_2d", {})
            all_records.append(
                {
                    "id_ref_zdc": r.get("id_ref_zdc"),
                    "station_name": r.get("nom_gares"),
                    "latitude": geo.get("lat"),
                    "longitude": geo.get("lon"),
                    "mode": r.get("mode"),
                    "operator": r.get("exploitant"),
                    "source": "idfm_ref_stations",
                    "ingestion_ts": ingestion_ts,
                }
            )

        total = data.get("total_count", 0)
        params["offset"] = int(params["offset"]) + len(results)
        logger.info("Fetched %d / %d stations", len(all_records), total)

        if int(params["offset"]) >= int(total):
            break

    blob_path = f"referentials/ref_stations_{datetime.now().strftime('%Y%m%d')}.json"
    ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in all_records)
    storage.Client().bucket(bucket_name).blob(blob_path).upload_from_string(
        ndjson, content_type="application/json"
    )
    gcs_uri = f"gs://{bucket_name}/{blob_path}"
    logger.info("Extracted %d stations → %s", len(all_records), gcs_uri)
    return gcs_uri


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Extract IDFM stations referential")
    parser.add_argument(
        "--bucket",
        default=None,
        help="GCS bucket name (default: GCS_BUCKET_RAW env var)",
    )
    args = parser.parse_args()
    extract_ref_stations(args.bucket)
