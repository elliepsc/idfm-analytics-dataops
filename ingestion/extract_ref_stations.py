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

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))
BASE_URL = "https://data.iledefrance-mobilites.fr/api/explore/v2.1"
DATASET_ID = "emplacement-des-gares-idf"


def extract_ref_stations(output_dir: Path = None) -> Path:
    """Extract all stations with id_ref_zdc + coordinates from IDFM API."""
    if output_dir is None:
        output_dir = PROJECT_ROOT / "ingestion" / "data" / "bronze" / "referentials"
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

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

    output_file = output_dir / f"ref_stations_{datetime.now().strftime('%Y%m%d')}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    logger.info("Extracted %d stations → %s", len(all_records), output_file)
    return output_file


if __name__ == "__main__":
    extract_ref_stations()
