"""
Extract stop-to-line mapping from IDFM GTFS feed.

Why GTFS and not the ODS API?
  The arrets-lignes dataset on the IDFM ODS API does not contain a usable
  line_id field and is capped at 10,000 records out of 73,264 available.
  The GTFS feed (offre-horaires-tc-gtfs-idfm) is the authoritative source
  for stop to route mapping and covers the full IDF network.

GTFS files used:
  trips.txt      -> route_id (= line_id in IDFM nomenclature), 62 MB
  stop_times.txt -> links stop_id to trip_id -> route_id, 1.1 GB uncompressed

Memory strategy:
  trips.txt is loaded fully into a dict (trip_id -> route_id).
  stop_times.txt is streamed line by line from the compressed ZIP to avoid OOM.
  Validated on WSL: 11.7M rows -> 73,725 unique (stop_id, line_id) pairs in ~2 min.

How the file_id is resolved:
  The GTFS ZIP file_id changes with each IDFM update (3x/day at 8h, 13h, 17h).
  We query the dataset records endpoint first to get the current file_id,
  then download the ZIP using that ID.

Output:
  One JSON record per unique (stop_id, line_id) pair.
  Loaded into BigQuery as transport_raw.raw_ref_stop_lines (WRITE_TRUNCATE).

Usage:
    IDFM_API_KEY=your_key python ingestion/extract_ref_stop_lines.py
    IDFM_API_KEY=your_key python ingestion/extract_ref_stop_lines.py --output /tmp/data
"""

import argparse
import io
import json
import logging
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

IDFM_BASE_URL = "https://data.iledefrance-mobilites.fr/api/explore/v2.1"
GTFS_DATASET_ID = "offre-horaires-tc-gtfs-idfm"

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))


def get_gtfs_file_id(api_key: str) -> tuple:
    """
    Query the dataset records to get the current GTFS ZIP file_id.
    The file_id changes with each IDFM update (3x/day).

    Returns (file_id, filename).
    """
    url = f"{IDFM_BASE_URL}/catalog/datasets/{GTFS_DATASET_ID}/records"
    params = {"limit": 1, "apikey": api_key}

    logger.info("Resolving current GTFS file_id from dataset records...")
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    data = response.json()
    results = data.get("results", [])

    if not results:
        raise ValueError("No records found in GTFS dataset")

    file_id = results[0]["url"]["id"]
    filename = results[0]["url"]["filename"]
    logger.info(f"Current GTFS file: {filename} (id={file_id})")
    return file_id, filename


def download_gtfs(api_key: str, file_id: str) -> bytes:
    """Download GTFS ZIP using the resolved file_id."""
    url = f"{IDFM_BASE_URL}/catalog/datasets/{GTFS_DATASET_ID}/files/{file_id}"
    params = {"apikey": api_key}

    logger.info(f"Downloading GTFS ZIP (file_id={file_id})...")
    response = requests.get(url, params=params, timeout=300, stream=True)
    response.raise_for_status()

    chunks = []
    total = 0
    for chunk in response.iter_content(chunk_size=1024 * 1024):
        chunks.append(chunk)
        total += len(chunk)
        if total % (20 * 1024 * 1024) == 0:
            logger.info(f"  Downloaded {total / 1024 / 1024:.0f} MB...")

    content = b"".join(chunks)
    logger.info(f"Download complete: {len(content) / 1024 / 1024:.1f} MB")
    return content


def load_trips(zip_bytes: bytes) -> dict:
    """
    Load trips.txt fully into a trip_id -> route_id dict.
    trips.txt is 62 MB uncompressed — safe to load entirely.
    """
    import csv

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        with zf.open("trips.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            trip_to_route = {
                row["trip_id"]: row["route_id"]
                for row in reader
                if row.get("trip_id") and row.get("route_id")
            }

    logger.info(f"Indexed {len(trip_to_route):,} trips -> routes")
    return trip_to_route


def stream_stop_line_pairs(zip_bytes: bytes, trip_to_route: dict) -> set:
    """
    Stream stop_times.txt line by line from the compressed ZIP.

    stop_times.txt is 1.1 GB uncompressed — must be streamed to avoid OOM.
    Reads directly from the ZipFile object without full decompression.

    Returns set of (stop_id, line_id) tuples.
    """
    import csv

    pairs = set()
    count = 0

    logger.info("Streaming stop_times.txt (1.1 GB uncompressed)...")
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        with zf.open("stop_times.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                count += 1
                trip_id = row.get("trip_id", "").strip()
                stop_id = row.get("stop_id", "").strip()

                if trip_id and stop_id:
                    route_id = trip_to_route.get(trip_id)
                    if route_id:
                        pairs.add((stop_id.upper(), route_id.upper()))

                if count % 1_000_000 == 0:
                    logger.info(
                        f"  {count:,} rows processed, " f"{len(pairs):,} pairs so far"
                    )

    logger.info(
        f"Streaming complete: {count:,} rows -> "
        f"{len(pairs):,} unique (stop_id, line_id) pairs"
    )
    return pairs


def extract_ref_stop_lines(output_dir: Path = None) -> Path:
    """
    Main entry point: resolve file_id, download GTFS, parse, write JSON.

    Returns path to the output JSON file.
    """
    if output_dir is None:
        output_dir = (
            PROJECT_ROOT
            / "orchestration"
            / "airflow"
            / "data"
            / "bronze"
            / "referentials"
        )

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    api_key = os.getenv("IDFM_API_KEY")
    if not api_key:
        raise ValueError(
            "IDFM_API_KEY environment variable not set. "
            "Register at https://data.iledefrance-mobilites.fr to get a key. "
            "Add IDFM_API_KEY to the root .env file used by local scripts and Airflow."
        )

    # Step 1: Resolve current file_id (changes 3x/day)
    file_id, filename = get_gtfs_file_id(api_key)

    # Step 2: Download ZIP (~140 MB compressed)
    gtfs_bytes = download_gtfs(api_key, file_id)

    # Step 3: Load trips.txt (62 MB — safe to load fully)
    trip_to_route = load_trips(gtfs_bytes)

    # Step 4: Stream stop_times.txt (1.1 GB — must stream)
    pairs = stream_stop_line_pairs(gtfs_bytes, trip_to_route)

    # Step 5: Write newline-delimited JSON
    ingestion_ts = datetime.now(timezone.utc).isoformat()
    records = [
        {
            "stop_id": stop_id,
            "line_id": line_id,
            "ingestion_ts": ingestion_ts,
            "source": "gtfs_idfm",
        }
        for stop_id, line_id in sorted(pairs)
    ]

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_path = output_dir / f"ref_stop_lines_{timestamp}.json"

    # Write as JSON array — consistent with all other extractors in this project.
    # load_bigquery_raw.py expects [{...}, {...}] format (converted to NDJSON in memory).
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    logger.info(f"Written {len(records):,} records to {output_path}")

    # Step 6: Extract TN_PA stop ID mapping (STIF code -> IDFM stop_id)
    # Bridges fct_validations_daily (STIF stop_ids) with stg_ref_stop_lines (IDFM stop_ids)
    extract_stop_id_mapping(gtfs_bytes, output_dir)

    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Extract IDFM stop-to-line mapping from GTFS feed"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="Output directory for JSON file (default: data/bronze/referentials)",
    )
    args = parser.parse_args()
    extract_ref_stop_lines(output_dir=args.output)


def extract_stop_id_mapping(zip_bytes: bytes, output_dir: Path) -> Path:
    """
    Extract TN_PA stop ID mapping from object_codes_extension.txt.

    TN_PA codes are the STIF numeric stop IDs used in IDFM validation data.
    This mapping bridges validations (STIF stop_id) → GTFS (IDFM stop_id).

    object_codes_extension.txt structure:
      object_type | object_id    | object_system | object_code
      stop_area   | IDFM:493509  | TN_PA         | 161

    Where:
      object_id   = IDFM GTFS stop_id (matches stg_ref_stop_lines.stop_id)
      object_code = STIF numeric code (matches fct_validations_daily.stop_id)
    """
    import csv

    logger.info("Extracting TN_PA stop ID mapping from object_codes_extension.txt...")
    mappings = []

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        with zf.open("object_codes_extension.txt") as f:
            reader = csv.DictReader(io.TextIOWrapper(f, encoding="utf-8-sig"))
            for row in reader:
                if row.get("object_system") == "TN_PA":
                    mappings.append(
                        {
                            "idfm_stop_id": row["object_id"],
                            "stif_stop_code": row["object_code"],
                        }
                    )

    logger.info(f"Found {len(mappings):,} TN_PA mappings (STIF → IDFM stop_id)")

    ingestion_ts = datetime.now(timezone.utc).isoformat()
    records = [
        {
            "idfm_stop_id": m["idfm_stop_id"],
            "stif_stop_code": m["stif_stop_code"],
            "ingestion_ts": ingestion_ts,
            "source": "gtfs_object_codes_tnpa",
        }
        for m in mappings
    ]

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    output_path = output_dir / f"ref_stop_id_mapping_{timestamp}.json"

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2, ensure_ascii=False)

    logger.info(f"Written {len(records):,} records to {output_path}")
    return output_path
