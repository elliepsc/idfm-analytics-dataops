"""
Extract stop-to-line mapping from IDFM GTFS feed.

Why GTFS and not the ODS API?
  The arrets-lignes dataset on the IDFM ODS API does not contain a usable
  line_id field and is capped at 10,000 records out of 73,264 available.
  The GTFS feed (offre-horaires-tc-gtfs-idfm) is the authoritative source
  for stop → route mapping and covers the full network.

GTFS files used:
  stops.txt    → stop_id
  trips.txt    → route_id (= line_id in IDFM nomenclature)
  stop_times.txt → links stop_id to trip_id → route_id

Output:
  One JSON record per unique (stop_id, line_id) pair.
  Loaded into BigQuery as transport_raw.raw_ref_stop_lines.

Usage:
    python ingestion/extract_ref_stop_lines.py
    python ingestion/extract_ref_stop_lines.py --output /tmp/data
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

# GTFS feed URL — IDFM open data, updated daily
# Source: https://data.iledefrance-mobilites.fr/explore/dataset/offre-horaires-tc-gtfs-idfm
GTFS_URL = (
    "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets"
    "/offre-horaires-tc-gtfs-idfm/exports/files"
)

# Fallback direct download URL (stable CDN link)
GTFS_DIRECT_URL = (
    "https://eu.ftp.opendatasoft.com/sncf/gtfs/export-intercites-gtfs-last.zip"
)

# IDFM GTFS actual URL (confirmed working)
GTFS_IDFM_URL = (
    "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets"
    "/offre-horaires-tc-gtfs-idfm/exports/files?apikey={api_key}"
)

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))


def download_gtfs(api_key: str = None) -> bytes:
    """Download GTFS zip from IDFM open data portal."""
    url = GTFS_URL
    headers = {}

    if api_key:
        headers["Authorization"] = f"Apikey {api_key}"

    logger.info(f"Downloading GTFS feed from {url}")
    response = requests.get(url, headers=headers, timeout=120, stream=True)

    if response.status_code == 401:
        raise ValueError(
            "GTFS download requires an IDFM API key. "
            "Set IDFM_API_KEY in your .env file. "
            "Register at: https://data.iledefrance-mobilites.fr"
        )

    response.raise_for_status()

    content = response.content
    logger.info(f"Downloaded {len(content) / 1024 / 1024:.1f} MB")
    return content


def parse_csv_from_zip(zip_bytes: bytes, filename: str) -> list[dict]:
    """Extract and parse a CSV file from a GTFS zip archive."""
    import csv

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        # GTFS files can be at root or in a subdirectory
        available = zf.namelist()
        target = next(
            (f for f in available if f.endswith(filename)),
            None,
        )

        if target is None:
            raise FileNotFoundError(
                f"{filename} not found in GTFS zip. " f"Available files: {available}"
            )

        logger.info(f"Parsing {target} from GTFS zip")

        with zf.open(target) as f:
            content = f.read().decode("utf-8-sig")  # handle BOM if present
            reader = csv.DictReader(io.StringIO(content))
            return list(reader)


def build_stop_line_mapping(
    trips: list[dict],
    stop_times: list[dict],
) -> list[dict]:
    """
    Build stop → line mapping from GTFS trips and stop_times.

    GTFS join logic:
      stop_times.trip_id → trips.trip_id → trips.route_id (= line_id)

    Returns deduplicated list of (stop_id, line_id) pairs.
    """
    # Build trip_id → route_id index
    trip_to_route = {
        row["trip_id"]: row["route_id"]
        for row in trips
        if row.get("trip_id") and row.get("route_id")
    }

    logger.info(f"Indexed {len(trip_to_route):,} trips → routes")

    # Build unique stop_id → set of line_ids
    stop_line_pairs: set[tuple] = set()
    skipped = 0

    for row in stop_times:
        trip_id = row.get("trip_id", "").strip()
        stop_id = row.get("stop_id", "").strip()

        if not trip_id or not stop_id:
            skipped += 1
            continue

        route_id = trip_to_route.get(trip_id)
        if route_id:
            stop_line_pairs.add((stop_id.upper(), route_id.upper()))

    logger.info(
        f"Built {len(stop_line_pairs):,} unique (stop_id, line_id) pairs "
        f"(skipped {skipped:,} rows with missing ids)"
    )

    return [
        {"stop_id": stop_id, "line_id": line_id}
        for stop_id, line_id in sorted(stop_line_pairs)
    ]


def extract_ref_stop_lines(output_dir: Path = None) -> Path:
    """
    Main entry point: download GTFS, parse, build mapping, write JSON.

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
        logger.warning(
            "IDFM_API_KEY not set — attempting unauthenticated download. "
            "This may fail. Set IDFM_API_KEY in .env for reliable access."
        )

    # Download GTFS
    gtfs_bytes = download_gtfs(api_key=api_key)

    # Parse required files
    logger.info("Parsing trips.txt")
    trips = parse_csv_from_zip(gtfs_bytes, "trips.txt")
    logger.info(f"  → {len(trips):,} trips")

    logger.info("Parsing stop_times.txt")
    stop_times = parse_csv_from_zip(gtfs_bytes, "stop_times.txt")
    logger.info(f"  → {len(stop_times):,} stop_times rows")

    # Build mapping
    pairs = build_stop_line_mapping(trips, stop_times)

    # Serialise to JSON (same format as other extractors)
    ingestion_ts = datetime.now(timezone.utc).isoformat()
    records = [
        {
            "stop_id": p["stop_id"],
            "line_id": p["line_id"],
            "ingestion_ts": ingestion_ts,
            "source": "gtfs_idfm",
        }
        for p in pairs
    ]

    output_path = (
        output_dir
        / f"ref_stop_lines_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}.json"
    )
    with open(output_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    logger.info(f"✅ Written {len(records):,} records to {output_path}")
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
