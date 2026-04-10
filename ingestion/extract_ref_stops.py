"""
Extract reference data: stops (arrets-lignes) from the full IDFM export.

Why not use the /records endpoint?
  The ODS /records endpoint is capped at 10,000 rows for this dataset.
  The /exports/json endpoint returns the full arrets-lignes snapshot and
  includes the transport mode, operator, and route metadata we need.

Default behavior:
  - Download the full dataset from /exports/json
  - Normalize rows to the Bronze JSON shape used by the pipeline
  - Save `ref_stops_<timestamp>.json` for BigQuery RAW loading

Optional behavior:
  - Filter on transport modes, e.g. Metro / RER
  - Save a side snapshot as Parquet for local analytics work

Usage:
    python ingestion/extract_ref_stops.py
    python ingestion/extract_ref_stops.py --transport-modes Metro RER --format parquet
    python ingestion/extract_ref_stops.py --transport-modes Metro RER --basename stops_metro_rer
"""

import argparse
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests
import yaml
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

load_dotenv()

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).parent.parent))
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "bronze" / "referentials"


def load_config() -> dict:
    """Load API configuration from YAML file."""
    config_path = PROJECT_ROOT / "config" / "apis.yml"
    with open(config_path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def download_stops_export(api_key: str, dataset_id: str, base_url: str) -> list[dict]:
    """Download the full arrets-lignes export as JSON."""
    if not api_key:
        raise ValueError(
            "IDFM_API_KEY environment variable not set. "
            "The full arrets-lignes export requires an IDFM API key."
        )

    url = f"{base_url.rstrip('/')}/catalog/datasets/{dataset_id}/exports/json"
    logger.info("Downloading full stops export from %s", url)

    response = requests.get(url, params={"apikey": api_key}, timeout=300)
    response.raise_for_status()

    payload = response.json()
    if not isinstance(payload, list):
        raise ValueError(
            "Unexpected arrets-lignes export payload: expected a JSON array."
        )

    logger.info("Downloaded %s raw stop rows", f"{len(payload):,}")
    return payload


def normalize_stop_records(
    records: list[dict],
    fields: dict[str, str],
    transport_modes: list[str] | None = None,
) -> list[dict]:
    """Map raw export rows to the Bronze schema used by the project."""
    allowed_modes = {mode.strip().upper() for mode in (transport_modes or []) if mode}
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    normalized = []
    for record in records:
        transport_mode = (record.get(fields["transport_mode"]) or "").strip()
        if allowed_modes and transport_mode.upper() not in allowed_modes:
            continue

        normalized_record = {
            target: record.get(source) for target, source in fields.items()
        }
        normalized_record["transport_mode"] = (
            transport_mode.upper() if transport_mode else None
        )
        normalized_record["latitude"] = safe_float(normalized_record.get("latitude"))
        normalized_record["longitude"] = safe_float(normalized_record.get("longitude"))
        normalized_record["ingestion_ts"] = ingestion_ts
        normalized_record["source"] = "idfm_ref_stops_export"
        normalized.append(normalized_record)

    return normalized


def safe_float(value):
    """Return a float when possible, else None."""
    if value in (None, ""):
        return None

    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def build_output_path(
    output_dir: Path,
    basename: str,
    output_format: str,
    transport_modes: list[str] | None = None,
) -> Path:
    """Build an output filename that stays compatible with the current pipeline."""
    effective_basename = basename
    if transport_modes and basename == "ref_stops":
        # Avoid matching load_bigquery_raw.py glob pattern ref_stops_*.json
        # when the user generates a filtered side snapshot.
        effective_basename = "ref_stops-filtered"

    suffix = ""
    if transport_modes:
        modes_slug = "_".join(sorted(mode.strip().lower() for mode in transport_modes))
        suffix = f"_{modes_slug}"

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    extension = "json" if output_format == "json" else "parquet"
    return output_dir / f"{effective_basename}{suffix}_{timestamp}.{extension}"


def write_output(records: list[dict], output_path: Path, output_format: str) -> Path:
    """Write normalized records to JSON or Parquet."""
    if output_format == "json":
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(records, f, indent=2, ensure_ascii=False)
        return output_path

    dataframe = pd.DataFrame(records)
    dataframe.to_parquet(output_path, index=False)
    return output_path


def extract_ref_stops(
    output_dir: Path | None = None,
    output_format: str = "json",
    transport_modes: list[str] | None = None,
    basename: str = "ref_stops",
) -> Path:
    """
    Extract the full arrets-lignes referential from the IDFM export endpoint.

    Args:
        output_dir: Target directory for the extracted file.
        output_format: json or parquet.
        transport_modes: Optional filter, e.g. ['Metro', 'RER'].
        basename: File prefix. Keep default `ref_stops` for pipeline compatibility.
    """
    config = load_config()
    idfm_config = config["idfm"]
    dataset_config = idfm_config["datasets"]["ref_stops"]

    output_path = Path(output_dir) if output_dir else DEFAULT_OUTPUT_DIR
    output_path.mkdir(parents=True, exist_ok=True)

    records = download_stops_export(
        api_key=os.getenv("IDFM_API_KEY"),
        dataset_id=dataset_config["id"],
        base_url=idfm_config["base_url"],
    )

    normalized = normalize_stop_records(
        records=records,
        fields=dataset_config["fields"],
        transport_modes=transport_modes,
    )

    final_output = build_output_path(
        output_dir=output_path,
        basename=basename,
        output_format=output_format,
        transport_modes=transport_modes,
    )
    write_output(normalized, final_output, output_format)

    logger.info("Saved %s normalized stops to %s", f"{len(normalized):,}", final_output)
    return final_output


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract IDFM stops referential")
    parser.add_argument(
        "--output",
        default=None,
        help="Output directory (default: PROJECT_ROOT/data/bronze/referentials)",
    )
    parser.add_argument(
        "--format",
        dest="output_format",
        choices=["json", "parquet"],
        default="json",
        help="Output file format. JSON stays compatible with the BigQuery RAW loader.",
    )
    parser.add_argument(
        "--transport-modes",
        nargs="+",
        default=None,
        help="Optional transport mode filter, e.g. Metro RER",
    )
    parser.add_argument(
        "--basename",
        default="ref_stops",
        help="Output filename prefix (default: ref_stops)",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    extract_ref_stops(
        output_dir=args.output,
        output_format=args.output_format,
        transport_modes=args.transport_modes,
        basename=args.basename,
    )


if __name__ == "__main__":
    main()
