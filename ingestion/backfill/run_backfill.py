"""
run_backfill.py
---------------
Manifest-driven orchestrator for IDFM historical data backfill.

Reads backfill_sources.yml, automatically downloads and extracts ZIP files
if needed, then processes each pending source and loads it into BigQuery.

Usage:
    # Dry run - download, parse all files, no BigQuery writes
    python run_backfill.py --dry-run

    # Load all pending sources (downloads ZIPs automatically)
    python run_backfill.py --base-dir "/mnt/c/Users/Ellie Pro/Downloads"

    # Load a single period only
    python run_backfill.py --base-dir "/mnt/c/Users/Ellie Pro/Downloads" --period 2023-S1

    # Re-run an already-loaded source (force)
    python run_backfill.py --base-dir "/mnt/c/Users/Ellie Pro/Downloads" --period 2023-S1 --force
"""

import argparse
import logging
import sys
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import requests
import yaml

# Import sibling modules
sys.path.insert(0, str(Path(__file__).parent))
from load_backfill_bq import load_to_bigquery  # noqa: E402
from parse_csv_historical import parse_file  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

MANIFEST_PATH = Path(__file__).parent / "backfill_sources.yml"

# IDFM catalog API — returns one record per year with ZIP download URL
IDFM_CATALOG_URL = (
    "https://data.iledefrance-mobilites.fr/api/explore/v2.1/"
    "catalog/datasets/histo-validations-reseau-ferre/records?limit=20"
)


def resolve_zip_url(year: str) -> str:
    """
    Dynamically resolve the ZIP download URL for a given year
    from the IDFM catalog API.

    This avoids hardcoding file hashes that change when IDFM updates their data.
    The API returns one record per year with the current download URL.

    Args:
        year: e.g. "2023" or "2024"

    Returns:
        Download URL for the ZIP file of that year.

    Raises:
        ValueError: if no ZIP is found for the given year.
    """
    logger.info(f"Resolving ZIP URL for year {year} from IDFM catalog API...")
    response = requests.get(IDFM_CATALOG_URL, timeout=30)
    response.raise_for_status()

    for record in response.json().get("results", []):
        if record.get("annee") == year:
            url = record["reseau_ferre"]["url"]
            logger.info(f"Resolved URL for {year}: {url}")
            return url

    raise ValueError(
        f"No ZIP found for year {year} in IDFM catalog. "
        f"Available years: {[r.get('annee') for r in response.json().get('results', [])]}"
    )


def load_manifest() -> dict:
    """Load the backfill manifest from YAML."""
    with open(MANIFEST_PATH, encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_manifest(manifest: dict) -> None:
    """Save updated manifest back to YAML."""
    with open(MANIFEST_PATH, "w", encoding="utf-8") as f:
        yaml.dump(
            manifest, f, allow_unicode=True, sort_keys=False, default_flow_style=False
        )
    logger.info(f"Manifest updated: {MANIFEST_PATH}")


def download_file(url: str, dest_path: Path) -> None:
    """
    Download any file from URL to dest_path.
    Shows progress every 10MB.
    Works for both ZIP and CSV files.
    """
    logger.info(f"Downloading: {url}")
    logger.info(f"Destination: {dest_path}")

    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    total = int(response.headers.get("content-length", 0))
    downloaded = 0
    chunk_size = 1024 * 1024  # 1MB chunks

    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=chunk_size):
            if chunk:
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = downloaded / total * 100
                    if downloaded % (10 * 1024 * 1024) < chunk_size:
                        logger.info(
                            f"  Progress: {pct:.0f}% ({downloaded // 1024 // 1024}MB)"
                        )

    logger.info(f"Download complete: {dest_path.name} ({downloaded // 1024 // 1024}MB)")


def extract_zip(zip_path: Path, extract_dir: Path) -> None:
    """Extract a ZIP file to extract_dir."""
    logger.info(f"Extracting: {zip_path.name} -> {extract_dir}")
    with zipfile.ZipFile(zip_path, "r") as zf:
        zf.extractall(extract_dir)
    logger.info(f"Extraction complete: {extract_dir}")


def ensure_file_available(source: dict, base_dir: Path) -> Path:
    """
    Ensure the source file is available locally.
    Downloads and extracts if needed.

    source_type controls the download strategy:
      - "zip"        (default): download ZIP, extract, find file inside
      - "csv_direct": download CSV directly (no ZIP)

    zip_url can be:
      - "dynamic": resolved at runtime from IDFM catalog API using source["year"]
      - a direct URL: used as-is
      - null: file must already exist locally (raises FileNotFoundError if missing)

    Returns the resolved local file path.
    """
    filepath = base_dir / source["file"]

    if filepath.exists():
        logger.info(f"File already available: {filepath.name}")
        return filepath

    source_type = source.get("source_type", "zip")
    zip_url = source.get("zip_url")
    zip_name = source.get("zip_name")

    # Resolve dynamic URL at runtime
    if zip_url == "dynamic":
        year = source.get("year")
        if not year:
            raise ValueError(
                f"source_type is 'zip' with zip_url='dynamic' but no 'year' field: {source['period']}"
            )
        zip_url = resolve_zip_url(year)

    if not zip_url:
        raise FileNotFoundError(
            f"File not found and no zip_url configured: {filepath}\n"
            f"Please download manually or add zip_url to backfill_sources.yml"
        )

    # CSV direct — download file directly without ZIP extraction
    if source_type == "csv_direct":
        logger.info(f"Downloading CSV directly: {filepath.name}")
        download_file(zip_url, filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"File still not found after download: {filepath}")
        return filepath

    # ZIP flow — download ZIP then extract
    zip_path = base_dir / zip_name
    if not zip_path.exists():
        logger.info(f"ZIP not found locally, downloading: {zip_name}")
        download_file(zip_url, zip_path)
    else:
        logger.info(f"ZIP already downloaded: {zip_name}")

    extract_zip(zip_path, base_dir)

    if not filepath.exists():
        raise FileNotFoundError(
            f"File still not found after extraction: {filepath}\n"
            f"Check that zip_name and file path are correct in backfill_sources.yml"
        )

    return filepath


def process_source(
    source: dict,
    base_dir: Path,
    dry_run: bool = False,
) -> dict:
    """
    Ensure, parse, and load a single source file.
    Returns updated source dict with loaded status.
    """
    logger.info(f"--- Processing: {source['description']} ({source['period']}) ---")

    # Step 1: Ensure file is available (download if needed)
    filepath = ensure_file_available(source, base_dir)

    # Step 2: Parse CSV into normalized DataFrame
    df = parse_file(filepath)

    # Step 3: Load to BigQuery via staging + MERGE
    summary = load_to_bigquery(df, dry_run=dry_run)

    if not dry_run:
        source["loaded"] = True
        source["loaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        logger.info(
            f"✅ {source['period']}: {summary['rows_inserted']} rows inserted "
            f"({summary['rows_input'] - summary['rows_inserted']} already existed)"
        )
    else:
        logger.info(
            f"[DRY RUN] {source['period']}: {summary['rows_input']} rows would be processed"
        )

    return source


def run(
    base_dir: str,
    dry_run: bool,
    period: str = None,
    force: bool = False,
) -> None:
    """
    Main orchestration loop.

    Args:
        base_dir : root directory for downloaded files
        dry_run  : if True, skip BigQuery writes
        period   : if set, only process this specific period (e.g. '2023-S1')
        force    : if True, re-process already-loaded sources
    """
    base_dir = Path(base_dir)
    manifest = load_manifest()
    sources = manifest["sources"]

    # Filter sources to process
    pending = []
    for source in sources:
        if period and source["period"] != period:
            continue
        if source["loaded"] and not force:
            logger.info(
                f"Skipping {source['period']} (already loaded on {source['loaded_at']})"
            )
            continue
        pending.append(source)

    if not pending:
        logger.info("No pending sources to process.")
        return

    logger.info(f"Sources to process: {len(pending)}")

    results = {"success": [], "failed": []}

    for source in pending:
        try:
            updated_source = process_source(source, base_dir, dry_run=dry_run)

            # Update manifest in memory
            for i, s in enumerate(manifest["sources"]):
                if s["period"] == source["period"]:
                    manifest["sources"][i] = updated_source

            results["success"].append(source["period"])

            # Save manifest after each success (safe against partial failures)
            if not dry_run:
                save_manifest(manifest)

        except FileNotFoundError as e:
            logger.error(f"❌ {source['period']}: File not found — {e}")
            results["failed"].append(source["period"])

        except Exception as e:
            logger.error(f"❌ {source['period']}: Load failed — {e}")
            results["failed"].append(source["period"])

    # Final summary
    print(f"\n{'='*50}")
    print(f"BACKFILL SUMMARY {'[DRY RUN]' if dry_run else ''}")
    print(f"{'='*50}")
    print(f"✅ Success : {len(results['success'])} — {results['success']}")
    print(f"❌ Failed  : {len(results['failed'])} — {results['failed']}")
    print(f"{'='*50}\n")

    if results["failed"]:
        sys.exit(1)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Manifest-driven backfill orchestrator for IDFM historical data"
    )
    parser.add_argument(
        "--base-dir",
        default="/mnt/c/Users/Ellie Pro/Downloads",
        help="Root directory for downloaded source files",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate all files without writing to BigQuery",
    )
    parser.add_argument(
        "--period",
        help="Process a specific period only (e.g. 2023-S1, 2024-T3)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-process sources already marked as loaded",
    )
    args = parser.parse_args()

    run(
        base_dir=args.base_dir,
        dry_run=args.dry_run,
        period=args.period,
        force=args.force,
    )
