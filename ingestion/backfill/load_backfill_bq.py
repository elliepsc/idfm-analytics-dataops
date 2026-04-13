"""
load_backfill_bq.py
-------------------
Load a normalized DataFrame into BigQuery raw_validations table
using a MERGE strategy to prevent duplicates.

Strategy:
  1. Load DataFrame into a temporary staging table
  2. MERGE staging -> raw_validations on natural key (jour, code_stif_arret, categorie_titre)
  3. Drop staging table

This makes the load idempotent: safe to re-run without creating duplicates.

Usage:
    from load_backfill_bq import load_to_bigquery
    load_to_bigquery(df, project_id=os.getenv("GCP_PROJECT_ID"))
"""

import logging
import os
from datetime import datetime, timezone

import pandas as pd
from google.cloud import bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# BigQuery config
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "idfm-analytics-dev-488611")
DATASET_RAW = os.getenv("BQ_DATASET_RAW", "transport_raw")
TABLE_TARGET = "raw_validations"
LOCATION = os.getenv("GCP_REGION", "europe-west1")

# Natural key for deduplication (BigQuery column names)
MERGE_KEY_COLS = ["date", "stop_id", "ticket_type"]

# All columns to insert (BigQuery schema)
ALL_COLUMNS = [
    "date",
    "line_code_trns",
    "line_code_res",
    "stop_id",
    "stop_name",
    "ticket_type",
    "validation_count",
    "source",
    "ingestion_ts",
]

# Mapping from raw source columns to BigQuery column names
COLUMN_RENAME = {
    "jour": "date",
    "code_stif_trns": "line_code_trns",
    "code_stif_res": "line_code_res",
    "code_stif_arret": "stop_id",
    "libelle_arret": "stop_name",
    "categorie_titre": "ticket_type",
    "nb_vald": "validation_count",
}


def _get_staging_table_id(project_id: str) -> str:
    """Generate a unique staging table name using current timestamp."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{project_id}.{DATASET_RAW}.raw_validations_staging_{ts}"


def _load_to_staging(
    client: bigquery.Client,
    df: pd.DataFrame,
    staging_table_id: str,
) -> None:
    """
    Load DataFrame into a temporary staging table.
    Table is created fresh (WRITE_TRUNCATE) with auto-detected schema.
    location is passed to load_table_from_dataframe, not to LoadJobConfig.
    """
    logger.info(f"Loading {len(df)} rows to staging table: {staging_table_id}")

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )

    job = client.load_table_from_dataframe(
        df,
        staging_table_id,
        job_config=job_config,
        location=LOCATION,  # location here, not in LoadJobConfig
    )
    job.result()

    logger.info(f"Staging table loaded: {staging_table_id}")


def _merge_into_target(
    client: bigquery.Client,
    staging_table_id: str,
    target_table_id: str,
) -> int:
    """
    MERGE staging table into raw_validations.
    Only inserts rows where the natural key does not already exist.
    Returns the number of rows inserted.
    """
    # Build ON clause from merge key columns
    on_clause = " AND ".join([f"T.{col} = S.{col}" for col in MERGE_KEY_COLS])

    # Build INSERT column list
    col_list = ", ".join(ALL_COLUMNS)
    val_list = ", ".join([f"S.{col}" for col in ALL_COLUMNS])

    merge_sql = f"""
    MERGE `{target_table_id}` AS T
    USING `{staging_table_id}` AS S
    ON (
        {on_clause}
    )
    WHEN NOT MATCHED THEN
        INSERT ({col_list})
        VALUES ({val_list})
    """

    logger.info("Running MERGE into target table...")
    logger.debug(f"MERGE SQL:\n{merge_sql}")

    job = client.query(merge_sql, location=LOCATION)
    job.result()  # Wait for completion

    # num_dml_affected_rows gives rows inserted by MERGE
    rows_inserted = job.num_dml_affected_rows
    logger.info(f"MERGE complete: {rows_inserted} rows inserted into {target_table_id}")

    return rows_inserted


def _drop_staging(client: bigquery.Client, staging_table_id: str) -> None:
    """Drop the temporary staging table."""
    client.delete_table(staging_table_id, not_found_ok=True)
    logger.info(f"Staging table dropped: {staging_table_id}")


def load_to_bigquery(
    df: pd.DataFrame,
    project_id: str = PROJECT_ID,
    dry_run: bool = False,
) -> dict:
    """
    Main entry point: load a normalized DataFrame into raw_validations
    using a staging + MERGE strategy.

    Args:
        df:         Normalized DataFrame from parse_csv_historical.parse_file()
        project_id: GCP project ID
        dry_run:    If True, skip actual BigQuery operations (for testing)

    Returns:
        dict with load summary (rows_input, rows_inserted, staging_table)
    """
    target_table_id = f"{project_id}.{DATASET_RAW}.{TABLE_TARGET}"
    staging_table_id = _get_staging_table_id(project_id)

    summary = {
        "rows_input": len(df),
        "rows_inserted": 0,
        "staging_table": staging_table_id,
        "target_table": target_table_id,
        "dry_run": dry_run,
    }

    if dry_run:
        logger.info(f"[DRY RUN] Would load {len(df)} rows to {target_table_id}")
        logger.info(f"[DRY RUN] Staging table would be: {staging_table_id}")
        return summary

    # Rename columns to match BigQuery schema
    df = df.rename(columns=COLUMN_RENAME)
    # Cast to match BigQuery schema types
    df["stop_id"] = df["stop_id"].astype(str)
    df["line_code_res"] = df["line_code_res"].astype(str)  # STRING in BQ
    df["source"] = "historical_backfill"
    df["ingestion_ts"] = datetime.now(timezone.utc)
    df = df[ALL_COLUMNS]

    client = bigquery.Client(project=project_id, location=LOCATION)

    try:
        # Step 1: Load to staging
        _load_to_staging(client, df, staging_table_id)

        # Step 2: MERGE into target
        rows_inserted = _merge_into_target(client, staging_table_id, target_table_id)
        summary["rows_inserted"] = rows_inserted

        # Step 3: Drop staging
        _drop_staging(client, staging_table_id)

        logger.info(
            f"Load complete: {rows_inserted}/{len(df)} rows inserted "
            f"({len(df) - rows_inserted} already existed)"
        )

    except Exception as e:
        logger.error(f"Load failed: {e}")
        # Attempt cleanup even on failure
        try:
            _drop_staging(client, staging_table_id)
        except Exception:
            logger.warning(f"Could not clean up staging table: {staging_table_id}")
        raise

    return summary


if __name__ == "__main__":
    # Quick test: dry run with a small sample
    import argparse

    from parse_csv_historical import parse_file

    parser = argparse.ArgumentParser(
        description="Load an IDFM historical CSV file into BigQuery raw_validations"
    )
    parser.add_argument("--file", required=True, help="Path to the CSV/TXT source file")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and validate without loading to BigQuery",
    )
    args = parser.parse_args()

    df = parse_file(args.file)

    summary = load_to_bigquery(df, dry_run=args.dry_run)

    print(f"\n{'[DRY RUN] ' if args.dry_run else ''}Load summary:")
    for k, v in summary.items():
        print(f"  {k}: {v}")
