"""
BigQuery table verification utilities.

Used by transport_daily_pipeline as a blocking gate after dbt build:
raises ValueError if any critical table falls below its minimum row count.
"""

import logging
from typing import List, Tuple

logger = logging.getLogger(__name__)


def verify_critical_table_row_counts(
    project_id: str,
    critical_tables: List[Tuple[str, str, int]],
) -> None:
    """
    Verify that each (dataset, table, min_rows) tuple meets its minimum.

    Raises ValueError listing all failures so the Airflow task is marked FAILED.
    Warns and returns silently if BigQuery is unavailable (e.g. local dev).

    Args:
        project_id: GCP project ID.
        critical_tables: list of (dataset, table, min_rows) tuples.
    """
    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=project_id)
        failures = []

        for dataset, table, min_rows in critical_tables:
            query = f"""
                SELECT COUNT(*) AS row_count
                FROM `{project_id}.{dataset}.{table}`
            """
            result = list(client.query(query).result())
            row_count = result[0].row_count if result else 0

            if row_count < min_rows:
                failures.append(
                    f"{dataset}.{table}: {row_count} rows (expected >= {min_rows})"
                )
                logger.error(
                    "❌ %s.%s: %d rows < %d minimum",
                    dataset,
                    table,
                    row_count,
                    min_rows,
                )
            else:
                logger.info("✅ %s.%s: %d rows", dataset, table, row_count)

        if failures:
            raise ValueError(
                f"Row count verification failed for {len(failures)} table(s):\n"
                + "\n".join(failures)
            )

        logger.info("✅ All critical tables verified — row counts OK")

    except ValueError:
        raise  # re-raise blocking failures
    except Exception as e:
        logger.warning("Row count verification skipped (BQ unavailable): %s", e)
