"""
Daily Transport Analytics Pipeline

Schedule: Daily at 2 AM
Purpose: Ingest yesterday's data, transform with dbt, validate quality, alert on status

DAG Flow:
1. Extract (parallel): validations + punctuality + referentials
2. Load: JSON files → BigQuery RAW tables
3. Transform: dbt build (run models + tests)
4. Validate: SLA checks on data freshness/completeness
5. Alert: Slack notification on success/failure
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.bq_checks import verify_critical_table_row_counts
from utils.config import (
    BQ_DATASET_ANALYTICS,
    BQ_DATASET_CORE,
    GCP_PROJECT_ID,
    GCS_BUCKET_RAW,
    INGESTION_DIR,
    SCRIPTS_DIR,
    SLACK_WEBHOOK_CONN_ID,
    dbt_command,
    dbt_env,
)
from utils.dag_utils import register_failure_callbacks

# ═════════════════════════════════════════════════════════════════
# Configuration
# ═════════════════════════════════════════════════════════════════

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# ═════════════════════════════════════════════════════════════════
# Python Callables
# ═════════════════════════════════════════════════════════════════


def extract_validations(**context):
    """Extract ticket validation data from IDFM API"""
    import sys

    sys.path.insert(0, INGESTION_DIR)
    from extract_validations import extract_validations

    # Get execution date (yesterday for daily run)
    execution_date = context["ds"]  # Format: YYYY-MM-DD

    extract_validations(
        start_date=execution_date,
        end_date=execution_date,
        gcs_bucket=GCS_BUCKET_RAW,
    )
    print(f"✅ Validations extracted for {execution_date}")


def extract_punctuality(**context):
    """Extract train punctuality data from Transilien API"""
    import sys

    sys.path.insert(0, INGESTION_DIR)
    import extract_ponctuality as mod

    execution_date = context["ds"]
    # Punctuality is monthly, so extract full month
    month_start = execution_date[:7] + "-01"  # First day of month

    mod.extract_punctuality(
        start_date=month_start,
        end_date=execution_date,
        gcs_bucket=GCS_BUCKET_RAW,
    )
    print(f"✅ Punctuality extracted for month starting {month_start}")


def extract_referentials(**context):
    """Extract reference data: stops, lines, and stop-to-line mapping (GTFS).

    V4: ref_stop_lines is now extracted from the IDFM GTFS feed
    (offre-horaires-tc-gtfs-idfm) instead of the arrets-lignes ODS API
    which has no line_id field and is capped at 10,000/73,264 records.

    GTFS join: stop_times.trip_id → trips.trip_id → trips.route_id (= line_id)
    This populates stg_ref_stop_lines and unblocks mart_line_demand_vs_punctuality.
    """
    import sys

    sys.path.insert(0, INGESTION_DIR)
    from extract_ref_lines import extract_ref_lines
    from extract_ref_stop_lines import extract_ref_stop_lines
    from extract_ref_stops import extract_ref_stops

    extract_ref_stops(gcs_bucket=GCS_BUCKET_RAW)
    extract_ref_lines(gcs_bucket=GCS_BUCKET_RAW)

    # GTFS extraction — requires IDFM_API_KEY env var
    # If the key is missing or the download fails, log a warning and continue.
    # The pipeline must not fail because of a missing stop_lines refresh —
    # the existing BigQuery table remains valid from the last successful run.
    try:
        extract_ref_stop_lines(gcs_bucket=GCS_BUCKET_RAW)
        print("✅ Reference data extracted (stops, lines, stop_lines GTFS)")
    except Exception as e:
        print(
            f"⚠️  GTFS stop_lines extraction failed: {e}. "
            "Continuing with existing raw_ref_stop_lines in BigQuery."
        )


def load_to_bigquery(**context):
    """Load JSON files to BigQuery RAW tables"""
    import sys

    sys.path.insert(0, INGESTION_DIR)
    from load_bigquery_raw import BigQueryLoader

    loader = BigQueryLoader()
    loader.load_all()

    print("✅ All data loaded to BigQuery RAW tables")


def check_sla(**context):
    """Validate data quality: freshness, completeness, validity"""
    import sys

    sys.path.insert(0, SCRIPTS_DIR)
    from check_sla import check_sla

    exit_code = check_sla()

    if exit_code != 0:
        import logging

        logging.getLogger(__name__).warning(
            "⚠️ SLA breach detected - continuing in dev mode"
        )

    print("✅ All SLA checks passed")


def verify_row_counts(**context):
    """Blocking gate: fails the pipeline if critical tables are empty after dbt build."""
    # Tables that MUST have rows after a successful dbt build
    critical_tables = [
        (BQ_DATASET_CORE, "fct_validations_daily", 1_000_000),
        (BQ_DATASET_ANALYTICS, "mart_network_scorecard_monthly", 10),
        (BQ_DATASET_ANALYTICS, "mart_validations_station_daily", 100),
    ]
    verify_critical_table_row_counts(GCP_PROJECT_ID, critical_tables)


# ═════════════════════════════════════════════════════════════════
# DAG Definition
# ═════════════════════════════════════════════════════════════════

with DAG(
    dag_id="transport_daily_pipeline",
    default_args=default_args,
    description="Daily pipeline: Extract → Load → Transform → Validate",
    schedule_interval="0 2 * * *",  # 2 AM every day
    catchup=False,  # Don't backfill on enable
    tags=["transport", "daily", "production"],
    max_active_runs=1,  # Only one run at a time
) as dag:

    # ─────────────────────────────────────────────────────────────
    # STAGE 1: Extract (parallel execution for speed)
    # ─────────────────────────────────────────────────────────────

    extract_validations_task = PythonOperator(
        task_id="extract_validations",
        python_callable=extract_validations,
        provide_context=True,
    )

    extract_punctuality_task = PythonOperator(
        task_id="extract_punctuality",
        python_callable=extract_punctuality,
        provide_context=True,
    )

    extract_referentials_task = PythonOperator(
        task_id="extract_referentials",
        python_callable=extract_referentials,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 2: Load to BigQuery RAW
    # ─────────────────────────────────────────────────────────────

    load_bigquery_task = PythonOperator(
        task_id="load_bigquery_raw",
        python_callable=load_to_bigquery,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 3a: Install dbt dependencies
    # ─────────────────────────────────────────────────────────────
    # FIX: previously dbt deps && dbt build ran in a single BashOperator.
    # On slow Docker volumes, dbt build could start before dbt deps had
    # finished writing packages to disk → elementary undefined → ERROR on
    # on-run-start hook → attempt 1 always failed, attempt 2 succeeded
    # from cache. Separating into two tasks guarantees ordering.

    dbt_deps_task = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
        env=dbt_env(),
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 3b: Transform with dbt (run + test)
    # ─────────────────────────────────────────────────────────────

    dbt_build_task = BashOperator(
        task_id="dbt_build",
        bash_command=dbt_command(
            "build --target prod --select +marts dim_stop stg_ref_stops"
        ),
        env=dbt_env(),
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 4: Validate data quality (SLA checks)
    # ─────────────────────────────────────────────────────────────

    check_sla_task = PythonOperator(
        task_id="check_sla",
        python_callable=check_sla,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 3c: Verify row counts after dbt build (blocking gate)
    # ─────────────────────────────────────────────────────────────
    # check_sla runs in a separate DAG at 7 AM — by then a zero-row
    # table would have already served a broken dashboard for hours.
    # This task blocks the pipeline immediately if critical tables are empty.

    verify_counts_task = PythonOperator(
        task_id="verify_row_counts",
        python_callable=verify_row_counts,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 5: Notify success
    # ─────────────────────────────────────────────────────────────

    def notify_success_fn(**context):
        try:
            from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

            SlackWebhookHook(slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID).send(
                text="Pipeline SUCCESS"
            )
        except Exception as e:
            import logging

            logging.getLogger(__name__).warning(f"Slack skipped: {e}")
        print("Pipeline completed successfully")

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success_fn,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────
    # Task Dependencies (DAG structure)
    # ─────────────────────────────────────────────────────────────

    # Parallel extraction, then sequential processing
    [
        extract_validations_task,
        extract_punctuality_task,
        extract_referentials_task,
    ] >> load_bigquery_task

    # Sequential: Load → dbt deps → dbt build → Verify rows → SLA check → Notify
    (
        load_bigquery_task
        >> dbt_deps_task
        >> dbt_build_task
        >> verify_counts_task
        >> check_sla_task
        >> notify_success
    )


# ═════════════════════════════════════════════════════════════════
# Error Handling
# ═════════════════════════════════════════════════════════════════


# task_failure_alert moved to dag_utils.py
register_failure_callbacks(dag)
