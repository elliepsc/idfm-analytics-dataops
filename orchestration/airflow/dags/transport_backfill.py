"""
Historical Data Backfill DAG

Schedule: Manual trigger only
Purpose: Load historical data for a date range

Use case: Initial data load or filling gaps in historical data
          Example: Load all data from 2023-01-01 to 2023-12-31

Trigger with parameters:
    airflow dags trigger transport_backfill --conf '{"start_date":"2023-01-01", "end_date":"2023-12-31"}'
"""

from datetime import datetime, timedelta

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from utils.config import (
    BQ_DATASET_RAW,
    GCP_PROJECT_ID,
    INGESTION_DIR,
    PUNCTUALITY_OUTPUT_DIR,
    REFERENTIALS_OUTPUT_DIR,
    SLACK_WEBHOOK_CONN_ID,
    VALIDATIONS_OUTPUT_DIR,
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
    "retry_delay": timedelta(minutes=10),
    "execution_timeout": timedelta(hours=6),  # Longer timeout for backfills
}

# ═════════════════════════════════════════════════════════════════
# Python Callables
# ═════════════════════════════════════════════════════════════════


def extract_validations_backfill(**context):
    """
    Extract validations for entire date range
    Date range comes from DAG run configuration
    """
    import sys

    sys.path.insert(0, INGESTION_DIR)
    from extract_validations import extract_validations

    # Get date range from DAG run config
    dag_run = context["dag_run"]
    start_date = dag_run.conf.get("start_date", "2024-01-01")
    end_date = dag_run.conf.get("end_date", "2024-01-31")

    print(f"📅 Backfilling validations from {start_date} to {end_date}")

    extract_validations(
        start_date=start_date,
        end_date=end_date,
        output_dir=VALIDATIONS_OUTPUT_DIR,
    )

    print(f"✅ Validations backfill complete: {start_date} to {end_date}")


def extract_punctuality_backfill(**context):
    """
    Extract punctuality for entire date range
    Punctuality is monthly, so we extract by month
    """
    import sys

    sys.path.insert(0, INGESTION_DIR)
    import extract_ponctuality as mod

    # Get date range from DAG run config
    dag_run = context["dag_run"]
    start_date_str = dag_run.conf.get("start_date", "2024-01-01")
    end_date_str = dag_run.conf.get("end_date", "2024-01-31")

    start_date = pendulum.parse(start_date_str)
    end_date = pendulum.parse(end_date_str)

    print(f"📅 Backfilling punctuality from {start_date_str} to {end_date_str}")

    # Extract month by month
    current = start_date.start_of("month")
    while current <= end_date:
        month_start = current.format("YYYY-MM-DD")
        month_end = current.end_of("month").format("YYYY-MM-DD")

        print(f"  Extracting month: {current.format('YYYY-MM')}")

        mod.extract_punctuality(
            start_date=month_start,
            end_date=month_end,
            output_dir=PUNCTUALITY_OUTPUT_DIR,
        )

        current = current.add(months=1)

    print(f"✅ Punctuality backfill complete: {start_date_str} to {end_date_str}")


def extract_referentials_backfill(**context):
    """
    Extract reference data (stops, lines, mappings)
    Reference data doesn't change by date, so extract once
    """
    import sys

    sys.path.insert(0, INGESTION_DIR)
    from extract_ref_lines import extract_ref_lines
    from extract_ref_stops import extract_ref_stops

    print("📅 Extracting reference data (stops, lines, mappings)")

    extract_ref_stops(output_dir=REFERENTIALS_OUTPUT_DIR)
    extract_ref_lines(output_dir=REFERENTIALS_OUTPUT_DIR)

    print("✅ Reference data extraction complete")


def load_to_bigquery_backfill(**context):
    """Load all extracted JSON files to BigQuery RAW"""
    import sys

    sys.path.insert(0, INGESTION_DIR)
    from load_bigquery_raw import BigQueryLoader

    print("📥 Loading all JSON files to BigQuery RAW")

    loader = BigQueryLoader()
    loader.load_all()

    print("✅ All data loaded to BigQuery RAW")


def validate_backfill(**context):
    """
    Validate backfill completeness
    Check that data exists for all dates in range
    """
    from google.cloud import bigquery

    # Get date range from DAG run config
    dag_run = context["dag_run"]
    start_date_str = dag_run.conf.get("start_date", "2024-01-01")
    end_date_str = dag_run.conf.get("end_date", "2024-01-31")

    start_date = pendulum.parse(start_date_str)
    end_date = pendulum.parse(end_date_str)

    client = bigquery.Client(project=GCP_PROJECT_ID)

    # Count records per date
    query = f"""
    SELECT
        date,
        COUNT(*) as record_count
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET_RAW}.raw_validations`
    WHERE date BETWEEN '{start_date_str}' AND '{end_date_str}'
    GROUP BY date
    ORDER BY date
    """

    results = client.query(query).result()

    # Check for missing dates
    dates_with_data = {row.date for row in results}
    expected_dates = set()
    current = start_date
    while current <= end_date:
        expected_dates.add(current.date())
        current = current.add(days=1)

    missing_dates = expected_dates - dates_with_data

    if missing_dates:
        missing_str = ", ".join(str(d) for d in sorted(missing_dates))
        print(f"⚠️  WARNING: Missing data for dates: {missing_str}")
    else:
        print(f"✅ All dates have data from {start_date_str} to {end_date_str}")

    return {"missing_dates": len(missing_dates), "total_dates": len(expected_dates)}


# ═════════════════════════════════════════════════════════════════
# DAG Definition
# ═════════════════════════════════════════════════════════════════

with DAG(
    dag_id="transport_backfill",
    default_args=default_args,
    description="Historical data backfill (manual trigger)",
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=["transport", "backfill", "manual"],
    max_active_runs=1,
    params={
        "start_date": "2024-01-01",
        "end_date": "2024-01-31",
    },
) as dag:

    # ─────────────────────────────────────────────────────────────
    # STAGE 1: Extract historical data (parallel)
    # ─────────────────────────────────────────────────────────────

    extract_validations_task = PythonOperator(
        task_id="extract_validations_backfill",
        python_callable=extract_validations_backfill,
        provide_context=True,
    )

    extract_punctuality_task = PythonOperator(
        task_id="extract_punctuality_backfill",
        python_callable=extract_punctuality_backfill,
        provide_context=True,
    )

    extract_referentials_task = PythonOperator(
        task_id="extract_referentials_backfill",
        python_callable=extract_referentials_backfill,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 2: Load to BigQuery RAW
    # ─────────────────────────────────────────────────────────────

    load_bigquery_task = PythonOperator(
        task_id="load_bigquery_backfill",
        python_callable=load_to_bigquery_backfill,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 3: Transform with dbt (full refresh)
    # dbt_deps is isolated to avoid Elementary race condition on
    # on-run-start hook ('elementary' is undefined at attempt=1
    # when deps and build run in the same bash command).
    # ─────────────────────────────────────────────────────────────

    dbt_deps_task = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
        retries=0,  # deps failure = real network/package issue, don't mask it
        env=dbt_env(),
    )

    dbt_build_task = BashOperator(
        task_id="dbt_build_full_refresh",
        bash_command=dbt_command("build --target prod --full-refresh"),
        retries=1,
        retry_delay=timedelta(minutes=2),
        env=dbt_env(),
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 4: Validate completeness
    # ─────────────────────────────────────────────────────────────

    validate_task = PythonOperator(
        task_id="validate_backfill_completeness",
        python_callable=validate_backfill,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 5: Success notification
    # ─────────────────────────────────────────────────────────────

    def notify_success_fn(**context):
        try:
            from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

            SlackWebhookHook(slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID).send(
                text="Backfill SUCCESS"
            )
        except Exception as e:
            import logging

            logging.getLogger(__name__).warning(f"Slack skipped: {e}")
        print("Backfill completed successfully")

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success_fn,
    )

    # ─────────────────────────────────────────────────────────────
    # Task Dependencies
    # ─────────────────────────────────────────────────────────────

    # Parallel extraction, then sequential processing
    [
        extract_validations_task,
        extract_punctuality_task,
        extract_referentials_task,
    ] >> load_bigquery_task

    # Sequential: Load → Deps → Build → Validate → Notify
    (
        load_bigquery_task
        >> dbt_deps_task
        >> dbt_build_task
        >> validate_task
        >> notify_success
    )


# ═════════════════════════════════════════════════════════════════
# Error Handling
# ═════════════════════════════════════════════════════════════════


# task_failure_alert moved to dag_utils.py
register_failure_callbacks(dag)
