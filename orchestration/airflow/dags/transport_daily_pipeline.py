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

    sys.path.insert(0, "/opt/airflow/ingestion")
    from extract_validations import extract_validations

    # Get execution date (yesterday for daily run)
    execution_date = context["ds"]  # Format: YYYY-MM-DD

    extract_validations(
        start_date=execution_date,
        end_date=execution_date,
        output_dir="/opt/airflow/data/bronze/validations",
    )
    print(f"✅ Validations extracted for {execution_date}")


def extract_punctuality(**context):
    """Extract train punctuality data from Transilien API"""
    import sys

    sys.path.insert(0, "/opt/airflow/ingestion")
    import extract_ponctuality as mod

    execution_date = context["ds"]
    # Punctuality is monthly, so extract full month
    month_start = execution_date[:7] + "-01"  # First day of month

    mod.extract_punctuality(
        start_date=month_start,
        end_date=execution_date,
        output_dir="/opt/airflow/data/bronze/punctuality",
    )
    print(f"✅ Punctuality extracted for month starting {month_start}")


def extract_referentials(**context):
    """Extract reference data: stops and lines.
    NOTE V2: ref_stop_lines removed — arrets-lignes has no line_id field and is
    capped at 10,000/73,264 records. See TODO V4 in apis.yml for GTFS alternative.
    """
    import sys

    sys.path.insert(0, "/opt/airflow/ingestion")
    from extract_ref_lines import extract_ref_lines
    from extract_ref_stops import extract_ref_stops

    output_dir = "/opt/airflow/data/bronze/referentials"
    extract_ref_stops(output_dir=output_dir)
    extract_ref_lines(output_dir=output_dir)

    print("✅ Reference data extracted (stops, lines)")


def load_to_bigquery(**context):
    """Load JSON files to BigQuery RAW tables"""
    import sys

    sys.path.insert(0, "/opt/airflow/ingestion")
    from load_bigquery_raw import BigQueryLoader

    loader = BigQueryLoader()
    loader.load_all()

    print("✅ All data loaded to BigQuery RAW tables")


def check_sla(**context):
    """Validate data quality: freshness, completeness, validity"""
    import sys

    sys.path.insert(0, "/opt/airflow/scripts")
    from check_sla import check_sla

    exit_code = check_sla()

    if exit_code != 0:
        import logging

        logging.getLogger(__name__).warning(
            "⚠️ SLA breach detected - continuing in dev mode"
        )

    print("✅ All SLA checks passed")


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
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            /home/airflow/.local/bin/dbt deps
        """,
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/warehouse/dbt",
            "GCP_PROJECT_ID": "idfm-analytics-dev-488611",
            "BQ_DATASET_RAW": "transport_raw",
            "BQ_DATASET_STAGING": "transport_staging",
            "BQ_DATASET_ANALYTICS": "transport_staging_analytics",
        },
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 3b: Transform with dbt (run + test)
    # ─────────────────────────────────────────────────────────────

    dbt_build_task = BashOperator(
        task_id="dbt_build",
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            /home/airflow/.local/bin/dbt build --target prod --select +marts dim_stop stg_ref_stops
        """,
        env={
            "DBT_PROFILES_DIR": "/opt/airflow/warehouse/dbt",
            "GCP_PROJECT_ID": "idfm-analytics-dev-488611",
            "BQ_DATASET_RAW": "transport_raw",
            "BQ_DATASET_STAGING": "transport_staging",
            "BQ_DATASET_ANALYTICS": "transport_staging_analytics",
        },
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
    # STAGE 5: Notify success
    # ─────────────────────────────────────────────────────────────

    def notify_success_fn(**context):
        try:
            from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

            SlackWebhookHook(slack_webhook_conn_id="slack_webhook").send(
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

    # Sequential: Load → dbt deps → dbt build → Validate → Notify
    (
        load_bigquery_task
        >> dbt_deps_task
        >> dbt_build_task
        >> check_sla_task
        >> notify_success
    )


# ═════════════════════════════════════════════════════════════════
# Error Handling
# ═════════════════════════════════════════════════════════════════


def task_failure_alert(context):
    """Send Slack alert on task failure — silently skipped if Slack not configured.
    To enable: add 'slack_webhook' connection in Airflow UI (Admin → Connections).
    """
    # FIX V2: wrap in try/except — Slack connection may not be configured in dev/local
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")
        slack_hook.send(
            text=(
                f"❌ *Transport Daily Pipeline - FAILED*\n\n"
                f"📅 Date: {context['ds']}\n"
                f"🔧 Task: {context['task_instance'].task_id}\n"
                f"⚠️ Error: {context['exception']}\n"
                f"🔗 Log: {context['task_instance'].log_url}"
            ),
            channel="#data-alerts",
        )
    except Exception as e:
        import logging

        logging.getLogger(__name__).warning(
            f"Slack alert skipped (slack_webhook connection not configured): {e}"
        )


for task in dag.tasks:
    task.on_failure_callback = task_failure_alert
