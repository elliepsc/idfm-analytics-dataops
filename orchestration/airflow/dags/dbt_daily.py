"""
dbt Daily Build DAG

Schedule: Daily at 3 AM (after data ingestion)
Purpose: Run dbt transformations only (no extraction/loading)

Use case: When you want to re-run transformations without re-extracting data
          For example, after fixing a dbt model bug
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
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "execution_timeout": timedelta(hours=1),
}

# ═════════════════════════════════════════════════════════════════
# DAG Definition
# ═════════════════════════════════════════════════════════════════

with DAG(
    dag_id="dbt_daily",
    default_args=default_args,
    description="dbt transformations only (no extraction)",
    schedule_interval="0 3 * * *",  # 3 AM daily (after ingestion pipeline)
    catchup=False,
    tags=["dbt", "transform", "daily"],
    max_active_runs=1,
) as dag:

    # ─────────────────────────────────────────────────────────────
    # TASK 1: Install dbt dependencies
    # ─────────────────────────────────────────────────────────────

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            /home/airflow/.local/bin/dbt deps --profiles-dir . --profile transport
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
    # TASK 2: Run staging models
    # ─────────────────────────────────────────────────────────────

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            /home/airflow/.local/bin/dbt run --target prod --select staging
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
    # TASK 3: Run core models (dimensions + facts)
    # ─────────────────────────────────────────────────────────────

    dbt_run_core = BashOperator(
        task_id="dbt_run_core",
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            /home/airflow/.local/bin/dbt run --target prod --select core
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
    # TASK 4: Run marts models (analytics)
    # ─────────────────────────────────────────────────────────────

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            /home/airflow/.local/bin/dbt run --target prod --select marts
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
    # TASK 5: Run all tests
    # ─────────────────────────────────────────────────────────────

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            /home/airflow/.local/bin/dbt test --target prod
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
    # TASK 6: Generate documentation
    # ─────────────────────────────────────────────────────────────

    dbt_docs_generate = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            /home/airflow/.local/bin/dbt docs generate --target prod
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
    # TASK 7: Success notification
    # ─────────────────────────────────────────────────────────────

    def notify_success_fn(**context):
        try:
            from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

            SlackWebhookHook(slack_webhook_conn_id="slack_webhook").send(
                text="dbt Daily SUCCESS"
            )
        except Exception as e:
            import logging

            logging.getLogger(__name__).warning(f"Slack skipped: {e}")
        print("dbt daily completed successfully")

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=notify_success_fn,
    )

    # ─────────────────────────────────────────────────────────────
    # Task Dependencies
    # ─────────────────────────────────────────────────────────────

    # Sequential execution through layers
    (
        dbt_deps
        >> dbt_run_staging
        >> dbt_run_core
        >> dbt_run_marts
        >> dbt_test
        >> dbt_docs_generate
        >> notify_success
    )


# ═════════════════════════════════════════════════════════════════
# Error Handling
# ═════════════════════════════════════════════════════════════════


def task_failure_alert(context):
    """Send alert on task failure"""
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

    slack_hook = SlackWebhookHook(slack_webhook_conn_id="slack_webhook")

    message = f"""
❌ *dbt Daily Build - FAILED*

📅 Date: {context['ds']}
🔧 Task: {context['task_instance'].task_id}
⚠️ Error: {context['exception']}
🔗 Log: {context['task_instance'].log_url}
    """

    slack_hook.send(text=message, channel="#data-alerts")


# Apply failure callback to all tasks
for task in dag.tasks:
    task.on_failure_callback = task_failure_alert
