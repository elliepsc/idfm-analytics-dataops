"""
Monitoring DAG — métriques pipeline, alertes seuils, santé des données.
Schedule: Daily at 7 AM (after transport_daily_pipeline at 2 AM)
"""

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/dags")
from utils.monitoring import (
    check_punctuality_freshness,
    check_validation_count_threshold,
    log_dag_metric,
    sla_miss_callback,
)

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "idfm-analytics-dev-488611")
DATASET_RAW = os.getenv("BQ_DATASET_RAW", "transport_raw")

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
}


def check_validations_threshold(**context):
    """Alerte si nombre de validations sous le seuil."""
    execution_date = context["ds"]
    ok = check_validation_count_threshold(
        project_id=PROJECT_ID,
        execution_date=execution_date,
        min_records=100,
    )
    if not ok:
        raise ValueError(f"Anomalie: validations insuffisantes pour {execution_date}")


def check_punctuality_lag(**context):
    """Alerte si données de ponctualité trop anciennes."""
    execution_date = context["ds"]
    ok = check_punctuality_freshness(
        project_id=PROJECT_ID,
        execution_date=execution_date,
        max_lag_days=45,
    )
    if not ok:
        raise ValueError(f"Anomalie: données ponctualité trop anciennes")


def log_pipeline_metrics(**context):
    """Log les métriques du run dans BigQuery."""
    dag_run = context["dag_run"]
    log_dag_metric(
        project_id=PROJECT_ID,
        dataset=DATASET_RAW,
        dag_id=context["dag"].dag_id,
        run_id=dag_run.run_id,
        task_id=context["task"].task_id,
        status="success",
        extra={"execution_date": context["ds"]},
    )


def notify_monitoring_success(**context):
    """Slack notification monitoring OK."""
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        SlackWebhookHook(slack_webhook_conn_id="slack_webhook").send(
            text=(
                f"✅ *Monitoring OK* — {context['ds']}\n"
                f"Validations: seuil OK | Ponctualité: fraîche"
            )
        )
    except Exception as e:
        logger.warning("Slack skipped: %s", e)


with DAG(
    dag_id="transport_monitoring",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=False,
    sla_miss_callback=sla_miss_callback,
    tags=["monitoring", "transport", "v2"],
    doc_md="""
    ## Transport Monitoring DAG
    Vérifie quotidiennement la qualité et la fraîcheur des données.
    - **check_validations**: seuil minimum de validations
    - **check_punctuality**: fraîcheur des données Transilien
    - **log_metrics**: logging BigQuery des métriques pipeline
    Runs at 7 AM after the main pipeline (2 AM).
    """,
) as dag:

    check_validations = PythonOperator(
        task_id="check_validations_threshold",
        python_callable=check_validations_threshold,
        sla=timedelta(hours=2),
    )

    check_punctuality = PythonOperator(
        task_id="check_punctuality_freshness",
        python_callable=check_punctuality_lag,
        sla=timedelta(hours=2),
    )

    log_metrics = PythonOperator(
        task_id="log_pipeline_metrics",
        python_callable=log_pipeline_metrics,
    )

    notify_success = PythonOperator(
        task_id="notify_monitoring_success",
        python_callable=notify_monitoring_success,
    )

    [check_validations, check_punctuality] >> log_metrics >> notify_success
