"""
Monitoring DAG — pipeline metrics & data quality checks
Schedule: Daily at 7 AM (after transport_daily_pipeline at 2 AM)
"""

import logging
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from utils.config import (
    BQ_DATASET_CORE,
    BQ_DATASET_RAW,
    DAGS_DIR,
    GCP_PROJECT_ID,
    SLACK_WEBHOOK_CONN_ID,
)
from utils.dag_utils import register_failure_callbacks

sys.path.insert(0, DAGS_DIR)
from utils.monitoring import (  # noqa: E402
    check_punctuality_freshness,
    check_statistical_anomaly,
    check_validation_count_threshold,
    log_dag_metric,
    send_anomaly_alert,
    sla_miss_callback,
)

logger = logging.getLogger(__name__)

PROJECT_ID = GCP_PROJECT_ID
DATASET_RAW = BQ_DATASET_RAW
DATASET_CORE = BQ_DATASET_CORE

# Z-score threshold — flag as anomaly if |z| > 2.5
# Corresponds to ~1.2% false positive rate (normal distribution)
Z_SCORE_THRESHOLD = 2.5

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
    """Alerts if the number of ticket validations is below the expected threshold."""
    execution_date = context["ds"]
    ok = check_validation_count_threshold(
        project_id=PROJECT_ID,
        execution_date=execution_date,
        dataset_raw=DATASET_RAW,
        min_records=100,
    )
    if not ok:
        raise ValueError(f"Anomalie: validations insuffisantes pour {execution_date}")


def check_punctuality_lag(**context):
    """Alerts if the punctuality data is too old."""
    execution_date = context["ds"]
    ok = check_punctuality_freshness(
        project_id=PROJECT_ID,
        execution_date=execution_date,
        dataset_raw=DATASET_RAW,
        max_lag_days=45,
    )
    if not ok:
        raise ValueError("Anomalie: données ponctualité trop anciennes")


def run_statistical_anomaly_check(**context):
    """
    Z-score anomaly detection on daily validation counts.

    Compares today's total validations against the 7-day rolling baseline.
    Logs z_score + is_anomaly to BigQuery dag_metrics.
    Sends enriched Slack alert if anomaly detected (via utils.monitoring).
    """
    execution_date = context["ds"]
    dag_run = context["dag_run"]

    anomaly = check_statistical_anomaly(
        project_id=PROJECT_ID,
        execution_date=execution_date,
        dataset_core=DATASET_CORE,
        z_score_threshold=Z_SCORE_THRESHOLD,
    )

    log_dag_metric(
        project_id=PROJECT_ID,
        dataset=DATASET_RAW,
        dag_id=context["dag"].dag_id,
        run_id=dag_run.run_id,
        task_id=context["task"].task_id,
        status="anomaly" if anomaly["is_anomaly"] else "ok",
        nb_records=anomaly["today_count"],
        extra={"execution_date": execution_date},
        z_score=anomaly["z_score"],
        is_anomaly=anomaly["is_anomaly"],
    )

    if anomaly["is_anomaly"]:
        send_anomaly_alert(anomaly, execution_date, Z_SCORE_THRESHOLD)
        # Don't raise — anomaly is logged and alerted, but doesn't fail the DAG
        logger.warning(
            "Anomaly detected but task continues (z=%.2f). "
            "Set raise on anomaly if you want task failure.",
            anomaly["z_score"],
        )
    else:
        logger.info(
            "No anomaly on %s (z=%.2f, today=%s)",
            execution_date,
            anomaly["z_score"] or 0,
            anomaly["today_count"],
        )


def log_pipeline_metrics(**context):
    """Log the metrics of the run in BigQuery."""
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

        SlackWebhookHook(slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID).send(
            text=(
                f"✅ *Monitoring OK* — {context['ds']}\n"
                f"Validations: seuil OK | Ponctualité: fraîche | Anomalie: aucune"
            )
        )
    except Exception as e:
        logger.warning("Slack skipped: %s", e)


with DAG(
    dag_id="transport_monitoring",
    default_args=default_args,
    schedule_interval="0 7 * * *",
    catchup=False,
    max_active_runs=1,
    sla_miss_callback=sla_miss_callback,
    tags=["monitoring", "transport", "v2"],
    doc_md=f"""
    ## Transport Monitoring DAG

    Vérifie quotidiennement la qualité et la fraîcheur des données.

    ### Tasks
    - **check_validations**: seuil minimum de validations (fixe)
    - **check_punctuality**: fraîcheur des données Transilien
    - **check_statistical_anomaly**: z-score sur validation_count 7j rolling
    - **log_metrics**: logging BigQuery des métriques pipeline
    - **notify_success**: Slack alert si tout OK

    ### Z-score anomaly detection
    Compares today's total validations against a 7-day rolling baseline.
    Threshold: |z| > 2.5 → anomaly alert sent to Slack.
    Results logged to {DATASET_RAW}.dag_metrics (z_score, is_anomaly columns).

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

    check_anomaly = PythonOperator(
        task_id="check_statistical_anomaly",
        python_callable=run_statistical_anomaly_check,
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

    # check_anomaly runs in parallel with the other checks
    (
        [check_validations, check_punctuality, check_anomaly]
        >> log_metrics
        >> notify_success
    )


# ═════════════════════════════════════════════════════════════════
# Pipeline Failure Alert
# ═════════════════════════════════════════════════════════════════


# task_failure_alert moved to dag_utils.py
register_failure_callbacks(dag)
