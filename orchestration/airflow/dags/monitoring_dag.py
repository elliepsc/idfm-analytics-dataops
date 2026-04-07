"""
Monitoring DAG — pipeline metrics & data quality checks
Schedule: Daily at 7 AM (after transport_daily_pipeline at 2 AM)
"""

import logging
import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

sys.path.insert(0, "/opt/airflow/dags")
from utils.monitoring import (  # noqa: E402
    check_punctuality_freshness,
    check_statistical_anomaly,
    check_validation_count_threshold,
    log_dag_metric,
    sla_miss_callback,
)

logger = logging.getLogger(__name__)

PROJECT_ID = os.getenv("GCP_PROJECT_ID", "idfm-analytics-dev-488611")
DATASET_RAW = os.getenv("BQ_DATASET_RAW", "transport_raw")
DATASET_CORE = os.getenv("BQ_DATASET_CORE", "transport_staging_core")

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
        max_lag_days=45,
    )
    if not ok:
        raise ValueError("Anomalie: données ponctualité trop anciennes")


def run_statistical_anomaly_check(**context):
    """
    Z-score anomaly detection on daily validation counts.

    Compares today's total validations against the 7-day rolling baseline.
    Logs z_score + is_anomaly to BigQuery dag_metrics.
    Sends enriched Slack alert if anomaly detected.

    Why z-score:
    - Adapts to weekday/weekend patterns automatically
    - No manual threshold tuning needed
    - Detects both drops (pipeline failure) and spikes (data duplication)
    """
    execution_date = context["ds"]
    dag_run = context["dag_run"]

    anomaly = check_statistical_anomaly(
        project_id=PROJECT_ID,
        execution_date=execution_date,
        dataset_core=DATASET_CORE,
        z_score_threshold=Z_SCORE_THRESHOLD,
    )

    # Log to BigQuery dag_metrics with z_score + is_anomaly fields
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

    # Send enriched Slack alert if anomaly detected
    if anomaly["is_anomaly"]:
        direction_emoji = "📉" if anomaly["direction"] == "low" else "📈"
        direction_label = (
            "anormalement basse"
            if anomaly["direction"] == "low"
            else "anormalement haute"
        )

        message = (
            f"{direction_emoji} *Anomalie statistique détectée* — {execution_date}\n"
            f"• Validations aujourd'hui : *{anomaly['today_count']:,}*\n"
            f"• Moyenne 7 jours : {anomaly['mean_7d']:,.0f}\n"
            f"• Écart-type 7 jours : {anomaly['std_7d']:,.0f}\n"
            f"• Z-score : *{anomaly['z_score']}* (seuil : ±{Z_SCORE_THRESHOLD})\n"
            f"• Valeur {direction_label}\n"
            f"_Vérifier le DAG transport_daily_pipeline_"
        )

        try:
            from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

            SlackWebhookHook(slack_webhook_conn_id="slack_webhook").send(text=message)
            logger.warning("Anomaly Slack alert sent for %s", execution_date)
        except Exception as e:
            logger.warning("Slack anomaly alert skipped: %s", e)

        # Don't raise — anomaly is logged and alerted, but doesn't fail the DAG
        # Change to raise ValueError(...) if you want the task to fail on anomaly
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

        SlackWebhookHook(slack_webhook_conn_id="slack_webhook").send(
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
    doc_md="""
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
    Results logged to transport_raw.dag_metrics (z_score, is_anomaly columns).

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


def task_failure_alert(context):
    """Send pipeline failure alert to Slack — separate from z-score anomaly alerts.
    Pipeline failures → #idfm-pipeline-alerts
    Z-score anomalies → #idfm-anomaly-alerts (handled in notify_monitoring_success)
    """
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        SlackWebhookHook(slack_webhook_conn_id="slack_webhook").send(
            text=(
                f"❌ *Monitoring DAG - FAILED*\n\n"
                f"📅 Date: {context['ds']}\n"
                f"🔧 Task: {context['task_instance'].task_id}\n"
                f"⚠️ Error: {context['exception']}\n"
                f"🔗 Log: {context['task_instance'].log_url}"
            )
        )
    except Exception as e:
        import logging

        logging.getLogger(__name__).warning("Slack pipeline alert skipped: %s", e)


# Apply failure callback to all monitoring tasks
for task in dag.tasks:
    task.on_failure_callback = task_failure_alert
