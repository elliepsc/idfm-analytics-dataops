"""
Monitoring utilities — Dag metrics & data quality checks.
Used by monitoring_dag.py et transport_daily_pipeline.py.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════
# DAG Metrics — logging to BigQuery
# ═════════════════════════════════════════════════════════════════


def log_dag_metric(
    project_id: str,
    dataset: str,
    dag_id: str,
    run_id: str,
    task_id: str,
    status: str,
    duration_seconds: Optional[float] = None,
    nb_records: Optional[int] = None,
    extra: Optional[dict] = None,
) -> None:
    """Log a DAG metric in BigQuery transport_raw.dag_metrics."""
    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=project_id)
        table_id = f"{project_id}.{dataset}.dag_metrics"

        row = {
            "ingestion_ts": datetime.now(timezone.utc).isoformat(),
            "dag_id": dag_id,
            "run_id": run_id,
            "task_id": task_id,
            "status": status,
            "duration_seconds": duration_seconds,
            "nb_records": nb_records,
            "extra": str(extra) if extra else None,
        }

        errors = client.insert_rows_json(table_id, [row])
        if errors:
            logger.warning("BigQuery insert errors: %s", errors)
        else:
            logger.info("Metric logged: %s/%s → %s", dag_id, task_id, status)

    except Exception as e:
        logger.warning("Metric logging skipped: %s", e)


# ═════════════════════════════════════════════════════════════════
# Alerts — threshold checks & Slack notifications
# ═════════════════════════════════════════════════════════════════


def check_validation_count_threshold(
    project_id: str,
    execution_date: str,
    min_records: int = 100,
) -> bool:
    """
    Checks that the number of validations for the day exceeds the minimum threshold.
    Returns True if OK, False if an anomaly is detected.
    """
    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=project_id)
        query = f"""
            SELECT COUNT(*) as nb
            FROM `{project_id}.transport_raw.validations_rail`
            WHERE DATE(date) = '{execution_date}'
        """
        result = list(client.query(query).result())
        nb = result[0].nb if result else 0

        if nb < min_records:
            logger.warning(
                "⚠️ Anomalie: seulement %d validations pour %s (seuil: %d)",
                nb,
                execution_date,
                min_records,
            )
            return False

        logger.info("✅ %d validations pour %s", nb, execution_date)
        return True

    except Exception as e:
        logger.warning("Threshold check skipped: %s", e)
        return True


def check_punctuality_freshness(
    project_id: str,
    execution_date: str,
    max_lag_days: int = 45,
) -> bool:
    """
    Checks that the punctuality data is not too old.
    Returns True if OK, False if the data is too stale.
    """
    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=project_id)
        query = f"""
            SELECT MAX(date) as latest_date
            FROM `{project_id}.transport_raw.punctuality_monthly`
        """
        result = list(client.query(query).result())
        latest = result[0].latest_date if result else None

        if not latest:
            logger.warning("⚠️ Aucune donnée de ponctualité trouvée")
            return False

        from datetime import date

        lag = (date.fromisoformat(execution_date) - latest).days
        if lag > max_lag_days:
            logger.warning(
                "⚠️ Données ponctualité trop anciennes: %d jours (max: %d)",
                lag,
                max_lag_days,
            )
            return False

        logger.info("✅ Ponctualité fraîche: lag=%d jours", lag)
        return True

    except Exception as e:
        logger.warning("Freshness check skipped: %s", e)
        return True


# ═════════════════════════════════════════════════════════════════
# SLA Miss Callback
# ═════════════════════════════════════════════════════════════════


def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    """Callback Airflow called when an SLA is missed."""
    missed = [str(s.task_id) for s in slas]
    logger.error("❌ SLA manqué pour les tâches: %s", missed)

    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        message = (
            f"⏰ *SLA Miss* sur `{dag.dag_id}`\n"
            f"Tâches en retard: {', '.join(missed)}\n"
            f"DAG: {dag.dag_id}"
        )
        SlackWebhookHook(slack_webhook_conn_id="slack_webhook").send(text=message)
    except Exception as e:
        logger.warning("Slack SLA alert skipped: %s", e)
