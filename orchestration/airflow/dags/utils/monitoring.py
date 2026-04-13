"""
Monitoring utilities — Dag metrics & data quality checks.
Used by monitoring_dag.py et transport_daily_pipeline.py.
"""

import logging
from datetime import datetime, timezone
from typing import Optional

from utils.config import BQ_DATASET_CORE, BQ_DATASET_RAW, SLACK_WEBHOOK_CONN_ID

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
    z_score: Optional[float] = None,
    is_anomaly: Optional[bool] = None,
) -> None:
    """Log a DAG metric in BigQuery `<dataset>.dag_metrics`."""
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
            "z_score": z_score,
            "is_anomaly": is_anomaly,
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
    dataset_raw: str = BQ_DATASET_RAW,
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
            FROM `{project_id}.{dataset_raw}.validations_rail`
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
    dataset_raw: str = BQ_DATASET_RAW,
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
            FROM `{project_id}.{dataset_raw}.punctuality_monthly`
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
# Statistical Anomaly Detection — Z-score
# ═════════════════════════════════════════════════════════════════


def check_statistical_anomaly(
    project_id: str,
    execution_date: str,
    dataset_core: str = BQ_DATASET_CORE,
    z_score_threshold: float = 2.5,
    lookback_days: int = 7,
) -> dict:
    """
    Detects statistical anomalies in daily validation counts using z-score.

    Strategy:
    - Fetches validation_count for the last (lookback_days + 1) days
    - Computes mean and std over the lookback window (excluding today)
    - Z-score = (today - mean) / std
    - Flags as anomaly if |z_score| > z_score_threshold

    Why z-score over fixed threshold:
    - Automatically adapts to weekday/weekend patterns
    - Self-calibrating: no manual threshold tuning needed
    - Detects both drops AND spikes

    Returns a dict with:
        - today_count: int
        - mean_7d: float
        - std_7d: float
        - z_score: float
        - is_anomaly: bool
        - direction: str ("low" | "high" | "normal")
        - execution_date: str
    """
    result = {
        "today_count": None,
        "mean_7d": None,
        "std_7d": None,
        "z_score": None,
        "is_anomaly": False,
        "direction": "normal",
        "execution_date": execution_date,
    }

    try:
        from google.cloud import bigquery

        client = bigquery.Client(project=project_id)

        # Fetch last (lookback_days + 1) days of daily validation totals
        query = f"""
            SELECT
                validation_date,
                SUM(validation_count) AS daily_total
            FROM `{project_id}.{dataset_core}.fct_validations_daily`
            WHERE validation_date >= DATE_SUB(DATE '{execution_date}', INTERVAL {lookback_days} DAY)
              AND validation_date <= DATE '{execution_date}'
            GROUP BY validation_date
            ORDER BY validation_date ASC
        """

        rows = list(client.query(query).result())

        if len(rows) < 3:
            logger.warning(
                "Not enough data for z-score (got %d rows, need >= 3). Skipping.",
                len(rows),
            )
            return result

        # Split: baseline (all days except today) vs today
        baseline = [
            row.daily_total
            for row in rows
            if str(row.validation_date) != execution_date
        ]
        today_rows = [
            row.daily_total
            for row in rows
            if str(row.validation_date) == execution_date
        ]

        if not today_rows:
            logger.warning(
                "No data found for execution_date %s. Skipping.", execution_date
            )
            return result

        today_count = today_rows[0]

        if len(baseline) < 2:
            logger.warning(
                "Not enough baseline days (%d). Skipping z-score.", len(baseline)
            )
            return result

        # Compute mean and std over baseline
        mean_7d = sum(baseline) / len(baseline)
        variance = sum((x - mean_7d) ** 2 for x in baseline) / len(baseline)
        std_7d = variance**0.5

        # Avoid division by zero (e.g. all baseline values identical)
        if std_7d == 0:
            logger.info("Std=0 on baseline — no variance detected. Z-score set to 0.")
            z_score = 0.0
        else:
            z_score = (today_count - mean_7d) / std_7d

        is_anomaly = abs(z_score) > z_score_threshold
        direction = "normal"
        if z_score < -z_score_threshold:
            direction = "low"
        elif z_score > z_score_threshold:
            direction = "high"

        result.update(
            {
                "today_count": int(today_count),
                "mean_7d": round(mean_7d, 0),
                "std_7d": round(std_7d, 0),
                "z_score": round(z_score, 3),
                "is_anomaly": is_anomaly,
                "direction": direction,
            }
        )

        if is_anomaly:
            logger.warning(
                "⚠️ Statistical anomaly detected on %s: today=%d, mean_7d=%.0f, z=%.2f (%s)",
                execution_date,
                today_count,
                mean_7d,
                z_score,
                direction,
            )
        else:
            logger.info(
                "✅ No anomaly on %s: today=%d, mean_7d=%.0f, z=%.2f",
                execution_date,
                today_count,
                mean_7d,
                z_score,
            )

        return result

    except Exception as e:
        logger.warning("Statistical anomaly check skipped: %s", e)
        return result


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
        SlackWebhookHook(slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID).send(text=message)
    except Exception as e:
        logger.warning("Slack SLA alert skipped: %s", e)
