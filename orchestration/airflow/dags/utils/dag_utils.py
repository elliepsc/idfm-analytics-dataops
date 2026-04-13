"""
Shared DAG utilities — imported by all DAGs in this project.

Centralizes cross-cutting concerns:
  - task_failure_alert: Slack notification on task failure
  - register_failure_callbacks: Apply failure callback to all tasks in a DAG

Usage:
    from utils.dag_utils import task_failure_alert, register_failure_callbacks

    with DAG(...) as dag:
        ...

    register_failure_callbacks(dag)
"""

import logging

from utils.config import SLACK_ALERT_CHANNEL, SLACK_WEBHOOK_CONN_ID

logger = logging.getLogger(__name__)


def task_failure_alert(context):
    """Send a Slack alert on task failure.

    Silently skipped if the slack_webhook connection is not configured.
    To enable: Admin → Connections → add 'slack_webhook' in Airflow UI.
    """
    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    execution_date = context["ds"]
    exception = context.get("exception", "unknown error")
    log_url = context["task_instance"].log_url
    dag_run_conf = getattr(context.get("dag_run"), "conf", {}) or {}

    details = [f"📅 Date: {execution_date}"]
    if dag_run_conf.get("start_date") and dag_run_conf.get("end_date"):
        details.append(
            f"🗓️ Range: {dag_run_conf['start_date']} → {dag_run_conf['end_date']}"
        )
    details.extend(
        [
            f"🔧 Task: {task_id}",
            f"⚠️ Error: {exception}",
            f"🔗 Log: {log_url}",
        ]
    )

    text = f"❌ *{dag_id} — FAILED*\n\n" + "\n".join(details)

    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        SlackWebhookHook(slack_webhook_conn_id=SLACK_WEBHOOK_CONN_ID).send(
            text=text,
            channel=SLACK_ALERT_CHANNEL,
        )
    except Exception as e:
        logger.warning(
            "Slack alert skipped (slack_webhook connection not configured): %s", e
        )


def register_failure_callbacks(dag):
    """Apply task_failure_alert to every task in the DAG.

    Call this after all tasks have been defined:

        with DAG(...) as dag:
            task_a = ...
            task_b = ...

        register_failure_callbacks(dag)
    """
    for task in dag.tasks:
        task.on_failure_callback = task_failure_alert
