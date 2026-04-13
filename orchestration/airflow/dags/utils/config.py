"""Shared runtime configuration for Airflow DAGs and helpers."""

import os

AIRFLOW_ROOT = "/opt/airflow"
INGESTION_DIR = f"{AIRFLOW_ROOT}/ingestion"
SCRIPTS_DIR = f"{AIRFLOW_ROOT}/scripts"
DAGS_DIR = f"{AIRFLOW_ROOT}/dags"

BRONZE_DIR = f"{AIRFLOW_ROOT}/data/bronze"
VALIDATIONS_OUTPUT_DIR = f"{BRONZE_DIR}/validations"
PUNCTUALITY_OUTPUT_DIR = f"{BRONZE_DIR}/punctuality"
REFERENTIALS_OUTPUT_DIR = f"{BRONZE_DIR}/referentials"

DBT_PROJECT_DIR = f"{AIRFLOW_ROOT}/warehouse/dbt"
DBT_PROFILES_DIR = DBT_PROJECT_DIR
DBT_BIN = os.getenv("DBT_BIN", "/home/airflow/.local/bin/dbt")

DEFAULT_GCP_PROJECT_ID = "idfm-analytics-dev-488611"
DEFAULT_BQ_DATASET_RAW = "transport_raw"
DEFAULT_BQ_DATASET_BASE = "transport"

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID", DEFAULT_GCP_PROJECT_ID)
BQ_DATASET_RAW = os.getenv("BQ_DATASET_RAW", DEFAULT_BQ_DATASET_RAW)
BQ_DATASET_BASE = os.getenv("BQ_DATASET_BASE", DEFAULT_BQ_DATASET_BASE)
BQ_DATASET_CORE = os.getenv("BQ_DATASET_CORE", f"{BQ_DATASET_BASE}_core")
BQ_DATASET_ANALYTICS = os.getenv("BQ_DATASET_ANALYTICS", f"{BQ_DATASET_BASE}_analytics")

SLACK_WEBHOOK_CONN_ID = os.getenv("SLACK_WEBHOOK_CONN_ID", "slack_webhook")
SLACK_ALERT_CHANNEL = os.getenv("SLACK_ALERT_CHANNEL", "#data-alerts")


def dbt_env() -> dict[str, str]:
    """Environment shared by dbt BashOperators."""
    return {
        "DBT_PROFILES_DIR": DBT_PROFILES_DIR,
        "DBT_BIN": DBT_BIN,
        "GCP_PROJECT_ID": GCP_PROJECT_ID,
        "BQ_DATASET_RAW": BQ_DATASET_RAW,
        "BQ_DATASET_BASE": BQ_DATASET_BASE,
        "BQ_DATASET_CORE": BQ_DATASET_CORE,
        "BQ_DATASET_ANALYTICS": BQ_DATASET_ANALYTICS,
    }


def dbt_command(args: str) -> str:
    """Build a dbt bash command from the shared project and binary paths."""
    return f"cd {DBT_PROJECT_DIR} && {DBT_BIN} {args}"
