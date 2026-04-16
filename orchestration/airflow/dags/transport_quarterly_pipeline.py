"""
Quarterly Transport Analytics Pipeline — Reliability Layer (X1 + X5)

Schedule: 1st of February, May, August, November at 6 AM
          (≈ 4 weeks after quarter end — IDFM publishes data with this lag)

Purpose: Ingest quarterly datasets and rebuild reliability layer dbt models.

DAG Flow:
1. Extract (parallel): service quality indicators + hourly validation profiles
2. Load: NDJSON files → BigQuery RAW tables (WRITE_TRUNCATE — full snapshot)
3. Transform: dbt build reliability layer models
4. Enable reminder: log a reminder to flip enabled=True in dbt models if first run

When to trigger manually:
  - After IDFM publishes a new quarter edition (check data.iledefrance-mobilites.fr)
  - After the first-ever load (to provision raw_service_quality + raw_hourly_profiles)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import GCS_BUCKET_RAW, INGESTION_DIR, dbt_command, dbt_env
from utils.dag_utils import register_failure_callbacks

# ═════════════════════════════════════════════════════════════════
# Configuration
# ═════════════════════════════════════════════════════════════════

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 1),
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
    "execution_timeout": timedelta(hours=1),
}

# ═════════════════════════════════════════════════════════════════
# Python Callables
# ═════════════════════════════════════════════════════════════════


def extract_service_quality(**context):
    """Extract quarterly service quality indicators from IDFM (X1 reliability layer)."""
    import sys

    sys.path.insert(0, INGESTION_DIR)
    from extract_service_quality import extract_service_quality

    extract_service_quality(gcs_bucket=GCS_BUCKET_RAW)
    print("✅ Service quality indicators extracted")


def extract_hourly_profiles(**context):
    """Extract quarterly hourly validation profiles from IDFM (X5 reliability layer)."""
    import sys

    sys.path.insert(0, INGESTION_DIR)
    from extract_hourly_profiles import extract_hourly_profiles

    extract_hourly_profiles(gcs_bucket=GCS_BUCKET_RAW)
    print("✅ Hourly validation profiles extracted")


def load_quarterly_to_bigquery(**context):
    """Load quarterly NDJSON snapshots to BigQuery RAW (WRITE_TRUNCATE)."""
    import sys

    sys.path.insert(0, INGESTION_DIR)
    from load_bigquery_raw import BigQueryLoader

    loader = BigQueryLoader()
    loader.load_service_quality()  # raw_service_quality — WRITE_TRUNCATE
    loader.load_hourly_profiles()  # raw_hourly_profiles — WRITE_TRUNCATE
    print("✅ Quarterly data loaded to BigQuery RAW tables")


# ═════════════════════════════════════════════════════════════════
# DAG Definition
# ═════════════════════════════════════════════════════════════════

with DAG(
    dag_id="transport_quarterly_pipeline",
    default_args=default_args,
    description="Quarterly pipeline: service quality (X1) + hourly profiles (X5)",
    # 6 AM on the 1st of Feb, May, Aug, Nov (one month after quarter end)
    schedule_interval="0 6 1 2,5,8,11 *",
    catchup=False,
    tags=["transport", "quarterly", "reliability"],
    max_active_runs=1,
) as dag:

    # ─────────────────────────────────────────────────────────────
    # STAGE 1: Extract (parallel)
    # ─────────────────────────────────────────────────────────────

    extract_service_quality_task = PythonOperator(
        task_id="extract_service_quality",
        python_callable=extract_service_quality,
        provide_context=True,
    )

    extract_hourly_profiles_task = PythonOperator(
        task_id="extract_hourly_profiles",
        python_callable=extract_hourly_profiles,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 2: Load to BigQuery RAW
    # ─────────────────────────────────────────────────────────────

    load_bigquery_task = PythonOperator(
        task_id="load_bigquery_raw",
        python_callable=load_quarterly_to_bigquery,
        provide_context=True,
    )

    # ─────────────────────────────────────────────────────────────
    # STAGE 3: dbt build — reliability layer only
    # Targets: fct_service_quality_quarterly + stg_service_quality_quarterly
    #          stg_hourly_profiles
    # NOTE: fct_incidents_daily, mart_line_reliability_scorecard,
    #       mart_station_reliability_scorecard are built by transport_daily_pipeline
    #       once raw_incidents_daily is populated.
    # ─────────────────────────────────────────────────────────────

    dbt_deps_task = BashOperator(
        task_id="dbt_deps",
        bash_command=dbt_command("deps"),
        env=dbt_env(),
    )

    dbt_build_task = BashOperator(
        task_id="dbt_build_quarterly",
        bash_command=dbt_command(
            "build --target prod "
            "--select stg_service_quality_quarterly "
            "fct_service_quality_quarterly "
            "stg_hourly_profiles"
        ),
        env=dbt_env(),
    )

    # ─────────────────────────────────────────────────────────────
    # Task Dependencies
    # ─────────────────────────────────────────────────────────────

    [extract_service_quality_task, extract_hourly_profiles_task] >> load_bigquery_task
    load_bigquery_task >> dbt_deps_task >> dbt_build_task


# ═════════════════════════════════════════════════════════════════
# Error Handling
# ═════════════════════════════════════════════════════════════════

register_failure_callbacks(dag)
