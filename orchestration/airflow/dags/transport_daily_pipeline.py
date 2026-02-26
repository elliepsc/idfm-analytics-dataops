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

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta

# ═════════════════════════════════════════════════════════════════
# Configuration
# ═════════════════════════════════════════════════════════════════

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# ═════════════════════════════════════════════════════════════════
# Python Callables
# ═════════════════════════════════════════════════════════════════

def extract_validations(**context):
    """Extract ticket validation data from IDFM API"""
    import sys
    sys.path.insert(0, '/opt/airflow/ingestion')
    from extract_validations import extract_validations
    
    # Get execution date (yesterday for daily run)
    execution_date = context['ds']  # Format: YYYY-MM-DD
    
    extract_validations(
        start_date=execution_date,
        end_date=execution_date,
        output_dir='/opt/airflow/data/bronze/validations'
    )
    print(f"✅ Validations extracted for {execution_date}")


def extract_punctuality(**context):
    """Extract train punctuality data from Transilien API"""
    import sys
    sys.path.insert(0, '/opt/airflow/ingestion')
    from extract_punctuality import extract_punctuality
    
    execution_date = context['ds']
    # Punctuality is monthly, so extract full month
    month_start = execution_date[:7] + '-01'  # First day of month
    
    extract_punctuality(
        start_date=month_start,
        end_date=execution_date,
        output_dir='/opt/airflow/data/bronze/punctuality'
    )
    print(f"✅ Punctuality extracted for month starting {month_start}")


def extract_referentials(**context):
    """Extract reference data: stops, lines, and stop-line mappings"""
    import sys
    sys.path.insert(0, '/opt/airflow/ingestion')
    from extract_ref_stops import extract_ref_stops
    from extract_ref_lines import extract_ref_lines
    from extract_ref_stop_lines import extract_ref_stop_lines
    
    # Reference data changes infrequently, extract all
    extract_ref_stops(output_dir='/opt/airflow/data/bronze/referentials')
    extract_ref_lines(output_dir='/opt/airflow/data/bronze/referentials')
    extract_ref_stop_lines(output_dir='/opt/airflow/data/bronze/referentials')
    
    print("✅ Reference data extracted (stops, lines, mappings)")


def load_to_bigquery(**context):
    """Load JSON files to BigQuery RAW tables"""
    import sys
    sys.path.insert(0, '/opt/airflow/ingestion')
    from load_bigquery_raw import BigQueryLoader
    
    loader = BigQueryLoader()
    loader.load_all()
    
    print("✅ All data loaded to BigQuery RAW tables")


def check_sla(**context):
    """Validate data quality: freshness, completeness, validity"""
    import sys
    sys.path.insert(0, '/opt/airflow/scripts')
    from check_sla import check_sla
    
    exit_code = check_sla()
    
    if exit_code != 0:
        raise Exception("❌ SLA breach detected - data quality issues")
    
    print("✅ All SLA checks passed")


# ═════════════════════════════════════════════════════════════════
# DAG Definition
# ═════════════════════════════════════════════════════════════════

with DAG(
    dag_id='transport_daily_pipeline',
    default_args=default_args,
    description='Daily pipeline: Extract → Load → Transform → Validate',
    schedule_interval='0 2 * * *',  # 2 AM every day
    catchup=False,  # Don't backfill on enable
    tags=['transport', 'daily', 'production'],
    max_active_runs=1,  # Only one run at a time
) as dag:
    
    # ─────────────────────────────────────────────────────────────
    # STAGE 1: Extract (parallel execution for speed)
    # ─────────────────────────────────────────────────────────────
    
    extract_validations_task = PythonOperator(
        task_id='extract_validations',
        python_callable=extract_validations,
        provide_context=True,
    )
    
    extract_punctuality_task = PythonOperator(
        task_id='extract_punctuality',
        python_callable=extract_punctuality,
        provide_context=True,
    )
    
    extract_referentials_task = PythonOperator(
        task_id='extract_referentials',
        python_callable=extract_referentials,
        provide_context=True,
    )
    
    # ─────────────────────────────────────────────────────────────
    # STAGE 2: Load to BigQuery RAW
    # ─────────────────────────────────────────────────────────────
    
    load_bigquery_task = PythonOperator(
        task_id='load_bigquery_raw',
        python_callable=load_to_bigquery,
        provide_context=True,
    )
    
    # ─────────────────────────────────────────────────────────────
    # STAGE 3: Transform with dbt (run + test)
    # ─────────────────────────────────────────────────────────────
    
    dbt_build_task = BashOperator(
        task_id='dbt_build',
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            dbt deps && \
            dbt build --target prod --select +marts
        """,
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/warehouse/dbt',
            'GOOGLE_APPLICATION_CREDENTIALS': '{{ var.value.gcp_credentials_path }}',
        },
    )
    
    # ─────────────────────────────────────────────────────────────
    # STAGE 4: Validate data quality (SLA checks)
    # ─────────────────────────────────────────────────────────────
    
    check_sla_task = PythonOperator(
        task_id='check_sla',
        python_callable=check_sla,
        provide_context=True,
    )
    
    # ─────────────────────────────────────────────────────────────
    # STAGE 5: Notify success
    # ─────────────────────────────────────────────────────────────
    
    notify_success = SlackWebhookOperator(
        task_id='notify_success',
        slack_webhook_conn_id='slack_webhook',
        message="""
✅ *Transport Daily Pipeline - SUCCESS*

📅 Date: {{ ds }}
⏱️ Duration: {{ task_instance.duration }}s
🎯 All data quality checks passed
        """,
        channel='#data-alerts',
    )
    
    # ─────────────────────────────────────────────────────────────
    # Task Dependencies (DAG structure)
    # ─────────────────────────────────────────────────────────────
    
    # Parallel extraction, then sequential processing
    [extract_validations_task, extract_punctuality_task, extract_referentials_task] >> load_bigquery_task
    
    # Sequential: Load → Transform → Validate → Notify
    load_bigquery_task >> dbt_build_task >> check_sla_task >> notify_success


# ═════════════════════════════════════════════════════════════════
# Error Handling
# ═════════════════════════════════════════════════════════════════

def task_failure_alert(context):
    """
    Callback function executed when any task fails
    Sends detailed error message to Slack
    """
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    
    slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_webhook')
    
    message = f"""
❌ *Transport Daily Pipeline - FAILED*

📅 Date: {context['ds']}
🔧 Task: {context['task_instance'].task_id}
⚠️ Error: {context['exception']}
🔗 Log: {context['task_instance'].log_url}
    """
    
    slack_hook.send(text=message, channel='#data-alerts')


# Apply failure callback to all tasks
for task in dag.tasks:
    task.on_failure_callback = task_failure_alert
