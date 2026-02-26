"""
Historical Data Backfill DAG

Schedule: Manual trigger only
Purpose: Load historical data for a date range

Use case: Initial data load or filling gaps in historical data
          Example: Load all data from 2023-01-01 to 2023-12-31

Trigger with parameters:
    airflow dags trigger transport_backfill --conf '{"start_date":"2023-01-01", "end_date":"2023-12-31"}'
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from datetime import datetime, timedelta
import pendulum

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
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=6),  # Longer timeout for backfills
}

# ═════════════════════════════════════════════════════════════════
# Python Callables
# ═════════════════════════════════════════════════════════════════

def extract_validations_backfill(**context):
    """
    Extract validations for entire date range
    Date range comes from DAG run configuration
    """
    import sys
    sys.path.insert(0, '/opt/airflow/ingestion')
    from extract_validations import extract_validations
    
    # Get date range from DAG run config
    dag_run = context['dag_run']
    start_date = dag_run.conf.get('start_date', '2024-01-01')
    end_date = dag_run.conf.get('end_date', '2024-01-31')
    
    print(f"📅 Backfilling validations from {start_date} to {end_date}")
    
    extract_validations(
        start_date=start_date,
        end_date=end_date,
        output_dir='/opt/airflow/data/bronze/validations'
    )
    
    print(f"✅ Validations backfill complete: {start_date} to {end_date}")


def extract_punctuality_backfill(**context):
    """
    Extract punctuality for entire date range
    Punctuality is monthly, so we extract by month
    """
    import sys
    sys.path.insert(0, '/opt/airflow/ingestion')
    from extract_punctuality import extract_punctuality
    from dateutil.relativedelta import relativedelta
    import pendulum
    
    # Get date range from DAG run config
    dag_run = context['dag_run']
    start_date_str = dag_run.conf.get('start_date', '2024-01-01')
    end_date_str = dag_run.conf.get('end_date', '2024-01-31')
    
    start_date = pendulum.parse(start_date_str)
    end_date = pendulum.parse(end_date_str)
    
    print(f"📅 Backfilling punctuality from {start_date_str} to {end_date_str}")
    
    # Extract month by month
    current = start_date.start_of('month')
    while current <= end_date:
        month_start = current.format('YYYY-MM-DD')
        month_end = current.end_of('month').format('YYYY-MM-DD')
        
        print(f"  Extracting month: {current.format('YYYY-MM')}")
        
        extract_punctuality(
            start_date=month_start,
            end_date=month_end,
            output_dir='/opt/airflow/data/bronze/punctuality'
        )
        
        current = current.add(months=1)
    
    print(f"✅ Punctuality backfill complete: {start_date_str} to {end_date_str}")


def extract_referentials_backfill(**context):
    """
    Extract reference data (stops, lines, mappings)
    Reference data doesn't change by date, so extract once
    """
    import sys
    sys.path.insert(0, '/opt/airflow/ingestion')
    from extract_ref_stops import extract_ref_stops
    from extract_ref_lines import extract_ref_lines
    from extract_ref_stop_lines import extract_ref_stop_lines
    
    print("📅 Extracting reference data (stops, lines, mappings)")
    
    extract_ref_stops(output_dir='/opt/airflow/data/bronze/referentials')
    extract_ref_lines(output_dir='/opt/airflow/data/bronze/referentials')
    extract_ref_stop_lines(output_dir='/opt/airflow/data/bronze/referentials')
    
    print("✅ Reference data extraction complete")


def load_to_bigquery_backfill(**context):
    """Load all extracted JSON files to BigQuery RAW"""
    import sys
    sys.path.insert(0, '/opt/airflow/ingestion')
    from load_bigquery_raw import BigQueryLoader
    
    print("📥 Loading all JSON files to BigQuery RAW")
    
    loader = BigQueryLoader()
    loader.load_all()
    
    print("✅ All data loaded to BigQuery RAW")


def validate_backfill(**context):
    """
    Validate backfill completeness
    Check that data exists for all dates in range
    """
    from google.cloud import bigquery
    import pendulum
    
    # Get date range from DAG run config
    dag_run = context['dag_run']
    start_date_str = dag_run.conf.get('start_date', '2024-01-01')
    end_date_str = dag_run.conf.get('end_date', '2024-01-31')
    
    start_date = pendulum.parse(start_date_str)
    end_date = pendulum.parse(end_date_str)
    
    client = bigquery.Client()
    
    # Count records per date
    query = f"""
    SELECT 
        date,
        COUNT(*) as record_count
    FROM `transport_raw.raw_validations`
    WHERE date BETWEEN '{start_date_str}' AND '{end_date_str}'
    GROUP BY date
    ORDER BY date
    """
    
    results = client.query(query).result()
    
    # Check for missing dates
    dates_with_data = {row.date for row in results}
    expected_dates = set()
    current = start_date
    while current <= end_date:
        expected_dates.add(current.date())
        current = current.add(days=1)
    
    missing_dates = expected_dates - dates_with_data
    
    if missing_dates:
        missing_str = ', '.join(str(d) for d in sorted(missing_dates))
        print(f"⚠️  WARNING: Missing data for dates: {missing_str}")
    else:
        print(f"✅ All dates have data from {start_date_str} to {end_date_str}")
    
    return {'missing_dates': len(missing_dates), 'total_dates': len(expected_dates)}


# ═════════════════════════════════════════════════════════════════
# DAG Definition
# ═════════════════════════════════════════════════════════════════

with DAG(
    dag_id='transport_backfill',
    default_args=default_args,
    description='Historical data backfill (manual trigger)',
    schedule_interval=None,  # Manual trigger only
    catchup=False,
    tags=['transport', 'backfill', 'manual'],
    max_active_runs=1,
    params={
        'start_date': '2024-01-01',
        'end_date': '2024-01-31',
    }
) as dag:
    
    # ─────────────────────────────────────────────────────────────
    # STAGE 1: Extract historical data (parallel)
    # ─────────────────────────────────────────────────────────────
    
    extract_validations_task = PythonOperator(
        task_id='extract_validations_backfill',
        python_callable=extract_validations_backfill,
        provide_context=True,
    )
    
    extract_punctuality_task = PythonOperator(
        task_id='extract_punctuality_backfill',
        python_callable=extract_punctuality_backfill,
        provide_context=True,
    )
    
    extract_referentials_task = PythonOperator(
        task_id='extract_referentials_backfill',
        python_callable=extract_referentials_backfill,
        provide_context=True,
    )
    
    # ─────────────────────────────────────────────────────────────
    # STAGE 2: Load to BigQuery RAW
    # ─────────────────────────────────────────────────────────────
    
    load_bigquery_task = PythonOperator(
        task_id='load_bigquery_backfill',
        python_callable=load_to_bigquery_backfill,
        provide_context=True,
    )
    
    # ─────────────────────────────────────────────────────────────
    # STAGE 3: Transform with dbt (full refresh)
    # ─────────────────────────────────────────────────────────────
    
    dbt_build_task = BashOperator(
        task_id='dbt_build_full_refresh',
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            dbt deps && \
            dbt build --target prod --full-refresh
        """,
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/warehouse/dbt',
            'GOOGLE_APPLICATION_CREDENTIALS': '{{ var.value.gcp_credentials_path }}',
        },
    )
    
    # ─────────────────────────────────────────────────────────────
    # STAGE 4: Validate completeness
    # ─────────────────────────────────────────────────────────────
    
    validate_task = PythonOperator(
        task_id='validate_backfill_completeness',
        python_callable=validate_backfill,
        provide_context=True,
    )
    
    # ─────────────────────────────────────────────────────────────
    # STAGE 5: Success notification
    # ─────────────────────────────────────────────────────────────
    
    notify_success = SlackWebhookOperator(
        task_id='notify_success',
        slack_webhook_conn_id='slack_webhook',
        message="""
✅ *Transport Backfill - SUCCESS*

📅 Date Range: {{ dag_run.conf.get('start_date') }} to {{ dag_run.conf.get('end_date') }}
⏱️ Duration: {{ task_instance.duration }}s
🎯 Historical data loaded and transformed
        """,
        channel='#data-alerts',
    )
    
    # ─────────────────────────────────────────────────────────────
    # Task Dependencies
    # ─────────────────────────────────────────────────────────────
    
    # Parallel extraction, then sequential processing
    [extract_validations_task, extract_punctuality_task, extract_referentials_task] >> load_bigquery_task
    
    # Sequential: Load → Transform → Validate → Notify
    load_bigquery_task >> dbt_build_task >> validate_task >> notify_success


# ═════════════════════════════════════════════════════════════════
# Error Handling
# ═════════════════════════════════════════════════════════════════

def task_failure_alert(context):
    """Send alert on task failure"""
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    
    slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_webhook')
    
    message = f"""
❌ *Transport Backfill - FAILED*

📅 Date Range: {context['dag_run'].conf.get('start_date')} to {context['dag_run'].conf.get('end_date')}
🔧 Task: {context['task_instance'].task_id}
⚠️ Error: {context['exception']}
🔗 Log: {context['task_instance'].log_url}
    """
    
    slack_hook.send(text=message, channel='#data-alerts')


# Apply failure callback to all tasks
for task in dag.tasks:
    task.on_failure_callback = task_failure_alert
