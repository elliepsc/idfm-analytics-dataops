"""
dbt Daily Build DAG

Schedule: Daily at 3 AM (after data ingestion)
Purpose: Run dbt transformations only (no extraction/loading)

Use case: When you want to re-run transformations without re-extracting data
          For example, after fixing a dbt model bug
"""

from airflow import DAG
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
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'execution_timeout': timedelta(hours=1),
}

# ═════════════════════════════════════════════════════════════════
# DAG Definition
# ═════════════════════════════════════════════════════════════════

with DAG(
    dag_id='dbt_daily',
    default_args=default_args,
    description='dbt transformations only (no extraction)',
    schedule_interval='0 3 * * *',  # 3 AM daily (after ingestion pipeline)
    catchup=False,
    tags=['dbt', 'transform', 'daily'],
    max_active_runs=1,
) as dag:
    
    # ─────────────────────────────────────────────────────────────
    # TASK 1: Install dbt dependencies
    # ─────────────────────────────────────────────────────────────
    
    dbt_deps = BashOperator(
        task_id='dbt_deps',
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            dbt deps --profiles-dir . --profile transport
        """,
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/warehouse/dbt',
            'GOOGLE_APPLICATION_CREDENTIALS': '{{ var.value.gcp_credentials_path }}',
        },
    )
    
    # ─────────────────────────────────────────────────────────────
    # TASK 2: Run staging models
    # ─────────────────────────────────────────────────────────────
    
    dbt_run_staging = BashOperator(
        task_id='dbt_run_staging',
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            dbt run --target prod --select staging
        """,
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/warehouse/dbt',
            'GOOGLE_APPLICATION_CREDENTIALS': '{{ var.value.gcp_credentials_path }}',
        },
    )
    
    # ─────────────────────────────────────────────────────────────
    # TASK 3: Run core models (dimensions + facts)
    # ─────────────────────────────────────────────────────────────
    
    dbt_run_core = BashOperator(
        task_id='dbt_run_core',
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            dbt run --target prod --select core
        """,
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/warehouse/dbt',
            'GOOGLE_APPLICATION_CREDENTIALS': '{{ var.value.gcp_credentials_path }}',
        },
    )
    
    # ─────────────────────────────────────────────────────────────
    # TASK 4: Run marts models (analytics)
    # ─────────────────────────────────────────────────────────────
    
    dbt_run_marts = BashOperator(
        task_id='dbt_run_marts',
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            dbt run --target prod --select marts
        """,
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/warehouse/dbt',
            'GOOGLE_APPLICATION_CREDENTIALS': '{{ var.value.gcp_credentials_path }}',
        },
    )
    
    # ─────────────────────────────────────────────────────────────
    # TASK 5: Run all tests
    # ─────────────────────────────────────────────────────────────
    
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            dbt test --target prod
        """,
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/warehouse/dbt',
            'GOOGLE_APPLICATION_CREDENTIALS': '{{ var.value.gcp_credentials_path }}',
        },
    )
    
    # ─────────────────────────────────────────────────────────────
    # TASK 6: Generate documentation
    # ─────────────────────────────────────────────────────────────
    
    dbt_docs_generate = BashOperator(
        task_id='dbt_docs_generate',
        bash_command="""
            cd /opt/airflow/warehouse/dbt && \
            dbt docs generate --target prod
        """,
        env={
            'DBT_PROFILES_DIR': '/opt/airflow/warehouse/dbt',
            'GOOGLE_APPLICATION_CREDENTIALS': '{{ var.value.gcp_credentials_path }}',
        },
    )
    
    # ─────────────────────────────────────────────────────────────
    # TASK 7: Success notification
    # ─────────────────────────────────────────────────────────────
    
    notify_success = SlackWebhookOperator(
        task_id='notify_success',
        slack_webhook_conn_id='slack_webhook',
        message="""
✅ *dbt Daily Build - SUCCESS*

📅 Date: {{ ds }}
⏱️ Duration: {{ task_instance.duration }}s
🎯 All models and tests passed
        """,
        channel='#data-alerts',
    )
    
    # ─────────────────────────────────────────────────────────────
    # Task Dependencies
    # ─────────────────────────────────────────────────────────────
    
    # Sequential execution through layers
    dbt_deps >> dbt_run_staging >> dbt_run_core >> dbt_run_marts >> dbt_test >> dbt_docs_generate >> notify_success


# ═════════════════════════════════════════════════════════════════
# Error Handling
# ═════════════════════════════════════════════════════════════════

def task_failure_alert(context):
    """Send alert on task failure"""
    from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook
    
    slack_hook = SlackWebhookHook(slack_webhook_conn_id='slack_webhook')
    
    message = f"""
❌ *dbt Daily Build - FAILED*

📅 Date: {context['ds']}
🔧 Task: {context['task_instance'].task_id}
⚠️ Error: {context['exception']}
🔗 Log: {context['task_instance'].log_url}
    """
    
    slack_hook.send(text=message, channel='#data-alerts')


# Apply failure callback to all tasks
for task in dag.tasks:
    task.on_failure_callback = task_failure_alert
