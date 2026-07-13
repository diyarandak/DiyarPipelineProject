import os
from pathlib import Path
from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, ExecutionConfig  # type: ignore

ICEBERG_PKG = os.getenv("ICEBERG_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3")

# dbt Paths
DEFAULT_DBT_ROOT_PATH = Path("/opt/airflow/olist_dbt")
DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

# Cosmos Profile Config using our profiles.yml
profile_config = ProfileConfig(
    profile_name="olist_dbt",
    target_name="dev",
    profiles_yml_filepath=DBT_ROOT_PATH / "profiles.yml"
)

# Mock Callback Functions for demonstration
def on_failure_callback(context):
    print(f"🚨 ALERT: Task {context.get('task_instance').task_id} failed!")
    # In a real scenario, this would send a Slack/Teams or Email alert.

def on_sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    print(f"⏰ SLA MISS: The pipeline took too long to complete. Missed SLAs: {slas}")

# Default settings for the DAG (Enterprise Grade)
default_args = {
    'owner': 'diyarandak',
    'depends_on_past': True, # Enforce strict chronological execution
    'email_on_failure': False, # Set true when SMTP is configured
    'email_on_retry': False,
    'retries': 3, # Exponential backoff strategy
    'retry_delay': timedelta(minutes=2),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
    'on_failure_callback': on_failure_callback,
    'sla': timedelta(hours=2) # Entire DAG should finish within 2 hours
}

# Define the DAG
with DAG(
    'olist_medallion_pipeline',
    default_args=default_args,
    description='Automated pipeline for Olist E-Commerce Data (Bronze -> Silver -> Gold)',
    schedule_interval='0 3 * * *', # Runs every day at 03:00 AM
    start_date=datetime(2026, 7, 1),
    catchup=False,
    max_active_runs=1, # Prevent concurrent runs to avoid database locks
    sla_miss_callback=on_sla_miss_callback,
    tags=['olist', 'medallion', 'spark', 'iceberg', 'dbt', 'cosmos'],
) as dag:

    # TASK 1: Bronze Ingestion
    # Submits the Spark job to ingest raw CSV data into Bronze Iceberg tables.
    bronze_task = BashOperator(
        task_id='bronze_ingestion',
        bash_command=f'spark-submit --packages {ICEBERG_PKG} /opt/airflow/processing/bronze_ingestion.py'
    )

    # TASK 2: dbt Transformations (Silver & Gold) via Cosmos
    # Automatically parses our dbt project and creates a task for every model/test!
    dbt_tg = DbtTaskGroup(
        group_id="dbt_transformations",
        project_config=ProjectConfig(DBT_ROOT_PATH),
        profile_config=profile_config,
        execution_config=ExecutionConfig(dbt_executable_path="dbt"),
        dag=dag
    )

    # DEFINE PIPELINE FLOW (Dependencies)
    bronze_task >> dbt_tg
