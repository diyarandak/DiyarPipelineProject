import os
from datetime import datetime, timedelta
from airflow import DAG  # type: ignore
from airflow.operators.bash import BashOperator  # type: ignore

ICEBERG_PKG = os.getenv("ICEBERG_PACKAGE", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3")

# Default settings for the DAG
default_args = {
    'owner': 'diyarandak',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'olist_medallion_pipeline',
    default_args=default_args,
    description='Automated pipeline for Olist E-Commerce Data (Bronze -> Silver -> Gold)',
    schedule_interval='0 3 * * *', # Runs every day at 03:00 AM
    start_date=datetime(2026, 7, 1),
    catchup=False,
    tags=['olist', 'medallion', 'spark', 'iceberg'],
) as dag:

    # TASK 1: Bronze Ingestion
    # Submits the Spark job to ingest raw CSV data into Bronze Iceberg tables.
    bronze_task = BashOperator(
        task_id='bronze_ingestion',
        bash_command=f'spark-submit --packages {ICEBERG_PKG} /opt/airflow/processing/bronze_ingestion.py'
    )

    # TASK 2: Silver Transformation
    # Submits the Spark job to cleanse data (deduplication, casting) and write to Silver Iceberg tables.
    silver_task = BashOperator(
        task_id='silver_transformation',
        bash_command=f'spark-submit --packages {ICEBERG_PKG} /opt/airflow/processing/silver_transformation.py'
    )

    # TASK 3: Gold Modeling
    # Submits the Spark job to model Silver tables into a Star Schema (Fact & Dim) for BI.
    gold_task = BashOperator(
        task_id='gold_modeling',
        bash_command=f'spark-submit --packages {ICEBERG_PKG} /opt/airflow/processing/gold_modeling.py'
    )

    # DEFINE PIPELINE FLOW (Dependencies)
    # Bronze must succeed before Silver starts. Silver must succeed before Gold starts.
    bronze_task >> silver_task >> gold_task
