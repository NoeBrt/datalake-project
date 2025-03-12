from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'kafka_to_s3_to_sql_pipeline',
    default_args=default_args,
    schedule_interval='*/15 * * * *',  # every 15 minutes
    catchup=False,
    description='Orchestrates staging (Kafka to S3) and curated (Parquet to SQL) steps'
)


# Sensor Task: Wait for a new Parquet file in S3 (using wildcards if needed)
wait_for_parquet = S3KeySensor(
    task_id='wait_for_parquet',
    bucket_key='data_*.parquet',  # Pattern to match new files
    bucket_name='staging',        # Change to your bucket name if needed
    wildcard_match=True,
    poke_interval=60,  # check every minute
    timeout=3600,      # fail after an hour if no file appears
    dag=dag,
)

# Task 2: Curated
curated = BashOperator(
    task_id='curated',
    bash_command='python /opt/airflow/scripts/process_curated.py',
    dag=dag,
)

# Set task dependencies: staging >> sensor >> curated
wait_for_parquet >> curated
