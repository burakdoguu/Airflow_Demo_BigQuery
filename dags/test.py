from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
import os
from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator


with DAG('send_file', start_date=datetime(2022,1,1),
    schedule_interval='@daily',catchup=False) as dag:

    upload_file = LocalFilesystemToGCSOperator(
        task_id ="upload_file",
        bucket='test_local_file',
        src='/opt/airflow/dags/resources/food_daily.csv',
        dst='test_local_file'
    )

