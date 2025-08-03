from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
from io import BytesIO
import json
import requests


def fetch_api_data():
    """Функция для получения данных с API"""
    url = "https://jsonplaceholder.typicode.com/todos/1"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    # Сохраняем данные в XCom
    return data


def upload_to_minio(ti):
    """Функция для загрузки данных в MinIO"""
    data = ti.xcom_pull(task_ids="fetch_data_task")

    # Преобразуем данные в байты
    content = json.dumps(data).encode("utf-8")

    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
    )

    bucket_name = 'dev'
    object_key = 'data/api_data.json'

    # Проверяем и создаём бакет, если нужно
    existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if bucket_name not in existing_buckets:
        s3.create_bucket(Bucket=bucket_name)

    # Загрузка файла
    s3.upload_fileobj(
        Fileobj=BytesIO(content),
        Bucket=bucket_name,
        Key=object_key,
        ExtraArgs={'ContentType': 'application/json'}
    )


default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='api_to_minio_without_aws',
    default_args=default_args,
    start_date=datetime(2025, 8, 3),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_api_data,
    )

    upload_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
    )

    fetch_task >> upload_task