from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
from botocore.client import Config
from io import BytesIO
import json
import requests
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
import logging

# --- Настройки MinIO ---
MINIO_ENDPOINT = 'http://minio:9000'
MINIO_ACCESS_KEY = 'minioadmin'
MINIO_SECRET_KEY = 'minioadmin'
BUCKET_NAME = 'dev'
OBJECT_KEY = 'data/api_data.json'

# --- DAG параметры ---
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


# --- Получение данных из API ---
def fetch_api_data(ti: TaskInstance) -> None:
    url = "https://jsonplaceholder.typicode.com/todos/1"
    logging.info(f"🔍 Запрос данных с {url}")

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    logging.info("✅ Данные получены, сохраняем в XCom")
    ti.xcom_push(key='api_data', value=data)


# --- Загрузка в MinIO ---
def upload_to_minio(ti: TaskInstance) -> None:
    logging.info("📦 Получаем данные из XCom")
    data = ti.xcom_pull(task_ids='fetch_data_task', key='api_data')
    content = json.dumps(data).encode("utf-8")

    logging.info("🔗 Подключаемся к MinIO")
    s3 = boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
    )

    # Создаём бакет, если не существует
    buckets = s3.list_buckets().get('Buckets', [])
    bucket_names = [b['Name'] for b in buckets]
    if BUCKET_NAME not in bucket_names:
        logging.info(f"🪣 Создаём бакет: {BUCKET_NAME}")
        s3.create_bucket(Bucket=BUCKET_NAME)

    logging.info(f"⬆️ Загружаем объект: {OBJECT_KEY}")
    s3.upload_fileobj(
        Fileobj=BytesIO(content),
        Bucket=BUCKET_NAME,
        Key=OBJECT_KEY,
        ExtraArgs={'ContentType': 'application/json'}
    )
    logging.info("✅ Загрузка завершена")


# --- DAG ---
with DAG(
        dag_id='api_to_minio_v2',
        default_args=default_args,
        start_date=days_ago(1),
        schedule_interval='@daily',
        catchup=False,
        description='DAG для загрузки данных из API в MinIO',
        tags=['minio', 'api'],
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