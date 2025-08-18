from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from datetime import timedelta
import logging
import json
import requests
import os
from io import BytesIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import BotoCoreError, ClientError

# Имя подключения, настроенного в Airflow Admin > Connections
AIRFLOW_CONN_ID = 'minio_default'

# Получаем конфигурацию из переменных окружения
BUCKET_NAME = os.getenv('MINIO_BUCKET', 'dev')
OBJECT_KEY = 'data/temperature.json'
API_URL = os.getenv('TEMPERATURE_API_URL', 'https://api.data.gov.sg/v1/environment/air-temperature')

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}


def fetch_api_data(ti: TaskInstance) -> None:
    logging.info(f"🔍 Запрос данных с {API_URL}")

    try:
        response = requests.get(API_URL)
        response.raise_for_status()
        data = response.json()
        logging.info("✅ Данные получены")
        ti.xcom_push(key='api_data', value=data)
    except requests.RequestException as e:
        logging.error(f"❌ Ошибка при получении данных: {e}")
        raise


def upload_to_minio(ti: TaskInstance) -> None:
    logging.info("📦 Получаем данные из XCom")
    data = ti.xcom_pull(task_ids='fetch_data_task', key='api_data')

    if data is None:
        logging.error("❌ Нет данных в XCom — завершение")
        raise ValueError("Нет данных для загрузки")

    content = json.dumps(data).encode("utf-8")

    try:
        logging.info("🔗 Подключаемся к MinIO через Airflow S3Hook")
        s3_hook = S3Hook(aws_conn_id=AIRFLOW_CONN_ID)

        # Проверяем наличие бакета, создаём если нужно
        existing_buckets = [bucket['Name'] for bucket in s3_hook.get_conn().list_buckets().get('Buckets', [])]
        if BUCKET_NAME not in existing_buckets:
            logging.info(f"🪣 Создаём бакет: {BUCKET_NAME}")
            s3_hook.get_conn().create_bucket(Bucket=BUCKET_NAME)

        logging.info(f"⬆️ Загружаем объект: {OBJECT_KEY}")
        s3_hook.load_bytes(
            bytes_data=content,
            key=OBJECT_KEY,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        logging.info("✅ Загрузка в MinIO успешно завершена")

    except (BotoCoreError, ClientError) as e:
        logging.error(f"❌ Ошибка при работе с MinIO: {e}")
        raise


with DAG(
    dag_id='api_to_minio_with_conn',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    description='DAG для загрузки данных из API в MinIO через Airflow Connection',
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