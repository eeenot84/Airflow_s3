from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from datetime import timedelta
import logging
import json
import requests
import os
import subprocess
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


def run_spark_job(ti: TaskInstance) -> None:
    """Запускает Spark job для обработки данных из MinIO и загрузки в PostgreSQL"""
    spark_script_path = "/opt/airflow/scripts/spark_jobs.py"
    
    logging.info(f"🚀 Запуск скрипта PySpark: {spark_script_path}")
    
    try:
        # Проверяем существование файла
        if not os.path.exists(spark_script_path):
            raise FileNotFoundError(f"❌ Скрипт не найден: {spark_script_path}")
        
        # Запускаем Spark job
        result = subprocess.run(
            ["python3", spark_script_path],
            capture_output=True,
            text=True,
            check=True,
            env=dict(os.environ)  # Передаем все переменные окружения
        )
        
        logging.info("✅ PySpark job успешно выполнен")
        logging.info(f"📄 Вывод: {result.stdout}")
        
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ PySpark завершился с ошибкой:")
        logging.error(f"📄 stdout: {e.stdout}")
        logging.error(f"📄 stderr: {e.stderr}")
        raise
    except Exception as e:
        logging.error(f"❌ Неожиданная ошибка при запуске PySpark: {e}")
        raise


with DAG(
    dag_id='api_to_minio_and_postgres',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',
    catchup=False,
    description='DAG для загрузки данных из API в MinIO и обработки через Spark в PostgreSQL',
    tags=['minio', 'api', 'spark', 'postgres'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_api_data,
    )

    upload_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
    )

    spark_task = PythonOperator(
        task_id='run_spark_job',
        python_callable=run_spark_job,
    )

    fetch_task >> upload_task >> spark_task