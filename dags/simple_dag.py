from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
from requests.auth import HTTPBasicAuth

# --- Константы ---
MINIO_BASE_URL = 'http://minio:9000'  # URL MinIO
BUCKET_NAME = 'dev'
OBJECT_KEY = 'data/api_data.json'
MINIO_ACCESS_KEY = 'Rinat'
MINIO_SECRET_KEY = '8241882418Rinat'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def fetch_api_data(**kwargs):
    """Получает данные из API и сохраняет в XCom"""
    url = 'https://jsonplaceholder.typicode.com/todos/1'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    kwargs['ti'].xcom_push(key='api_data', value=data)

def upload_to_minio(**kwargs):
    """Загружает данные в MinIO с использованием requests и HTTP Basic Auth"""
    ti = kwargs['ti']
    data = ti.xcom_pull(key='api_data', task_ids='fetch_api_data')
    json_bytes = json.dumps(data).encode('utf-8')

    url = f"{MINIO_BASE_URL}/{BUCKET_NAME}/{OBJECT_KEY}"
    headers = {'Content-Type': 'application/json'}
    auth = HTTPBasicAuth(MINIO_ACCESS_KEY, MINIO_SECRET_KEY)

    response = requests.put(url, data=json_bytes, headers=headers, auth=auth)
    response.raise_for_status()

    print(f"✔️ Uploaded to {url} — Status: {response.status_code}")

# --- DAG ---
with DAG(
    dag_id='api_to_minio_without_aws',
    default_args=default_args,
    description='Получает данные из API и сохраняет в MinIO напрямую',
    schedule_interval='@hourly',
    catchup=False,
    tags=['minio', 'api', 'requests'],
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )

    upload_task = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
    )

    fetch_task >> upload_task