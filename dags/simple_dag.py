from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import json

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

BUCKET_NAME = 'dev'
OBJECT_KEY = 'data/api_data.json'
MINIO_BASE_URL = 'http://minio:9000'  # Замени на свой URL MinIO или S3-совместимого сервиса

class SimpleS3Hook(BaseHook):
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url.rstrip('/')

    def upload_bytes(self, bucket_name, object_key, data_bytes, content_type='application/octet-stream'):
        url = f"{self.base_url}/{bucket_name}/{object_key}"
        headers = {'Content-Type': content_type}
        response = requests.put(url, data=data_bytes, headers=headers)
        response.raise_for_status()
        self.log.info(f"Uploaded to {url} with status {response.status_code}")
        return response.status_code

def fetch_api_data(**kwargs):
    ti = kwargs['ti']
    url = 'https://jsonplaceholder.typicode.com/todos/1'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    ti.xcom_push(key='api_data', value=data)

def upload_to_s3_without_aws(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(key='api_data', task_ids='fetch_api_data')
    json_bytes = json.dumps(data).encode('utf-8')

    hook = SimpleS3Hook(base_url=MINIO_BASE_URL)
    hook.upload_bytes(bucket_name=BUCKET_NAME, object_key=OBJECT_KEY, data_bytes=json_bytes, content_type='application/json')

with DAG(
    dag_id='api_to_minio_without_aws',
    default_args=default_args,
    description='DAG получает данные из API и загружает в MinIO без AWS SDK',
    schedule_interval='@hourly',
    catchup=False,
    tags=['example'],
) as dag:

    t1 = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
    )

    t2 = PythonOperator(
        task_id='upload_to_s3_without_aws',
        python_callable=upload_to_s3_without_aws,
    )

    t1 >> t2