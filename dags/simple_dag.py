from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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

S3_CONN_ID = 'my_s3_conn'
BUCKET_NAME = 'my-bucket'
OBJECT_KEY = 'data/api_data.json'

def fetch_api_data(**context):
    url = 'https://jsonplaceholder.typicode.com/todos/1'
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    context['ti'].xcom_push(key='api_data', value=data)

def upload_to_s3(**context):
    data = context['ti'].xcom_pull(key='api_data', task_ids='fetch_api_data')
    json_bytes = json.dumps(data).encode('utf-8')

    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    s3_hook.load_bytes(
        bytes_data=json_bytes,
        key=OBJECT_KEY,
        bucket_name=BUCKET_NAME,
        replace=True,
    )
    print(f"Файл загружен в S3: s3://{BUCKET_NAME}/{OBJECT_KEY}")

with DAG(
    'api_to_s3_with_hook',
    default_args=default_args,
    description='DAG тянет данные из API и загружает в S3 через S3Hook',
    schedule_interval='@hourly',
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id='fetch_api_data',
        python_callable=fetch_api_data,
        provide_context=True,
    )

    t2 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        provide_context=True,
    )

    t1 >> t2