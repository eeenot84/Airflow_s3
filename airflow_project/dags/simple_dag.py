from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def hello_airflow():
    print("Hello, Airflow!")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'simple_hello_dag',
    default_args=default_args,
    description='Простой DAG с выводом в лог',
    schedule_interval='*/1 * * * *',  # каждую минуту
    catchup=False,
) as dag:

    task_hello = PythonOperator(
        task_id='say_hello',
        python_callable=hello_airflow
    )