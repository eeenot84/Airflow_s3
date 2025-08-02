FROM apache/airflow:2.9.1

USER root
RUN apt-get update && apt-get install -y python3-distutils
USER airflow

# Обновляем pip уже под airflow пользователем
RUN python3 -m pip install --upgrade pip

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt


COPY ./dags/ /opt/airflow/dags/