FROM apache/airflow:2.9.1

USER root

# Копируем зависимости
COPY requirements.txt /requirements.txt

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r /requirements.txt

# Копируем DAG-файлы
COPY ./dags/ /opt/airflow/dags/

USER airflow