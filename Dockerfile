FROM apache/airflow:2.9.1

# Переключаемся на airflow-пользователя сразу, как требует Airflow-документация
USER airflow

# Копируем requirements.txt внутрь контейнера
COPY requirements.txt /requirements.txt

# Устанавливаем зависимости от имени пользователя airflow
RUN pip install --no-cache-dir -r /requirements.txt

# Копируем DAG-файлы
COPY ./dags/ /opt/airflow/dags/