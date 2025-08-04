FROM apache/airflow:2.9.1

# Обновляем pip
RUN python3 -m pip install --upgrade pip

# Копируем файл зависимостей
COPY requirements.txt /

# Устанавливаем переменные для указания версии Airflow и Python
ARG AIRFLOW_VERSION=2.9.1
ARG PYTHON_VERSION=3.9

# Устанавливаем URL constraints-файла (совместимые зависимости)
ENV CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Устанавливаем зависимости с учётом constraints
RUN pip install --no-cache-dir -r /requirements.txt --constraint "${CONSTRAINT_URL}"