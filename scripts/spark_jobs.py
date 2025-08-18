# spark_jobs.py
import json
import boto3
import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# -------------------------------
# 1️⃣ Настройка MinIO из переменных окружения
# -------------------------------
# В Docker окружении используем имя сервиса 'minio'
minio_endpoint = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
access_key = os.getenv("MINIO_ROOT_USER")
secret_key = os.getenv("MINIO_ROOT_PASSWORD")
bucket_name = os.getenv("MINIO_BUCKET", "dev")
object_key = "data/temperature.json"

# Проверяем наличие обязательных переменных
if not access_key or not secret_key:
    raise ValueError("❌ MinIO credentials not found in environment variables. Set MINIO_ROOT_USER and MINIO_ROOT_PASSWORD.")

logging.info(f"🔗 Подключение к MinIO: {minio_endpoint}")

s3 = boto3.client(
    's3',
    endpoint_url=minio_endpoint,
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# -------------------------------
# 2️⃣ Получаем объект из MinIO
# -------------------------------
obj = s3.get_object(Bucket=bucket_name, Key=object_key)
data = json.loads(obj['Body'].read())

# -------------------------------
# 3️⃣ Инициализация PySpark с драйвером PostgreSQL
# -------------------------------
jdbc_driver_path = os.getenv("JDBC_DRIVER_PATH", "/opt/libs/postgresql-42.7.7.jar")
spark = SparkSession.builder \
    .appName("MinIO_to_Postgres") \
    .config("spark.jars", jdbc_driver_path) \
    .getOrCreate()

# -------------------------------
# 4️⃣ Преобразование JSON в DataFrame
# -------------------------------
stations_list = [
    (s["id"], s["device_id"], s["name"], s["location"]["latitude"], s["location"]["longitude"])
    for s in data["metadata"]["stations"]
]
stations_df = spark.createDataFrame(stations_list, ["station_id", "device_id", "name", "latitude", "longitude"])

readings_list = []
for item in data["items"]:
    ts = item["timestamp"]
    for r in item["readings"]:
        readings_list.append((r["station_id"], ts, r["value"]))

readings_df = spark.createDataFrame(readings_list, ["station_id", "timestamp", "value"])

# Преобразуем timestamp в TimestampType для PostgreSQL
readings_df = readings_df.withColumn(
    "timestamp",
    to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ssXXX")
)

# -------------------------------
# 5️⃣ Подключение к PostgreSQL из переменных окружения
# -------------------------------
# В Docker окружении используем имя сервиса 'postgres', а не localhost
postgres_host = os.getenv("POSTGRES_HOST", "postgres")
postgres_port = os.getenv("POSTGRES_INTERNAL_PORT", "5432")  # Внутренний порт контейнера
postgres_db = os.getenv("POSTGRES_DB", "airflow")
postgres_user = os.getenv("POSTGRES_USER")
postgres_password = os.getenv("POSTGRES_PASSWORD")

# Проверяем наличие обязательных переменных
if not postgres_user or not postgres_password:
    raise ValueError("❌ PostgreSQL credentials not found in environment variables. Set POSTGRES_USER and POSTGRES_PASSWORD.")

jdbc_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
logging.info(f"🔗 Подключение к PostgreSQL: {postgres_host}:{postgres_port}/{postgres_db}")
jdbc_props = {
    "user": postgres_user,
    "password": postgres_password,
    "driver": "org.postgresql.Driver"
}

# -------------------------------
# 6️⃣ Загрузка данных в PostgreSQL
# -------------------------------
stations_df.write.jdbc(jdbc_url, "stations", mode="ignore", properties=jdbc_props)
readings_df.write.jdbc(jdbc_url, "readings", mode="append", properties=jdbc_props)

logging.info("✅ Данные успешно загружены в PostgreSQL")

# -------------------------------
# 7️⃣ Закрытие Spark
# -------------------------------
spark.stop()