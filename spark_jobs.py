# spark_jobs.py
import json
import boto3
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp

# -------------------------------
# 1️⃣ Настройка MinIO
# -------------------------------
minio_endpoint = "http://localhost:9000"
access_key = "Rinat"
secret_key = "8241882418Rinat"
bucket_name = "dev"
object_key = "data/temperature.json"

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
jdbc_driver_path = "/Users/rinatmubinov/PycharmProjects/PythonProject6/libs/postgresql-42.7.7.jar"
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
# 5️⃣ Подключение к PostgreSQL
# -------------------------------
jdbc_url = "jdbc:postgresql://localhost:5433/airflow"  # локальный порт PostgreSQL
jdbc_props = {
    "user": "airflow",
    "password": "airflow",
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