"""
DAG для анимации статичных фотографий.
Загружает изображения из MinIO, создает анимацию и сохраняет результат обратно.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import TaskInstance
from datetime import timedelta
import logging
import os
from io import BytesIO

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import BotoCoreError, ClientError
from photo_animator import PhotoAnimator

# Имя подключения MinIO
AIRFLOW_CONN_ID = 'minio_default'

# Конфигурация из переменных окружения
BUCKET_NAME = os.getenv('MINIO_BUCKET', 'dev')
INPUT_PREFIX = 'photos/input/'
OUTPUT_PREFIX = 'photos/animated/'

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

logger = logging.getLogger(__name__)


def list_photos_in_minio(ti: TaskInstance) -> None:
    """Получает список фотографий из MinIO для обработки."""
    logger.info("🔍 Поиск фотографий в MinIO")
    
    try:
        s3_hook = S3Hook(aws_conn_id=AIRFLOW_CONN_ID)
        s3_client = s3_hook.get_conn()
        
        # Получаем список объектов в папке input
        objects = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            Prefix=INPUT_PREFIX
        )
        
        photo_keys = []
        if 'Contents' in objects:
            photo_keys = [
                obj['Key'] 
                for obj in objects['Contents'] 
                if obj['Key'].lower().endswith(('.jpg', '.jpeg', '.png', '.bmp', '.webp'))
            ]
        
        logger.info(f"📸 Найдено фотографий: {len(photo_keys)}")
        
        if not photo_keys:
            logger.warning("⚠️ Фотографии не найдены")
        
        ti.xcom_push(key='photo_keys', value=photo_keys)
        
    except (BotoCoreError, ClientError) as e:
        logger.error(f"❌ Ошибка при работе с MinIO: {e}")
        raise


def download_photo_from_minio(ti: TaskInstance, **context) -> None:
    """Загружает фотографию из MinIO."""
    photo_keys = ti.xcom_pull(task_ids='list_photos', key='photo_keys')
    
    if not photo_keys:
        logger.warning("⚠️ Нет фотографий для обработки")
        return
    
    # Берем первую фотографию (можно расширить для обработки всех)
    photo_key = photo_keys[0]
    logger.info(f"📥 Загрузка фотографии: {photo_key}")
    
    try:
        s3_hook = S3Hook(aws_conn_id=AIRFLOW_CONN_ID)
        s3_client = s3_hook.get_conn()
        
        # Читаем бинарные данные напрямую
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=photo_key)
        photo_data = response['Body'].read()
        
        ti.xcom_push(key='photo_data', value=photo_data)
        ti.xcom_push(key='photo_key', value=photo_key)
        
        logger.info("✅ Фотография загружена")
        
    except (BotoCoreError, ClientError) as e:
        logger.error(f"❌ Ошибка при загрузке фотографии: {e}")
        raise


def animate_photo_task(ti: TaskInstance) -> None:
    """Создает анимацию из фотографии."""
    photo_data = ti.xcom_pull(task_ids='download_photo', key='photo_data')
    photo_key = ti.xcom_pull(task_ids='download_photo', key='photo_key')
    
    if not photo_data:
        logger.error("❌ Нет данных фотографии")
        raise ValueError("Нет данных фотографии для обработки")
    
    logger.info("🎬 Создание анимации...")
    
    try:
        # Определяем имя выходного файла
        photo_name = os.path.basename(photo_key)
        photo_name_without_ext = os.path.splitext(photo_name)[0]
        output_key = f"{OUTPUT_PREFIX}{photo_name_without_ext}_animated.gif"
        
        # Создаем временный файл для обработки
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix='.jpg') as tmp_input:
            # photo_data уже байты из предыдущей задачи
            tmp_input.write(photo_data)
            tmp_input_path = tmp_input.name
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.gif') as tmp_output:
            tmp_output_path = tmp_output.name
        
        try:
            # Создаем анимацию
            animator = PhotoAnimator(output_format='gif')
            animator.animate_photo(
                image_path=tmp_input_path,
                output_path=tmp_output_path,
                effect='ken_burns',  # Эффект Кена Бернса
                frames=60,
                zoom_factor=1.3,
                pan_direction='right',
                duration=3.0
            )
            
            # Читаем результат
            with open(tmp_output_path, 'rb') as f:
                animated_data = f.read()
            
            # Сохраняем в XCom для следующей задачи
            ti.xcom_push(key='animated_data', value=animated_data)
            ti.xcom_push(key='output_key', value=output_key)
            
            logger.info(f"✅ Анимация создана: {output_key}")
            
        finally:
            # Удаляем временные файлы
            if os.path.exists(tmp_input_path):
                os.unlink(tmp_input_path)
            if os.path.exists(tmp_output_path):
                os.unlink(tmp_output_path)
        
    except Exception as e:
        logger.error(f"❌ Ошибка при создании анимации: {e}")
        raise


def upload_animated_photo_to_minio(ti: TaskInstance) -> None:
    """Загружает анимированную фотографию обратно в MinIO."""
    animated_data = ti.xcom_pull(task_ids='animate_photo', key='animated_data')
    output_key = ti.xcom_pull(task_ids='animate_photo', key='output_key')
    
    if not animated_data:
        logger.error("❌ Нет данных анимации")
        raise ValueError("Нет данных анимации для загрузки")
    
    logger.info(f"⬆️ Загрузка анимации в MinIO: {output_key}")
    
    try:
        s3_hook = S3Hook(aws_conn_id=AIRFLOW_CONN_ID)
        
        # Проверяем наличие бакета
        existing_buckets = [bucket['Name'] for bucket in s3_hook.get_conn().list_buckets().get('Buckets', [])]
        if BUCKET_NAME not in existing_buckets:
            logger.info(f"🪣 Создаём бакет: {BUCKET_NAME}")
            s3_hook.get_conn().create_bucket(Bucket=BUCKET_NAME)
        
        # Загружаем анимацию
        s3_hook.load_bytes(
            bytes_data=animated_data,
            key=output_key,
            bucket_name=BUCKET_NAME,
            replace=True,
        )
        
        logger.info(f"✅ Анимация успешно загружена в MinIO: {output_key}")
        
    except (BotoCoreError, ClientError) as e:
        logger.error(f"❌ Ошибка при загрузке в MinIO: {e}")
        raise


with DAG(
    dag_id='photo_animation',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval='@daily',  # Можно изменить на нужный интервал
    catchup=False,
    description='DAG для создания анимаций из статичных фотографий',
    tags=['photo', 'animation', 'minio'],
) as dag:

    list_task = PythonOperator(
        task_id='list_photos',
        python_callable=list_photos_in_minio,
    )

    download_task = PythonOperator(
        task_id='download_photo',
        python_callable=download_photo_from_minio,
    )

    animate_task = PythonOperator(
        task_id='animate_photo',
        python_callable=animate_photo_task,
    )

    upload_task = PythonOperator(
        task_id='upload_animated_photo',
        python_callable=upload_animated_photo_to_minio,
    )

    list_task >> download_task >> animate_task >> upload_task
