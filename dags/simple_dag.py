import boto3
from botocore.client import Config
from io import BytesIO

def upload_to_minio():
    # Настройки подключения к MinIO
    s3 = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        config=Config(signature_version='s3v4'),
        region_name='us-east-1',
    )

    bucket_name = 'dev'
    object_key = 'data/api_data.json'
    content = b'{"hello": "world"}'

    # Проверка наличия бакета (создать, если нет)
    existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
    if bucket_name not in existing_buckets:
        s3.create_bucket(Bucket=bucket_name)

    # Загрузка объекта
    s3.upload_fileobj(
        Fileobj=BytesIO(content),
        Bucket=bucket_name,
        Key=object_key,
        ExtraArgs={'ContentType': 'application/json'}
    )