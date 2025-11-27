#!/usr/bin/env python3
import os
import boto3

S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL')
S3_BUCKET = os.getenv('S3_BUCKET_NAME')
S3_REGION = os.getenv('S3_REGION', 'ru-1')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')


try:
    s3_client = boto3.client(
        's3',
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )

    print(f"✅ Connected to: {S3_ENDPOINT_URL}")
    print(f"📦 Bucket: {S3_BUCKET}")

    # Проверяем доступ к бакету
    response = s3_client.head_bucket(Bucket=S3_BUCKET)
    print("✅ Bucket is accessible")

    # Пробуем создать тестовый файл
    test_key = "test/connection_test.txt"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=test_key,
        Body=b"Test connection"
    )
    print("✅ Write test successful")

    # Удаляем тестовый файл
    s3_client.delete_object(Bucket=S3_BUCKET, Key=test_key)
    print("✅ Delete test successful")

    print("\n🎉 All tests passed!")

except Exception as e:
    print(f"❌ Connection failed: {e}")