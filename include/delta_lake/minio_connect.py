"""
MinIO Connection

Simple MinIO connection utility like database.py
"""

import os
import boto3
from botocore.client import Config
from botocore.exceptions import ClientError
from include.config.env_detection import ENV


def get_minio_client():
    """Get MinIO S3 client"""
    if not ENV.config.get('use_minio'):
        return None
    
    # Get credentials directly from environment
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    
    if not access_key or not secret_key:
        print("❌ MinIO credentials not found in environment")
        return None
    
    try:
        client = boto3.client(
            's3',
            endpoint_url=f"http://{os.getenv('VPS_IP')}:9000",
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4')
        )
        
        # Ensure bucket exists
        bucket = ENV.config['minio_bucket']
        try:
            client.head_bucket(Bucket=bucket)
        except ClientError as e:
            if int(e.response['Error']['Code']) == 404:
                client.create_bucket(Bucket=bucket)
        
        return client
    except Exception as e:
        print(f"❌ MinIO connection failed: {e}")
        return None


def get_bucket_name():
    """Get MinIO bucket name"""
    return ENV.config.get('minio_bucket')


# Simple connection test
if __name__ == "__main__":
    client = get_minio_client()
    if client:
        print(f"✅ MinIO connected to bucket: {get_bucket_name()}")
    else:
        print("⚠️  MinIO not available")