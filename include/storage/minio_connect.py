"""
MinIO S3 Connection Module - SECURE Implementation

NO HARDCODED CREDENTIALS - Git safe!
Uses ONLY environment variables, fails fast if not set.
"""

import os
import boto3
from botocore.exceptions import ClientError


def get_minio_client():
    """Get MinIO S3 client - REQUIRES environment variables"""
    
    # REQUIRED environment variables - fail if not set
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    endpoint = os.getenv('MINIO_ENDPOINT')
    
    # Fail fast if any required env vars missing
    if not access_key:
        raise ValueError("MINIO_ACCESS_KEY environment variable is required")
    if not secret_key:
        raise ValueError("MINIO_SECRET_KEY environment variable is required")
    if not endpoint:
        raise ValueError("MINIO_ENDPOINT environment variable is required")
    
    try:
        client = boto3.client(
            's3',
            endpoint_url=f'http://{endpoint}',
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name='us-east-1'
        )
        
        # Test connection
        client.list_buckets()
        return client
        
    except Exception as e:
        raise ConnectionError(f"MinIO connection failed to {endpoint}: {e}")


def get_bucket_name():
    """Get MinIO bucket name from environment - REQUIRED"""
    bucket = os.getenv('MINIO_BUCKET')
    if not bucket:
        raise ValueError("MINIO_BUCKET environment variable is required")
    return bucket


def ensure_bucket_exists():
    """Ensure MinIO bucket exists"""
    client = get_minio_client()
    bucket = get_bucket_name()
    
    try:
        client.head_bucket(Bucket=bucket)
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            client.create_bucket(Bucket=bucket)
            return True
        raise


if __name__ == "__main__":
    # Test connection - will fail fast if env vars not set
    print("🧪 Testing MinIO Connection (requires env vars)")
    try:
        client = get_minio_client()
        bucket = get_bucket_name()
        ensure_bucket_exists()
        print(f"✅ MinIO working with bucket: {bucket}")
    except Exception as e:
        print(f"❌ Failed: {e}")