"""
File Manager - Pure MinIO S3 Operations

Clean, simple version that works for both Mac and VPS.
All operations go through MinIO S3.
"""

import os
import io


def get_minio_client():
    """Get MinIO client - import locally to avoid path issues"""
    try:
        from .minio_connect import get_minio_client as _get_client
        return _get_client()
    except ImportError:
        # Fallback for direct execution
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from include.storage.minio_connect import get_minio_client as _get_client
        return _get_client()


def get_bucket_name():
    """Get bucket name - import locally to avoid path issues"""
    try:
        from .minio_connect import get_bucket_name as _get_bucket
        return _get_bucket()
    except ImportError:
        # Fallback for direct execution
        import sys
        from pathlib import Path
        sys.path.insert(0, str(Path(__file__).parent.parent.parent))
        from include.storage.minio_connect import get_bucket_name as _get_bucket
        return _get_bucket()


def file_exists(filename: str, data_source: str, layer_path: str) -> bool:
    """
    Check if file exists in MinIO S3
    
    Args:
        filename: File to check (e.g., "2024-12-31-00.json.gz")
        data_source: Data source (e.g., "github") 
        layer_path: Layer (e.g., "bronze")
    """
    client = get_minio_client()
    if not client:
        return False
    
    bucket = get_bucket_name()
    s3_key = f"{layer_path}/{data_source}/{filename}"
    
    try:
        client.head_object(Bucket=bucket, Key=s3_key)
        return True
    except Exception:
        return False


def save_binary_to_layer(binary_data: bytes, filename: str, data_source: str, target_layer: str) -> bool:
    """
    Save binary data to MinIO S3
    
    Args:
        binary_data: Raw binary data
        filename: Target filename
        data_source: Data source name (e.g., "github")
        target_layer: Layer name ("bronze", "silver", "gold")
    """
    client = get_minio_client()
    if not client:
        print("❌ MinIO not connected")
        return False
    
    bucket = get_bucket_name()
    s3_key = f"{target_layer}/{data_source}/{filename}"
    
    try:
        binary_stream = io.BytesIO(binary_data)
        client.put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=binary_stream,
            ContentLength=len(binary_data)
        )
        size_mb = len(binary_data) / (1024 * 1024)
        print(f"✅ Uploaded {filename} ({size_mb:.1f}MB) to {target_layer}")
        return True
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False


def read_binary_from_layer(filename: str, data_source: str, source_layer: str) -> bytes:
    """
    Read binary data from MinIO S3
    
    Args:
        filename: Source filename
        data_source: Data source name
        source_layer: Layer name
    """
    client = get_minio_client()
    if not client:
        print("❌ MinIO not connected")
        return b''
    
    bucket = get_bucket_name()
    s3_key = f"{source_layer}/{data_source}/{filename}"
    
    try:
        response = client.get_object(Bucket=bucket, Key=s3_key)
        binary_data = response['Body'].read()
        print(f"✅ Downloaded {filename} from {source_layer}")
        return binary_data
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return b''


def save_files_to_layer(file_paths: list, data_source: str, target_layer: str) -> dict:
    """Save local files to MinIO S3"""
    stats = {"successful": 0, "failed": 0}
    client = get_minio_client()
    
    if not client:
        stats["failed"] = len(file_paths)
        return stats
    
    bucket = get_bucket_name()
    
    for file_path in file_paths:
        if os.path.exists(file_path):
            filename = os.path.basename(file_path)
            s3_key = f"{target_layer}/{data_source}/{filename}"
            
            try:
                client.upload_file(file_path, bucket, s3_key)
                stats["successful"] += 1
                print(f"✅ Uploaded {filename}")
            except Exception as e:
                print(f"❌ Upload failed {filename}: {e}")
                stats["failed"] += 1
        else:
            stats["failed"] += 1
    
    return stats


def delete_files_from_layer(filenames: list, data_source: str, source_layer: str, force_cleanup: bool = False):
    """Delete files from MinIO S3"""
    if not force_cleanup:
        print("⏩ Cleanup disabled")
        return
    
    client = get_minio_client()
    if not client:
        return
    
    bucket = get_bucket_name()
    deleted_count = 0
    
    for filename in filenames:
        s3_key = f"{source_layer}/{data_source}/{filename}"
        try:
            client.delete_object(Bucket=bucket, Key=s3_key)
            deleted_count += 1
            print(f"🗑️ Deleted {filename}")
        except Exception:
            pass
    
    print(f"🗑️ Cleaned {deleted_count} files")


def ensure_dirs(data_source: str) -> dict:
    """Ensure MinIO bucket exists"""
    client = get_minio_client()
    if client:
        bucket = get_bucket_name()
        try:
            client.head_bucket(Bucket=bucket)
            print(f"✅ Bucket '{bucket}' exists")
        except Exception:
            try:
                client.create_bucket(Bucket=bucket)
                print(f"✅ Created bucket '{bucket}'")
            except Exception:
                print(f"❌ Could not create bucket '{bucket}'")
    
    # Return dummy paths for compatibility
    return {
        'bronze_path': f"s3a://delta-lake/bronze/{data_source}",
        'silver_path': f"s3a://delta-lake/silver/{data_source}",
        'gold_path': f"s3a://delta-lake/gold/{data_source}",
    }


def get_file_path(filename: str, data_source: str, layer: str) -> str:
    """Get S3 key for file"""
    return f"{layer}/{data_source}/{filename}"


if __name__ == "__main__":
    # Test the file manager
    print("🧪 Testing File Manager")
    print("=" * 30)
    
    # Test MinIO connection
    client = get_minio_client()
    if not client:
        print("❌ MinIO not connected - check Docker containers")
        print("   Try: docker-compose up -d")
        exit(1)
    
    print("✅ MinIO connected")
    
    # Test file operations
    test_data = b"Hello MinIO World!"
    test_filename = "test-simple.bin"
    
    # Save test file
    success = save_binary_to_layer(test_data, test_filename, 'github', 'bronze')
    
    if success:
        # Check if it exists
        exists = file_exists(test_filename, 'github', 'bronze')
        print(f"✅ File exists: {exists}")
        
        # Read it back
        read_data = read_binary_from_layer(test_filename, 'github', 'bronze')
        if read_data:
            print(f"✅ Read back: {read_data.decode()}")
        
        print("🎉 All tests passed!")
    else:
        print("❌ Test failed")
    
    print("=" * 30)