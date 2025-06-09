"""
File Manager

Generic file operations for data processing pipelines.
Works with any data source using Bronze -> Silver layer structure.
Enhanced with binary file support for unified Mac/VPS flow.
"""

import os
import shutil
import io
from include.config.env_detection import ENV, get_storage_paths

# Only import MinIO on VPS
if ENV.environment == 'vps':
    from include.delta_lake.minio_connect import get_minio_client, get_bucket_name
else:
    # Mac: Dummy functions
    def get_minio_client(): return None
    def get_bucket_name(): return None


def file_exists(filename: str, data_source: str, layer_path: str) -> bool:
    """
    Check if a specific file exists in any layer (with optional subfolder support)
    
    Args:
        filename: File to check (e.g., "tech_trends_summary_2024-12-31.parquet")
        data_source: Data source (e.g., "github") 
        layer_path: Layer with optional subfolder (e.g., "silver" or "silver/tech_trends_summary")
    """
    paths = get_storage_paths(data_source)  # date not needed for paths
    
    # Parse layer and subfolder
    if '/' in layer_path:
        layer, subfolder = layer_path.split('/', 1)
    else:
        layer, subfolder = layer_path, None
    
    if ENV.environment == 'mac':
        # Check local layer (with optional subfolder)
        base_layer_path = paths[f'{layer}_path']
        
        if subfolder:
            full_path = os.path.join(base_layer_path, subfolder)
        else:
            full_path = base_layer_path
            
        file_path = os.path.join(full_path, filename)
        exists = os.path.exists(file_path) and os.path.getsize(file_path) > 0
        
        if exists:
            print(f"✅ File exists locally in {layer_path}: {filename}")
        return exists
        
    else:
        # Check MinIO layer (with optional subfolder)
        client = get_minio_client()
        if client:
            bucket = get_bucket_name()
            
            if subfolder:
                s3_key = f"{layer}/{data_source}/{subfolder}/{filename}"
            else:
                s3_key = f"{layer}/{data_source}/{filename}"
                
            try:
                client.head_object(Bucket=bucket, Key=s3_key)
                print(f"✅ File exists in MinIO {layer_path}: {filename}")
                return True
            except client.exceptions.NoSuchKey:
                # File doesn't exist - this is normal, not an error
                return False
            except Exception as e:
                # Only print actual errors, not "file not found"
                if "404" not in str(e) and "Not Found" not in str(e):
                    print(f"⚠️  MinIO check failed for {filename}: {e}")
                return False
    
    return False


def ensure_dirs(data_source: str) -> dict:
    """Create directories for any data source processing"""
    paths = get_storage_paths(data_source)
    
    # Create all layer directories
    dirs = {
        'bronze': paths['bronze_path'],
        'silver': paths['silver_path'],
        'gold': paths['gold_path']
    }
    
    # Only create directories on Mac (VPS uses MinIO)
    if ENV.environment == 'mac':
        for path in dirs.values():
            os.makedirs(path, exist_ok=True)
        print(f"📁 Created local directories for {data_source}")
    else:
        print(f"📁 Using MinIO for {data_source} (no local dirs needed)")
    
    return dirs


def save_binary_to_layer(binary_data: bytes, filename: str, data_source: str, target_layer: str) -> bool:
    """
    Save binary data directly to any layer without temp files
    Perfect for .gz files in unified Mac/VPS flow
    
    Args:
        binary_data: Raw binary data (e.g., from requests.content)
        filename: Target filename (e.g., "2024-12-31-00.json.gz")
        data_source: Data source name (e.g., "github")
        target_layer: Layer name ("bronze", "silver", "gold")
    
    Returns:
        bool: True if successful, False if failed
    """
    paths = get_storage_paths(data_source)
    
    if ENV.environment == 'mac':
        # Save to local directory
        layer_path = paths[f'{target_layer}_path']
        os.makedirs(layer_path, exist_ok=True)
        
        file_path = os.path.join(layer_path, filename)
        try:
            with open(file_path, 'wb') as f:
                f.write(binary_data)
            size_mb = len(binary_data) / (1024 * 1024)
            print(f"✅ Saved binary to {target_layer}: {filename} ({size_mb:.1f}MB)")
            return True
        except Exception as e:
            print(f"❌ Failed to save binary {filename}: {e}")
            return False
            
    else:  # VPS
        # Upload directly to MinIO
        client = get_minio_client()
        bucket = get_bucket_name()
        
        if client:
            s3_key = f"{target_layer}/{data_source}/{filename}"
            try:
                # Convert bytes to file-like object for MinIO
                binary_stream = io.BytesIO(binary_data)
                client.put_object(
                    Bucket=bucket,
                    Key=s3_key,
                    Body=binary_stream,
                    ContentLength=len(binary_data)
                )
                size_mb = len(binary_data) / (1024 * 1024)
                print(f"✅ Uploaded binary to {target_layer}: {filename} ({size_mb:.1f}MB)")
                return True
            except Exception as e:
                print(f"❌ Binary upload failed {filename}: {e}")
                return False
        else:
            print("❌ MinIO not connected, cannot save binary")
            return False


def read_binary_from_layer(filename: str, data_source: str, source_layer: str) -> bytes:
    """
    Read binary data from any layer 
    Perfect for reading .gz files in unified Mac/VPS flow
    
    Args:
        filename: Source filename (e.g., "2024-12-31-00.json.gz") 
        data_source: Data source name (e.g., "github")
        source_layer: Layer name ("bronze", "silver", "gold")
    
    Returns:
        bytes: Binary data if successful, empty bytes if failed
    """
    paths = get_storage_paths(data_source)
    
    if ENV.environment == 'mac':
        # Read from local directory
        layer_path = paths[f'{source_layer}_path']
        file_path = os.path.join(layer_path, filename)
        
        try:
            with open(file_path, 'rb') as f:
                binary_data = f.read()
            print(f"✅ Read binary from {source_layer}: {filename}")
            return binary_data
        except Exception as e:
            print(f"❌ Failed to read binary {filename}: {e}")
            return b''
            
    else:  # VPS
        # Download from MinIO
        client = get_minio_client()
        bucket = get_bucket_name()
        
        if client:
            s3_key = f"{source_layer}/{data_source}/{filename}"
            try:
                response = client.get_object(Bucket=bucket, Key=s3_key)
                binary_data = response['Body'].read()
                print(f"✅ Downloaded binary from {source_layer}: {filename}")
                return binary_data
            except Exception as e:
                print(f"❌ Binary download failed {filename}: {e}")
                return b''
        else:
            print("❌ MinIO not connected, cannot read binary")
            return b''


def save_files_to_layer(csv_files: list, data_source: str, target_layer: str) -> dict:
    """Save CSV files to any layer (bronze/silver/gold) for any data source"""
    stats = {"successful": 0, "failed": 0}
    paths = get_storage_paths(data_source)
    
    if ENV.environment == 'mac':
        # Save to local layer
        layer_path = paths[f'{target_layer}_path']
        os.makedirs(layer_path, exist_ok=True)
        
        for csv_file in csv_files:
            if os.path.exists(csv_file):
                filename = os.path.basename(csv_file)
                shutil.copy2(csv_file, os.path.join(layer_path, filename))
                stats["successful"] += 1
                print(f"✅ Saved locally to {target_layer}: {filename}")
            else:
                stats["failed"] += 1
    else:
        # Save to MinIO layer
        client = get_minio_client()
        bucket = get_bucket_name()
        
        if client:
            for csv_file in csv_files:
                if os.path.exists(csv_file):
                    filename = os.path.basename(csv_file)
                    s3_key = f"{target_layer}/{data_source}/{filename}"
                    
                    try:
                        client.upload_file(csv_file, bucket, s3_key)
                        stats["successful"] += 1
                        print(f"✅ Uploaded to {target_layer}: {filename}")
                    except Exception as e:
                        print(f"❌ Upload failed {filename}: {e}")
                        stats["failed"] += 1
                else:
                    stats["failed"] += 1
    
    return stats


def delete_files_from_layer(filenames: list, data_source: str, source_layer: str, force_cleanup: bool = False):
    """Delete specific files from any layer - DAG chooses which files to delete"""
    if not force_cleanup:
        print("⏩ Cleanup disabled (use force_cleanup=True to enable)")
        return
        
    if ENV.environment == 'mac':
        # Delete specific files from local layer
        paths = get_storage_paths(data_source)
        layer_path = paths[f'{source_layer}_path']
        
        files_removed = 0
        for filename in filenames:
            file_path = os.path.join(layer_path, filename)
            if os.path.exists(file_path):
                os.remove(file_path)
                files_removed += 1
                print(f"🗑️  Deleted from {source_layer}: {filename}")
        
        print(f"🗑️  Cleaned {files_removed} {source_layer} files")
    else:
        # VPS: Delete specific files from MinIO
        client = get_minio_client()
        bucket = get_bucket_name()
        
        if client:
            deleted_count = 0
            for filename in filenames:
                s3_key = f"{source_layer}/{data_source}/{filename}"
                try:
                    client.delete_object(Bucket=bucket, Key=s3_key)
                    deleted_count += 1
                    print(f"🗑️  Deleted from {source_layer}: {filename}")
                except Exception as e:
                    print(f"❌ Failed to delete {filename}: {e}")
            
            print(f"🗑️  Cleaned {deleted_count} {source_layer} files")
        else:
            print("❌ MinIO not connected, cannot cleanup files")


def get_file_path(filename: str, data_source: str, layer: str) -> str:
    """Get file path for any data source and layer"""
    paths = get_storage_paths(data_source)
    
    if ENV.environment == 'prod':
        # Return S3 key format
        return f"{layer}/{data_source}/{filename}"
    else:
        # Return local file path
        layer_path = paths[f'{layer}_path']
        return os.path.join(layer_path, filename)