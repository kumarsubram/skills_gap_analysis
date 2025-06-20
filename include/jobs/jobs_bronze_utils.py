"""
Jobs Bronze Utilities - Clean From Scratch

Simple utilities for saving raw job API responses to Bronze Delta tables.
Uses your existing production delta_table_utils for consistency.
"""

import os
import json
from typing import Dict, List
from datetime import datetime, timezone
import pyarrow as pa
from deltalake import write_deltalake


def get_minio_storage_options() -> Dict[str, str]:
    """Get MinIO storage options for Delta Lake operations"""
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY') 
    endpoint = os.getenv('MINIO_ENDPOINT')
    
    if not all([access_key, secret_key, endpoint]):
        raise ValueError("Missing MinIO environment variables: MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_ENDPOINT")
    
    return {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_ENDPOINT_URL": f"http://{endpoint}",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


def save_raw_api_to_bronze_delta(raw_responses: List[Dict], source_name: str, date_str: str) -> bool:
    """
    Save raw API responses to Bronze Delta table
    
    Table will be created as: s3://delta-lake/bronze/bronze_jobs_{source}_rawdata
    
    Args:
        raw_responses: List of raw API response dictionaries
        source_name: Source name (greenhouse, hackernews, remoteok)
        date_str: Collection date (YYYY-MM-DD)
    
    Returns:
        bool: True if successful, False otherwise
    """
    if not raw_responses:
        print(f"   ⚠️  No data to save for {source_name}")
        return False
    
    # Use your enterprise naming convention
    table_name = f"jobs_{source_name}_rawdata"
    enterprise_table_name = f"bronze_{table_name}"
    table_path = f"s3://delta-lake/bronze/{enterprise_table_name}"
    
    print(f"   💾 Saving {len(raw_responses)} records to: {enterprise_table_name}")
    
    try:
        # Create arrays for PyArrow table
        record_ids = []
        source_names = []
        collection_dates = []
        collection_timestamps = []
        raw_json_data = []
        
        timestamp_now = datetime.now(timezone.utc).isoformat()
        
        # Process each response
        for i, response in enumerate(raw_responses):
            record_ids.append(f"{source_name}_{date_str}_{i:06d}")
            source_names.append(source_name)
            collection_dates.append(date_str)
            collection_timestamps.append(timestamp_now)
            # Convert to JSON string for storage
            raw_json_data.append(json.dumps(response, ensure_ascii=False, separators=(',', ':')))
        
        # Create PyArrow table with explicit schema
        schema = pa.schema([
            ('record_id', pa.string()),
            ('source_name', pa.string()), 
            ('date', pa.string()),  # Use 'date' to match your production tables
            ('collection_timestamp', pa.string()),
            ('raw_response_json', pa.string())
        ])
        
        # Build the table
        table = pa.table([
            pa.array(record_ids, type=pa.string()),
            pa.array(source_names, type=pa.string()),
            pa.array(collection_dates, type=pa.string()),
            pa.array(collection_timestamps, type=pa.string()),
            pa.array(raw_json_data, type=pa.string())
        ], schema=schema)
        
        # Write to Delta Lake
        write_deltalake(
            table_or_uri=table_path,
            data=table,
            storage_options=get_minio_storage_options(),
            mode="append"
        )
        
        print(f"   ✅ Successfully saved {len(raw_responses)} records")
        return True
        
    except Exception as e:
        print(f"   ❌ Failed to save to Bronze Delta: {str(e)}")
        return False


def bronze_delta_exists(source_name: str, date_str: str) -> bool:
    """
    Check if Bronze Delta table has data for this source and date
    Uses your existing production delta_table_utils
    
    Args:
        source_name: Source name (greenhouse, hackernews, remoteok)
        date_str: Collection date (YYYY-MM-DD)
    
    Returns:
        bool: True if data exists, False otherwise
    """
    try:
        # Import your production utilities
        from include.delta.delta_table_utils import get_table_count_for_date_partition, check_table_exists
        
        # Table name: jobs_greenhouse_rawdata
        table_name = f"jobs_{source_name}_rawdata"
        
        # First check if table exists at all using your production function
        if not check_table_exists(table_name, layer="bronze", data_source="jobs"):
            return False
        
        # Then check if data exists for this date using your production function
        count = get_table_count_for_date_partition(table_name, date_str, layer="bronze", data_source="jobs")
        return count is not None and count > 0
        
    except Exception as e:
        print(f"   🔍 Error checking table existence: {str(e)}")
        return False


def get_bronze_record_count(source_name: str, date_str: str) -> int:
    """
    Get record count for specific source and date
    Uses your existing production delta_table_utils
    
    Args:
        source_name: Source name (greenhouse, hackernews, remoteok)
        date_str: Collection date (YYYY-MM-DD)
    
    Returns:
        int: Number of records (0 if none or error)
    """
    try:
        # Import your production utilities
        from include.delta.delta_table_utils import get_table_count_for_date_partition
        
        # Table name: jobs_greenhouse_rawdata
        table_name = f"jobs_{source_name}_rawdata"
        
        count = get_table_count_for_date_partition(table_name, date_str, layer="bronze", data_source="jobs")
        return count if count is not None else 0
        
    except Exception as e:
        print(f"   🔍 Error getting record count: {str(e)}")
        return 0


def get_bronze_summary(source_name: str, date_str: str) -> Dict:
    """
    Get summary of Bronze Delta table for this source and date
    
    Args:
        source_name: Source name (greenhouse, hackernews, remoteok)
        date_str: Collection date (YYYY-MM-DD)
    
    Returns:
        dict: Summary information
    """
    # Use enterprise naming convention
    table_name = f"jobs_{source_name}_rawdata"
    enterprise_table_name = f"bronze_{table_name}"
    exists = bronze_delta_exists(source_name, date_str)
    
    summary = {
        'source': source_name,
        'date': date_str,
        'table_name': enterprise_table_name,
        'table_path': f"s3://delta-lake/bronze/{enterprise_table_name}",
        'exists': exists,
        'status': 'bronze_complete' if exists else 'missing',
        'record_count': get_bronze_record_count(source_name, date_str) if exists else 0
    }
    
    return summary


def ensure_jobs_directories():
    """
    Ensure MinIO bucket exists for Delta Lake storage
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Import MinIO client
        from include.storage.minio_connect import get_minio_client
        
        client = get_minio_client()
        bucket = 'delta-lake'
        
        # Check if bucket exists
        try:
            client.head_bucket(Bucket=bucket)
            print(f"✅ MinIO bucket '{bucket}' exists")
        except Exception:
            # Create bucket if it doesn't exist
            try:
                client.create_bucket(Bucket=bucket)
                print(f"✅ Created MinIO bucket '{bucket}'")
            except Exception as create_error:
                print(f"❌ Failed to create bucket '{bucket}': {create_error}")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ MinIO setup error: {str(e)}")
        return False