"""
Streaming Table Utilities - Enhanced Schema
==========================================

Utilities for creating and managing streaming Delta tables with enhanced schema.
Follows your existing patterns but adds streaming-specific functionality.

Place at: include/streaming/streaming_table_utils.py
"""

import os
import sys
from pathlib import Path
from typing import Dict, Optional
import pyarrow as pa
from deltalake import write_deltalake, DeltaTable

# Add project root for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def get_minio_storage_options() -> Dict[str, str]:
    """Get MinIO storage options - SAME AS YOUR EXISTING PATTERN"""
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


def get_streaming_table_schema() -> pa.Schema:
    """
    Get enhanced PyArrow schema for streaming GitHub table
    
    Returns:
        PyArrow schema with enhanced streaming columns
    """
    return pa.schema([
        # Original batch columns (for compatibility)
        ('hour', pa.int32()),
        ('minute', pa.int32()),
        ('keyword', pa.string()),
        ('mentions', pa.int32()),
        ('top_repo', pa.string()),
        ('repo_mentions', pa.int32()),
        ('event_mentions', pa.int32()),
        ('date', pa.date32()),
        ('source_file', pa.string()),
        ('processing_time', pa.timestamp('us', tz='UTC')),
        
        # Enhanced streaming columns
        ('event_id', pa.string()),                    # GitHub event ID for deduplication
        ('event_timestamp', pa.timestamp('us', tz='UTC')),  # Precise GitHub event time
        ('event_type', pa.string()),                  # "PushEvent", "PullRequestEvent", etc.
        ('actor_login', pa.string()),                 # GitHub username
        ('kafka_timestamp', pa.timestamp('us', tz='UTC')),  # When received in Kafka
        ('batch_id', pa.string()),                    # Streaming batch identifier
        ('window_start', pa.timestamp('us', tz='UTC')),     # Window start time
        ('window_end', pa.timestamp('us', tz='UTC'))        # Window end time
    ])


def get_streaming_table_path() -> str:
    """Get the streaming table path - FOLLOWS YOUR ENTERPRISE NAMING"""
    return "s3://delta-lake/bronze/bronze_github_streaming_keyword_extractions"


def check_streaming_table_exists() -> bool:
    """
    Check if streaming table exists
    
    Returns:
        bool: True if table exists, False otherwise
    """
    table_path = get_streaming_table_path()
    
    try:
        storage_options = get_minio_storage_options()
        dt = DeltaTable(table_path, storage_options=storage_options)
        dt.version()  # This will raise exception if table doesn't exist
        return True
    except Exception:
        return False



def create_empty_streaming_table() -> bool:
    """
    Create empty streaming table with enhanced schema - COMPATIBLE VERSION
    """
    table_path = get_streaming_table_path()
    
    try:
        print(f"🏗️ Creating streaming table: {table_path}")
        
        # Get schema and storage options
        schema = get_streaming_table_schema()
        storage_options = get_minio_storage_options()
        
        # Create empty table with correct schema
        empty_arrays = []
        for field in schema:
            empty_array = pa.array([], type=field.type)
            empty_arrays.append(empty_array)
        
        empty_table = pa.table(empty_arrays, schema=schema)
        
        # FIXED: Basic write_deltalake without delta_metadata (compatible)
        write_deltalake(
            table_or_uri=table_path,
            data=empty_table,
            storage_options=storage_options,
            mode="overwrite",
            configuration={
                # Data retention - CRITICAL: Use proper interval format for 1-day retention
                "delta.logRetentionDuration": "interval 1 days",
                "delta.deletedFileRetentionDuration": "interval 1 days",

                # Auto-optimization (available in open-source)
                "delta.autoOptimize.optimizeWrite": "false",
                "delta.autoOptimize.autoCompact": "false",

                # File size optimization for streaming (small files)
                "delta.targetFileSize": "2097152",  # 2MB

                # Data skipping (open-source feature)
                "delta.dataSkippingNumIndexedCols": "5",
                "delta.dataSkippingStatsColumns": "date,hour,minute,processing_time,kafka_timestamp",

                # Checkpoint settings - more frequent for streaming
                "delta.checkpointInterval": "50",

                # Column mapping (if needed for schema evolution)
                "delta.columnMapping.mode": "none",

                # Enable change data feed (open-source)
                "delta.enableChangeDataFeed": "false",

                # Isolation level
                "delta.isolationLevel": "WriteSerializable"
            },
            partition_by=["date", "hour", "minute"]  # Partition by date, hour, and minute for efficient pruning
        )
        
        
        print("✅ Successfully created streaming table")
        print(f"📋 Schema columns: {len(schema.names)}")
        print(f"🏢 Location: {table_path}")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to create streaming table: {e}")
        import traceback
        traceback.print_exc()
        return False


def get_streaming_table_info() -> Optional[Dict]:
    """
    Get comprehensive information about streaming table
    
    Returns:
        Dict with table information or None if error
    """
    table_path = get_streaming_table_path()
    
    try:
        storage_options = get_minio_storage_options()
        dt = DeltaTable(table_path, storage_options=storage_options)
        df = dt.to_pandas()
        
        info = {
            "table_path": table_path,
            "version": dt.version(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "column_types": {col: str(df[col].dtype) for col in df.columns},
            "is_empty": len(df) == 0,
            "latest_event_time": df['event_timestamp'].max() if len(df) > 0 and 'event_timestamp' in df.columns else None,
            "latest_processing_time": df['processing_time'].max() if len(df) > 0 else None
        }
        
        # Add sample data if table has rows
        if len(df) > 0:
            info["sample_rows"] = df.head(3).to_dict('records')
            info["unique_keywords"] = df['keyword'].nunique() if 'keyword' in df.columns else 0
            info["unique_repos"] = df['top_repo'].nunique() if 'top_repo' in df.columns else 0
        
        return info
        
    except Exception as e:
        print(f"❌ Error getting table info: {e}")
        return None


def cleanup_old_partitions(hours_to_keep=1) -> bool:
    """
    Clean up old partitions keeping only specified hours of data
    
    Args:
        hours_to_keep: Number of hours of data to retain (default: 1)
    
    Returns:
        bool: True if cleanup successful, False otherwise
    """
    from datetime import datetime, timedelta, timezone
    
    table_path = get_streaming_table_path()
    storage_options = get_minio_storage_options()
    
    try:
        # Calculate cutoff time
        cutoff_time = datetime.now(timezone.utc) - timedelta(hours=hours_to_keep)
        cutoff_date = cutoff_time.date()
        cutoff_hour = cutoff_time.hour
        
        print("\n🧹 Cleaning old partitions")
        print(f"⏰ Keeping data from: {cutoff_date} {cutoff_hour:02d}:00 UTC onwards")
        
        # Load Delta table
        dt = DeltaTable(table_path, storage_options=storage_options)
        
        # Count rows before deletion
        df_before = dt.to_pandas()
        rows_before = len(df_before)
        
        if rows_before == 0:
            print("📭 Table is empty - no cleanup needed")
            return True
        
        # Count rows to delete
        old_rows = df_before[
            (df_before['date'] < cutoff_date) | 
            ((df_before['date'] == cutoff_date) & (df_before['hour'] < cutoff_hour))
        ]
        rows_to_delete = len(old_rows)
        
        if rows_to_delete > 0:
            print(f"🗑️ Deleting {rows_to_delete:,} rows from old partitions")
            
            # Perform deletion using deltalake Python API
            # Build the deletion predicate
            delete_expr = f"(date < '{cutoff_date}') OR (date = '{cutoff_date}' AND hour < {cutoff_hour})"
            
            # Delete old partitions
            dt.delete(delete_expr)
            
            # Skip VACUUM or use minimum allowed retention
            # Delta enforces minimum retention, so we'll just delete without vacuum
            # The files will be cleaned up in the next scheduled vacuum run
            print(f"✅ Cleanup complete: {rows_to_delete:,} rows removed")
            print(f"📊 Remaining rows: {rows_before - rows_to_delete:,}")
            print("ℹ️ Deleted files will be cleaned in next scheduled VACUUM")
        else:
            print("✅ No old partitions to clean")
        
        return True
        
    except Exception as e:
        print(f"❌ Cleanup failed: {e}")
        return False


def ensure_streaming_table_exists() -> bool:
    """
    Ensure streaming table exists with correct enhanced schema
    
    Returns:
        bool: True if table is ready, False if failed
    """
    print("🔍 STREAMING TABLE SETUP")
    print("=" * 40)
    
    table_path = get_streaming_table_path()
    print(f"🎯 Target table: {table_path}")
    
    # Check if table already exists
    if check_streaming_table_exists():
        print("✅ Streaming table already exists")
        
        # Get table info to verify schema
        info = get_streaming_table_info()
        if info:
            print(f"📊 Current records: {info['row_count']:,}")
            print(f"📋 Columns: {info['column_count']}")
            print(f"🔢 Version: {info['version']}")
            
            # Check if enhanced schema columns exist
            required_streaming_cols = ['event_id', 'event_timestamp', 'kafka_timestamp', 'batch_id']
            existing_cols = info['columns']
            
            missing_cols = [col for col in required_streaming_cols if col not in existing_cols]
            
            if missing_cols:
                print(f"⚠️ Missing enhanced columns: {missing_cols}")
                print("💡 Consider recreating table with enhanced schema")
                return False
            else:
                print("✅ Enhanced schema verified")
                
                # NEW: Clean up old partitions at startup
                print("\n🔄 Running startup cleanup...")
                cleanup_success = cleanup_old_partitions(hours_to_keep=1)
                
                if cleanup_success:
                    print("✅ Startup cleanup completed")
                    return True
                else:
                    print("⚠️ Cleanup failed but table is still usable")
                    return True  # Still return True as table exists and is usable
        else:
            print("❌ Could not verify table schema")
            return False
    
    else:
        print("📝 Streaming table does not exist - creating...")
        
        # Create new table with enhanced schema
        success = create_empty_streaming_table()
        
        if success:
            print("✅ Streaming table created successfully")
            
            # Verify creation
            info = get_streaming_table_info()
            if info:
                print("🔍 Verification:")
                print(f"   📊 Records: {info['row_count']}")
                print(f"   📋 Columns: {info['column_count']}")
                print(f"   🔢 Version: {info['version']}")
                
                # Show enhanced columns
                enhanced_cols = ['event_id', 'event_timestamp', 'kafka_timestamp', 'batch_id']
                print("✅ Enhanced streaming columns:")
                for col in enhanced_cols:
                    if col in info['columns']:
                        print(f"   • {col}: ✅")
                    else:
                        print(f"   • {col}: ❌")
                
                return True
            else:
                print("❌ Could not verify table creation")
                return False
        else:
            print("❌ Failed to create streaming table")
            return False


def print_streaming_table_summary():
    """Print a nice summary of the streaming table"""
    print("🔍 STREAMING TABLE SUMMARY")
    print("=" * 50)
    
    table_path = get_streaming_table_path()
    
    if not check_streaming_table_exists():
        print("❌ Streaming table does not exist")
        print(f"🏢 Expected location: {table_path}")
        print("💡 Run ensure_streaming_table_exists() to create")
        return
    
    info = get_streaming_table_info()
    if not info:
        print("❌ Could not get table information")
        return
    
    print(f"📍 Table path: {info['table_path']}")
    print(f"📊 Version: {info['version']}")
    print(f"🔢 Total rows: {info['row_count']:,}")
    print(f"📋 Columns: {info['column_count']}")
    
    if info['row_count'] > 0:
        print(f"🏷️ Unique keywords: {info.get('unique_keywords', 'N/A')}")
        print(f"📦 Unique repositories: {info.get('unique_repos', 'N/A')}")
        
        if info.get('latest_event_time'):
            print(f"⏰ Latest event: {info['latest_event_time']}")
        if info.get('latest_processing_time'):
            print(f"🔄 Latest processing: {info['latest_processing_time']}")
        
        print("\n👀 Sample records:")
        for i, row in enumerate(info.get('sample_rows', [])[:2]):
            print(f"   Row {i+1}: {row.get('keyword', 'N/A')} in {row.get('top_repo', 'N/A')}")
    else:
        print("📭 Table is empty (ready for streaming)")
    
    print("\n📋 Schema verification:")
    enhanced_cols = ['event_id', 'event_timestamp', 'kafka_timestamp', 'batch_id', 'window_start', 'window_end']
    for col in enhanced_cols:
        status = "✅" if col in info['columns'] else "❌"
        print(f"   {status} {col}")
    
    print("=" * 50)


def main():
    """Command line interface for streaming table management"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Manage streaming Delta table')
    parser.add_argument('--check', action='store_true', help='Check if table exists')
    parser.add_argument('--create', action='store_true', help='Create table if not exists')
    parser.add_argument('--info', action='store_true', help='Show table information')
    parser.add_argument('--ensure', action='store_true', help='Ensure table exists (check + create)')
    
    args = parser.parse_args()
    
    if args.check:
        exists = check_streaming_table_exists()
        print(f"Streaming table exists: {exists}")
    
    elif args.create:
        success = create_empty_streaming_table()
        print(f"Table creation: {'Success' if success else 'Failed'}")
    
    elif args.info:
        print_streaming_table_summary()
    
    elif args.ensure:
        success = ensure_streaming_table_exists()
        print(f"Table ready: {'Yes' if success else 'No'}")
    
    else:
        # Default: show summary
        print_streaming_table_summary()


if __name__ == "__main__":
    main()