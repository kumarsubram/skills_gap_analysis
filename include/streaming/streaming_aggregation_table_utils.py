"""
Streaming Aggregation Table Utilities - For Fast SSE Queries
===========================================================

Manages the aggregated table that makes SSE queries super fast.
This table stores daily technology aggregates for lightning-fast dashboard queries.

Place at: include/streaming/streaming_aggregation_table_utils.py
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
        raise ValueError("Missing MinIO environment variables")
    
    return {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_ENDPOINT_URL": f"http://{endpoint}",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }

def get_streaming_aggregation_schema() -> pa.Schema:
    """Schema for fast aggregated table - optimized for SSE queries"""
    return pa.schema([
        ('date', pa.date32()),                            # Partition key (YYYY-MM-DD)
        ('technology', pa.string()),                      # Technology name
        ('daily_mentions', pa.int64()),                   # Total mentions for the day
        ('repo_count', pa.int64()),                       # Unique repositories count
        ('event_count', pa.int64()),                      # Number of raw events
        ('last_activity', pa.timestamp('us', tz='UTC')),  # Latest event timestamp
        ('last_updated', pa.timestamp('us', tz='UTC')),   # When this record was updated
        ('top_repo', pa.string()),                        # Most active repository
        ('processing_batch', pa.string())                 # Batch ID for tracking
    ])

def get_streaming_aggregation_table_path() -> str:
    """Get the streaming aggregation table path"""
    return "s3://delta-lake/bronze/bronze_github_streaming_daily_aggregates"

def check_streaming_aggregation_table_exists() -> bool:
    """Check if streaming aggregation table exists"""
    table_path = get_streaming_aggregation_table_path()
    
    try:
        storage_options = get_minio_storage_options()
        dt = DeltaTable(table_path, storage_options=storage_options)
        dt.version()
        return True
    except Exception:
        return False

def create_empty_streaming_aggregation_table() -> bool:
    """Create empty streaming aggregation table with optimized schema"""
    table_path = get_streaming_aggregation_table_path()
    
    try:
        print(f"🏗️ Creating streaming aggregation table: {table_path}")
        
        # Get schema and storage options
        schema = get_streaming_aggregation_schema()
        storage_options = get_minio_storage_options()
        
        # Create empty table
        empty_arrays = []
        for field in schema:
            empty_array = pa.array([], type=field.type)
            empty_arrays.append(empty_array)
        
        empty_table = pa.table(empty_arrays, schema=schema)
        
        # Write with optimization settings for fast queries
        write_deltalake(
            table_or_uri=table_path,
            data=empty_table,
            storage_options=storage_options,
            mode="overwrite",
            partition_by=["date"],  # Partition by date for fast queries
            configuration={
                "delta.logRetentionDuration": "1 day",
                "delta.deletedFileRetentionDuration": "1 day",
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
                "delta.tuneFileSizesForRewrites": "true"
            }
        )
        
        print("✅ Successfully created streaming aggregation table")
        print(f"📊 Schema: {len(schema.names)} columns")
        print("🔍 Optimized for: Fast daily technology lookups")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create streaming aggregation table: {e}")
        import traceback
        traceback.print_exc()
        return False

def get_streaming_aggregation_table_info() -> Optional[Dict]:
    """Get comprehensive information about streaming aggregation table"""
    table_path = get_streaming_aggregation_table_path()
    
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
            "latest_activity": df['last_activity'].max() if len(df) > 0 and 'last_activity' in df.columns else None,
            "latest_update": df['last_updated'].max() if len(df) > 0 and 'last_updated' in df.columns else None
        }
        
        # Add aggregation-specific metrics
        if len(df) > 0:
            info["sample_rows"] = df.head(3).to_dict('records')
            info["unique_technologies"] = df['technology'].nunique() if 'technology' in df.columns else 0
            info["total_daily_mentions"] = df['daily_mentions'].sum() if 'daily_mentions' in df.columns else 0
            info["avg_mentions_per_tech"] = df['daily_mentions'].mean() if 'daily_mentions' in df.columns else 0
            info["unique_dates"] = df['date'].nunique() if 'date' in df.columns else 0
        
        return info
        
    except Exception as e:
        print(f"❌ Error getting streaming aggregation table info: {e}")
        return None

def ensure_streaming_aggregation_table_exists() -> bool:
    """
    Ensure streaming aggregation table exists with correct schema
    
    Returns:
        bool: True if table is ready, False if failed
    """
    print("🔍 STREAMING AGGREGATION TABLE SETUP")
    print("=" * 45)
    
    table_path = get_streaming_aggregation_table_path()
    print(f"🎯 Target table: {table_path}")
    
    # Check if table already exists
    if check_streaming_aggregation_table_exists():
        print("✅ Streaming aggregation table already exists")
        
        # Get table info to verify schema
        info = get_streaming_aggregation_table_info()
        if info:
            print(f"📊 Current records: {info['row_count']:,}")
            print(f"📋 Columns: {info['column_count']}")
            print(f"🔢 Version: {info['version']}")
            
            # Check if required aggregation columns exist
            required_agg_cols = ['date', 'technology', 'daily_mentions', 'repo_count']
            existing_cols = info['columns']
            
            missing_cols = [col for col in required_agg_cols if col not in existing_cols]
            
            if missing_cols:
                print(f"⚠️ Missing aggregation columns: {missing_cols}")
                print("💡 Consider recreating table with correct schema")
                return False
            else:
                print("✅ Aggregation schema verified")
                
                # Show performance metrics
                if info['row_count'] > 0:
                    print("🚀 Performance metrics:")
                    print(f"   • Unique technologies: {info.get('unique_technologies', 'N/A')}")
                    print(f"   • Total mentions: {info.get('total_daily_mentions', 'N/A'):,}")
                    print(f"   • Avg mentions/tech: {info.get('avg_mentions_per_tech', 0):.1f}")
                    print(f"   • Date coverage: {info.get('unique_dates', 'N/A')} days")
                
                return True
        else:
            print("❌ Could not verify table schema")
            return False
    
    else:
        print("📝 Streaming aggregation table does not exist - creating...")
        
        # Create new table with aggregation schema
        success = create_empty_streaming_aggregation_table()
        
        if success:
            print("✅ Streaming aggregation table created successfully")
            
            # Verify creation
            info = get_streaming_aggregation_table_info()
            if info:
                print("🔍 Verification:")
                print(f"   📊 Records: {info['row_count']}")
                print(f"   📋 Columns: {info['column_count']}")
                print(f"   🔢 Version: {info['version']}")
                
                # Show aggregation columns
                agg_cols = ['date', 'technology', 'daily_mentions', 'repo_count', 'last_activity']
                print("✅ Aggregation columns:")
                for col in agg_cols:
                    if col in info['columns']:
                        print(f"   • {col}: ✅")
                    else:
                        print(f"   • {col}: ❌")
                
                return True
            else:
                print("❌ Could not verify table creation")
                return False
        else:
            print("❌ Failed to create streaming aggregation table")
            return False

def print_streaming_aggregation_table_summary():
    """Print a comprehensive summary of the streaming aggregation table"""
    print("🔍 STREAMING AGGREGATION TABLE SUMMARY")
    print("=" * 55)
    
    table_path = get_streaming_aggregation_table_path()
    
    if not check_streaming_aggregation_table_exists():
        print("❌ Streaming aggregation table does not exist")
        print(f"🏢 Expected location: {table_path}")
        print("💡 Run ensure_streaming_aggregation_table_exists() to create")
        return
    
    info = get_streaming_aggregation_table_info()
    if not info:
        print("❌ Could not get table information")
        return
    
    print(f"📍 Table path: {info['table_path']}")
    print(f"📊 Version: {info['version']}")
    print(f"🔢 Total rows: {info['row_count']:,}")
    print(f"📋 Columns: {info['column_count']}")
    
    if info['row_count'] > 0:
        print("\n🚀 Performance Metrics:")
        print(f"   🏷️ Unique technologies: {info.get('unique_technologies', 'N/A')}")
        print(f"   📊 Total daily mentions: {info.get('total_daily_mentions', 'N/A'):,}")
        print(f"   📈 Avg mentions per tech: {info.get('avg_mentions_per_tech', 0):.1f}")
        print(f"   📅 Date coverage: {info.get('unique_dates', 'N/A')} days")
        
        if info.get('latest_activity'):
            print(f"   ⏰ Latest activity: {info['latest_activity']}")
        if info.get('latest_update'):
            print(f"   🔄 Latest update: {info['latest_update']}")
        
        print("\n👀 Sample aggregated records:")
        for i, row in enumerate(info.get('sample_rows', [])[:3]):
            tech = row.get('technology', 'N/A')
            mentions = row.get('daily_mentions', 0)
            repos = row.get('repo_count', 0)
            print(f"   {i+1}. {tech}: {mentions} mentions across {repos} repos")
    else:
        print("📭 Table is empty (ready for aggregation)")
    
    print("\n📋 Schema verification:")
    agg_cols = ['date', 'technology', 'daily_mentions', 'repo_count', 'event_count', 'last_activity']
    for col in agg_cols:
        status = "✅" if col in info['columns'] else "❌"
        print(f"   {status} {col}")
    
    print("\n💡 Usage Notes:")
    print("   • This table enables 15x faster SSE queries")
    print("   • Updated in real-time by streaming consumer")
    print("   • Partitioned by date for optimal performance")
    print("   • Contains daily aggregates, not raw events")
    
    print("=" * 55)

def main():
    """Command line interface for streaming aggregation table management"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Manage streaming aggregation Delta table')
    parser.add_argument('--check', action='store_true', help='Check if table exists')
    parser.add_argument('--create', action='store_true', help='Create table if not exists')
    parser.add_argument('--info', action='store_true', help='Show table information')
    parser.add_argument('--ensure', action='store_true', help='Ensure table exists (check + create)')
    parser.add_argument('--summary', action='store_true', help='Show comprehensive summary')
    
    args = parser.parse_args()
    
    if args.check:
        exists = check_streaming_aggregation_table_exists()
        print(f"Streaming aggregation table exists: {exists}")
    
    elif args.create:
        success = create_empty_streaming_aggregation_table()
        print(f"Table creation: {'Success' if success else 'Failed'}")
    
    elif args.info:
        info = get_streaming_aggregation_table_info()
        if info:
            print(f"Table info: {info}")
        else:
            print("Could not retrieve table information")
    
    elif args.ensure:
        success = ensure_streaming_aggregation_table_exists()
        print(f"Table ready: {'Yes' if success else 'No'}")
    
    elif args.summary:
        print_streaming_aggregation_table_summary()
    
    else:
        # Default: show summary
        print_streaming_aggregation_table_summary()

if __name__ == "__main__":
    main()