#!/usr/bin/env python3
"""
Clean Delta Table Checker
Simple script to check your bronze Delta table and get row counts
"""

import os
from deltalake import DeltaTable

def main():
    print("🔍 Delta Table Checker")
    print("=" * 40)
    
    # Table path (we know this works from previous test)
    table_path = "s3://delta-lake/bronze/github/keyword_extractions"
    
    # MinIO connection settings
    storage_options = {
        "AWS_ACCESS_KEY_ID": os.getenv('MINIO_ACCESS_KEY'),
        "AWS_SECRET_ACCESS_KEY": os.getenv('MINIO_SECRET_KEY'),
        "AWS_ENDPOINT_URL": f"http://{os.getenv('MINIO_ENDPOINT')}",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }
    
    print(f"📍 Table path: {table_path}")
    print(f"🔗 MinIO endpoint: {os.getenv('MINIO_ENDPOINT')}")
    print()
    
    try:
        # Load the Delta table
        dt = DeltaTable(table_path, storage_options=storage_options)
        
        print("✅ Delta table loaded successfully!")
        print(f"📊 Table version: {dt.version()}")
        print()
        
        # Get the data as pandas DataFrame
        df = dt.to_pandas()
        
        # Show basic stats
        print(f"🔢 Total rows: {len(df):,}")
        print(f"📋 Total columns: {len(df.columns)}")
        print()
        
        # Show column names and types
        print("📝 Columns:")
        for col in df.columns:
            dtype = str(df[col].dtype)
            print(f"   • {col}: {dtype}")
        print()
        
        # Show first few rows if data exists
        if len(df) > 0:
            print("👀 First 3 rows:")
            print(df.head(3).to_string())
            print()
            
            # Memory usage
            memory_mb = df.memory_usage(deep=True).sum() / (1024 * 1024)
            print(f"💾 Memory usage: {memory_mb:.1f} MB")
        else:
            print("📭 Table is empty (0 rows)")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        print()
        print("💡 Troubleshooting:")
        print("   • Check if MinIO containers are running")
        print("   • Verify the table path exists")
        print("   • Check environment variables")

if __name__ == "__main__":
    main()