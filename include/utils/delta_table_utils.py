#!/usr/bin/env python3
"""
Delta Table Utilities - ENTERPRISE EDITION

✅ FIXED: Uses enterprise table naming (bronze_github_keyword_extractions)
✅ FIXED: No subdirectories - clean table paths
✅ FIXED: Proper medallion architecture
"""

import os
import sys
from pathlib import Path
from typing import Dict, Optional, List

# Add project root for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from deltalake import DeltaTable
import pandas as pd


def get_minio_storage_options() -> Dict[str, str]:
    """
    Get MinIO storage options for deltalake library
    Uses direct environment variable access (more reliable)
    """
    # Get environment variables directly (same as your other modules)
    access_key = os.getenv('MINIO_ACCESS_KEY')
    secret_key = os.getenv('MINIO_SECRET_KEY')
    endpoint = os.getenv('MINIO_ENDPOINT')
    
    if not access_key or not secret_key or not endpoint:
        raise ValueError("MINIO_ACCESS_KEY, MINIO_SECRET_KEY, and MINIO_ENDPOINT environment variables are required")
    
    return {
        "AWS_ACCESS_KEY_ID": access_key,
        "AWS_SECRET_ACCESS_KEY": secret_key,
        "AWS_ENDPOINT_URL": f"http://{endpoint}",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


def get_enterprise_table_path(table_name: str, layer: str = "bronze") -> str:
    """
    ✅ ENTERPRISE: Generate proper table paths
    
    Args:
        table_name: Base table name (e.g., "keyword_extractions")
        layer: Data layer ("bronze", "silver", "gold")
    
    Returns:
        Enterprise table path: s3://delta-lake/bronze/bronze_github_keyword_extractions
    """

    enterprise_table_name = f"{layer}_github_{table_name}"
    enterprise_path = f"s3://delta-lake/{layer}/{enterprise_table_name}"
    
    print(f"🏢 Enterprise table path: {enterprise_path}")
    return enterprise_path


def get_table_count(table_name: str, layer: str = "bronze", data_source: str = "github") -> Optional[int]:
    """
    Get record count for a Delta table - ENTERPRISE VERSION
    
    Args:
        table_name: Name of the table (e.g., "keyword_extractions")
        layer: Data layer ("bronze", "silver", "gold")
        data_source: Data source ("github", etc.) - kept for compatibility
    
    Returns:
        int: Number of records, or None if table doesn't exist/error
    """

    table_path = get_enterprise_table_path(table_name, layer)
    
    try:
        storage_options = get_minio_storage_options()
        dt = DeltaTable(table_path, storage_options=storage_options)
        df = dt.to_pandas()
        return len(df)
    except Exception as e:
        print(f"❌ Error reading table {table_path}: {e}")
        return None
    

def get_table_count_for_date(table_name: str, date_str: str, layer: str = "bronze", data_source: str = "github") -> Optional[int]:
    """
    Get record count for a specific date in a Delta table - ENTERPRISE VERSION
    
    Args:
        table_name: Name of the table (e.g., "keyword_extractions")
        date_str: Date string in YYYY-MM-DD format
        layer: Data layer ("bronze", "silver", "gold")
        data_source: Data source ("github", etc.) - kept for compatibility
    
    Returns:
        int: Number of records for that date, or None if table doesn't exist/error
    """

    table_path = get_enterprise_table_path(table_name, layer)
    
    try:
        storage_options = get_minio_storage_options()
        dt = DeltaTable(table_path, storage_options=storage_options)
        
        # Filter by date - assuming you have a 'date' column
        df = dt.to_pandas()
        
        # Filter for specific date (adjust column name if different)
        date_filtered_df = df[df['date'] == date_str]
        
        return len(date_filtered_df)
        
    except Exception as e:
        print(f"❌ Error reading table {table_path} for date {date_str}: {e}")
        return None


def get_table_count_for_date_partition(table_name: str, date_str: str, layer: str = "bronze", data_source: str = "github") -> Optional[int]:
    """
    Alternative: Get count using Delta table partitioning - ENTERPRISE VERSION
    """

    table_path = get_enterprise_table_path(table_name, layer)
    
    try:
        storage_options = get_minio_storage_options()
        dt = DeltaTable(table_path, storage_options=storage_options)
        
        # Use Delta Lake's filtering capability (more efficient for large tables)
        df = dt.to_pandas(filters=[('date', '=', date_str)])
        
        return len(df)
        
    except Exception as e:
        print(f"❌ Error reading table {table_path} for date {date_str}: {e}")
        return None


def get_table_info(table_name: str, layer: str = "bronze", data_source: str = "github") -> Optional[Dict]:
    """
    Get comprehensive information about a Delta table - ENTERPRISE VERSION
    """
    table_path = get_enterprise_table_path(table_name, layer)
    
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
            "memory_mb": df.memory_usage(deep=True).sum() / (1024 * 1024) if len(df) > 0 else 0,
            "is_empty": len(df) == 0
        }
        
        # Add sample data if table has rows
        if len(df) > 0:
            info["sample_rows"] = df.head(3).to_dict('records')
        
        return info
        
    except Exception as e:
        print(f"❌ Error reading table {table_path}: {e}")
        return None


def list_tables_in_layer(layer: str = "bronze", data_source: str = "github") -> List[str]:
    """
    List all Delta tables in a layer - ENTERPRISE VERSION
    """
    try:
        # Import your existing MinIO connection
        from include.storage.minio_connect import get_minio_client, get_bucket_name
        
        client = get_minio_client()
        bucket = get_bucket_name()
        
        # List objects in layer (no subdirectories)
        prefix = f"{layer}/"
        
        response = client.list_objects_v2(
            Bucket=bucket,
            Prefix=prefix,
            Delimiter='/'
        )
        
        # Extract table names from common prefixes
        tables = []
        if 'CommonPrefixes' in response:
            for prefix_info in response['CommonPrefixes']:
                # Extract table name from prefix like "bronze/bronze_github_keyword_extractions/"
                table_path = prefix_info['Prefix']
                table_name = table_path.rstrip('/').split('/')[-1]
                

                if table_name.startswith(f"{layer}_{data_source}_"):
                    # Remove the prefix to get base table name
                    base_name = table_name.replace(f"{layer}_{data_source}_", "")
                    tables.append(base_name)
        
        return tables
        
    except Exception as e:
        print(f"❌ Error listing tables: {e}")
        return []


def check_table_exists(table_name: str, layer: str = "bronze", data_source: str = "github") -> bool:
    """
    Check if a Delta table exists - ENTERPRISE VERSION
    """

    table_path = get_enterprise_table_path(table_name, layer)
    
    try:
        storage_options = get_minio_storage_options()
        dt = DeltaTable(table_path, storage_options=storage_options)
        # Just check if we can get the version
        dt.version()
        return True
    except Exception:
        return False


def print_table_summary(table_name: str, layer: str = "bronze", data_source: str = "github"):
    """
    Print a nice summary of a Delta table - ENTERPRISE VERSION
    """

    enterprise_path = get_enterprise_table_path(table_name, layer)
    
    print(f"🔍 ENTERPRISE Delta Table Summary: {table_name}")
    print("=" * 50)
    
    info = get_table_info(table_name, layer, data_source)
    
    if info is None:
        print("❌ Table not found or error reading table")
        print(f"🏢 Expected path: {enterprise_path}")
        return
    
    print(f"📍 Table path: {info['table_path']}")
    print(f"📊 Table version: {info['version']}")
    print(f"🔢 Total rows: {info['row_count']:,}")
    print(f"📋 Total columns: {info['column_count']}")
    print()
    
    if info['columns']:
        print("📝 Columns:")
        for col in info['columns']:
            col_type = info['column_types'].get(col, 'unknown')
            print(f"   • {col}: {col_type}")
        print()
    
    if info['row_count'] > 0:
        print("👀 First 3 rows:")
        for i, row in enumerate(info.get('sample_rows', [])[:3]):
            print(f"   Row {i+1}: {row}")
        print()
        
        print(f"💾 Memory usage: {info['memory_mb']:.1f} MB")
    else:
        print("📭 Table is empty (0 rows)")
    
    print("=" * 50)


def main():
    """Command line interface for table checking - ENTERPRISE VERSION"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Check ENTERPRISE Delta tables in MinIO')
    parser.add_argument('table_name', help='Base table name (e.g., keyword_extractions)')
    parser.add_argument('--layer', default='bronze', help='Data layer (default: bronze)')
    parser.add_argument('--data-source', default='github', help='Data source (default: github)')
    parser.add_argument('--count-only', action='store_true', help='Show only record count')
    parser.add_argument('--list-tables', action='store_true', help='List all tables in layer')
    parser.add_argument('--date', help='Get count for specific date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    print("🏢 ENTERPRISE DELTA TABLE UTILITIES")
    print("=" * 50)
    
    if args.list_tables:
        tables = list_tables_in_layer(args.layer, args.data_source)
        print(f"📋 Enterprise tables in {args.layer}:")
        for table in tables:
            count = get_table_count(table, args.layer, args.data_source)
            enterprise_name = f"{args.layer}_{args.data_source}_{table}"
            print(f"   • {enterprise_name}: {count:,} records" if count is not None else f"   • {enterprise_name}: error")
        return
    
    if args.count_only:
        if args.date:
            count = get_table_count_for_date(args.table_name, args.date, args.layer, args.data_source)
        else:
            count = get_table_count(args.table_name, args.layer, args.data_source)
        
        if count is not None:
            print(f"{count:,}")
        else:
            print("ERROR")
            sys.exit(1)
    else:
        print_table_summary(args.table_name, args.layer, args.data_source)


if __name__ == "__main__":
    main()