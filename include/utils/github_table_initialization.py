"""
Clean GitHub Table Initialization with Proper Partitioning
Replace: include/utils/github_table_initialization.py
"""

import sys
from pathlib import Path
from typing import Dict
import pyarrow as pa
from deltalake import write_deltalake

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from include.schemas.github_schemas import (
    get_silver_keyword_trends_schema,
    get_gold_technology_daily_activity_schema,
    get_analytics_technology_trends_7d_schema,
    get_analytics_technology_trends_30d_schema,
    get_analytics_technology_trends_90d_schema,
    get_analytics_technology_trends_alltime_schema
)
from include.utils.delta_table_utils import (
    get_enterprise_table_path,
    get_minio_storage_options
)


def create_table(table_path: str, schema: pa.Schema, storage_options: Dict, 
                table_name: str, partition_cols: list = None) -> bool:
    """Create Delta table with proper partitioning"""
    try:
        print(f"📝 Creating {table_name}")
        print(f"   Path: {table_path}")
        if partition_cols:
            print(f"   🗂️ Partitioned by: {partition_cols}")
        
        # Create empty table
        empty_arrays = [pa.array([], type=field.type) for field in schema]
        empty_table = pa.table(empty_arrays, schema=schema)
        
        write_deltalake(
            table_or_uri=table_path,
            data=empty_table,
            storage_options=storage_options,
            mode="overwrite",
            partition_by=partition_cols
        )
        
        print(f"✅ Created {table_name}")
        return True
        
    except Exception as e:
        print(f"❌ Failed to create {table_name}: {e}")
        return False


def create_all_tables() -> Dict:
    """Create all Silver, Gold, and Analytics tables with proper partitioning"""
    print("🏗️ CREATING ALL TABLES WITH PROPER PARTITIONING")
    print("=" * 60)
    
    storage_options = get_minio_storage_options()
    results = {}
    
    # Silver Table - DATE PARTITIONED
    print("\n🥈 SILVER TABLE")
    silver_path = get_enterprise_table_path("keyword_trends", "silver")
    silver_schema = get_silver_keyword_trends_schema()
    results['silver'] = create_table(
        silver_path, silver_schema, storage_options,
        "Silver keyword trends", ["date"]
    )
    
    # Gold Table - DATE PARTITIONED
    print("\n🥇 GOLD TABLE")
    gold_path = get_enterprise_table_path("technology_daily_activity", "gold")
    gold_schema = get_gold_technology_daily_activity_schema()
    results['gold'] = create_table(
        gold_path, gold_schema, storage_options,
        "Gold technology daily activity", ["date"]
    )
    
    # Analytics Tables - ANALYSIS_DATE PARTITIONED
    analytics_configs = [
        ("7d", "s3://delta-lake/analytics/analytics_github_technology_trends_7d", 
         get_analytics_technology_trends_7d_schema),
        ("30d", "s3://delta-lake/analytics/analytics_github_technology_trends_30d", 
         get_analytics_technology_trends_30d_schema),
        ("90d", "s3://delta-lake/analytics/analytics_github_technology_trends_90d", 
         get_analytics_technology_trends_90d_schema),
        ("alltime", "s3://delta-lake/analytics/analytics_github_technology_trends_alltime", 
         get_analytics_technology_trends_alltime_schema)
    ]
    
    print("\n📊 ANALYTICS TABLES")
    for timeframe, path, schema_func in analytics_configs:
        schema = schema_func()
        results[f'analytics_{timeframe}'] = create_table(
            path, schema, storage_options,
            f"Analytics {timeframe} trends", ["analysis_date"]
        )   
    
    # Summary
    total_tables = len(results)
    successful_tables = sum(1 for success in results.values() if success)
    
    print(f"\n📊 CREATION SUMMARY: {successful_tables}/{total_tables} tables created")
    
    return {
        'success': successful_tables == total_tables,
        'tables': results,
        'total': total_tables,
        'successful': successful_tables
    }

def ensure_all_github_tables_exist(include_analytics: bool = True) -> Dict:
    """Main function for DAG - create all tables"""
    return create_all_tables()


if __name__ == "__main__":
    print("🧪 TESTING CLEAN TABLE CREATION")
    result = create_all_tables()
    
    if result['success']:
        print("\n🎉 ALL TABLES CREATED SUCCESSFULLY")
    else:
        print(f"\n❌ FAILED: {result['successful']}/{result['total']} tables created")