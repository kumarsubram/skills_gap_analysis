"""
Daily Delta Lake VACUUM DAG - Simple Python Implementation
==========================================================

Standalone DAG using deltalake Python library (no Spark needed!)
Runs 30 minutes after analytics processing to clean up old file versions.

Schedule: 5:30 PM EST (30 minutes after analytics DAG)
"""

import os
from datetime import datetime, timedelta, timezone

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator

# VACUUM Configuration
VACUUM_RETENTION_HOURS = 24   # Keep 1 day of file versions for regular tables
VACUUM_DRY_RUN = False        # Set to True for testing

# Streaming table with special 1-hour retention
STREAMING_TABLE_PATH = "s3://delta-lake/bronze/bronze_github_streaming_keyword_extractions"
STREAMING_RETENTION_HOURS = 24  # 1 day retention for streaming table

# Regular Delta table paths (7 days minimum retention)
DELTA_TABLE_PATHS = [
    # Analytics tables (4 tables)
    "s3://delta-lake/analytics/analytics_github_technology_trends_30d",
    "s3://delta-lake/analytics/analytics_github_technology_trends_7d", 
    "s3://delta-lake/analytics/analytics_github_technology_trends_90d",
    "s3://delta-lake/analytics/analytics_github_technology_trends_alltime",
    
    # Bronze tables (4 tables - streaming table handled separately)
    "s3://delta-lake/bronze/bronze_github_keyword_extractions",
    "s3://delta-lake/bronze/bronze_jobs_greenhouse_rawdata",
    "s3://delta-lake/bronze/bronze_jobs_hackernews_rawdata",
    "s3://delta-lake/bronze/bronze_jobs_remoteok_rawdata",
    
    # Gold table (1 table)
    "s3://delta-lake/gold/gold_github_technology_daily_activity",
    
    # Silver table (1 table)
    "s3://delta-lake/silver/silver_github_keyword_trends",
]


def get_storage_options():
    """Get MinIO/S3 storage options for deltalake library"""
    return {
        "AWS_ACCESS_KEY_ID": os.getenv('MINIO_ACCESS_KEY'),
        "AWS_SECRET_ACCESS_KEY": os.getenv('MINIO_SECRET_KEY'),
        "AWS_ENDPOINT_URL": "http://{}".format(os.getenv('MINIO_ENDPOINT')),
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"
    }


def check_vacuum_prerequisites(**context):
    """Check if we can run VACUUM - simple checks only"""
    print("🔍 SIMPLE VACUUM PREREQUISITES")
    print("=" * 40)
    
    # Check 1: deltalake library
    try:
        from deltalake import DeltaTable
        print("✅ deltalake library available")
    except ImportError:
        print("❌ deltalake library not installed")
        return False
    
    # Check 2: Environment variables
    required_vars = ['MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY', 'MINIO_ENDPOINT']
    for var in required_vars:
        if os.getenv(var):
            print("✅ {} is set".format(var))
        else:
            print("❌ {} not set".format(var))
            return False
    
    print("🎉 Prerequisites passed - ready for simple VACUUM")
    return True


def vacuum_delta_tables_simple(**context):
    """
    Simple VACUUM using deltalake Python library - NO SPARK NEEDED!
    Handles streaming table separately with 1-day retention.
    """
    print("🧹 SIMPLE DELTA LAKE VACUUM (NO SPARK)")
    print("=" * 50)
    
    total_regular_tables = len(DELTA_TABLE_PATHS)
    total_tables = total_regular_tables + 1  # Include streaming table
    retention_days = VACUUM_RETENTION_HOURS / 24
    streaming_retention_days = STREAMING_RETENTION_HOURS / 24
    
    print("📋 Processing {} tables total".format(total_tables))
    print("🕐 Regular tables retention: {} hours ({:.1f} days)".format(VACUUM_RETENTION_HOURS, retention_days))
    print("🌊 Streaming table retention: {} hours ({:.1f} days)".format(STREAMING_RETENTION_HOURS, streaming_retention_days))
    print("🧪 Dry run: {}".format(VACUUM_DRY_RUN))
    print("🚀 Method: deltalake Python library (no Spark session needed)")
    
    try:
        # Import deltalake
        from deltalake import DeltaTable
        
        # Get storage options
        storage_options = get_storage_options()
        print("🔗 MinIO connection configured")
        
        # Initialize tracking
        results = {
            'successful': [],
            'failed': [],
            'skipped': [],
            'total_checked': total_tables
        }
        
        # Process streaming table first with special retention
        print("\n[STREAMING] 🌊 {}".format(STREAMING_TABLE_PATH))
        
        try:
            # Try to load streaming table
            try:
                dt = DeltaTable(STREAMING_TABLE_PATH, storage_options=storage_options)
                print("✅ Streaming table loaded successfully")
            except Exception as load_error:
                error_msg = str(load_error)
                if "Path does not exist" in error_msg or "not a delta table" in error_msg:
                    print("⏩ Skipped - streaming table not found")
                    results['skipped'].append({
                        'table': STREAMING_TABLE_PATH,
                        'reason': 'table_not_found'
                    })
                else:
                    raise load_error
            else:
                # Perform VACUUM on streaming table
                if VACUUM_DRY_RUN:
                    # Dry run mode
                    files_to_delete = dt.vacuum(retention_hours=STREAMING_RETENTION_HOURS, dry_run=True)
                    files_count = len(files_to_delete)
                    print("🧪 DRY RUN: Would delete {} files".format(files_count))
                    
                    results['successful'].append({
                        'table': STREAMING_TABLE_PATH,
                        'files_would_delete': files_count,
                        'mode': 'dry_run',
                        'retention_hours': STREAMING_RETENTION_HOURS
                    })
                else:
                    # Actual VACUUM
                    deleted_files = dt.vacuum(retention_hours=STREAMING_RETENTION_HOURS, dry_run=False)
                    deleted_count = len(deleted_files)
                    print("✅ VACUUM completed - deleted {} files".format(deleted_count))
                    
                    results['successful'].append({
                        'table': STREAMING_TABLE_PATH,
                        'files_deleted': deleted_count,
                        'retention_hours': STREAMING_RETENTION_HOURS
                    })
                
        except Exception as table_error:
            error_msg = str(table_error)
            print("❌ VACUUM failed: {}".format(error_msg))
            results['failed'].append({
                'table': STREAMING_TABLE_PATH,
                'error': error_msg
            })
        
        # Process regular tables with standard retention
        for i, table_path in enumerate(DELTA_TABLE_PATHS, 1):
            print("\n[{}/{}] 🔍 {}".format(i, total_regular_tables, table_path))
            
            try:
                # Try to load table
                try:
                    dt = DeltaTable(table_path, storage_options=storage_options)
                    print("✅ Table loaded successfully")
                except Exception as load_error:
                    error_msg = str(load_error)
                    if "Path does not exist" in error_msg or "not a delta table" in error_msg:
                        print("⏩ Skipped - table not found")
                        results['skipped'].append({
                            'table': table_path,
                            'reason': 'table_not_found'
                        })
                        continue
                    else:
                        raise load_error
                
                # Perform VACUUM with regular retention (will use 7 days minimum)
                if VACUUM_DRY_RUN:
                    # Dry run mode - use 7 days (168 hours) as minimum
                    retention_to_use = max(VACUUM_RETENTION_HOURS, 168)
                    files_to_delete = dt.vacuum(retention_hours=retention_to_use, dry_run=True)
                    files_count = len(files_to_delete)
                    print("🧪 DRY RUN: Would delete {} files (using {} hours retention)".format(files_count, retention_to_use))
                    
                    results['successful'].append({
                        'table': table_path,
                        'files_would_delete': files_count,
                        'mode': 'dry_run',
                        'retention_hours': retention_to_use
                    })
                else:
                    # Actual VACUUM - use 7 days (168 hours) as minimum
                    retention_to_use = max(VACUUM_RETENTION_HOURS, 168)
                    deleted_files = dt.vacuum(retention_hours=retention_to_use, dry_run=False)
                    deleted_count = len(deleted_files)
                    print("✅ VACUUM completed - deleted {} files (using {} hours retention)".format(deleted_count, retention_to_use))
                    
                    results['successful'].append({
                        'table': table_path,
                        'files_deleted': deleted_count,
                        'retention_hours': retention_to_use
                    })
                
            except Exception as table_error:
                error_msg = str(table_error)
                print("❌ VACUUM failed: {}".format(error_msg))
                results['failed'].append({
                    'table': table_path,
                    'error': error_msg
                })
        
        # Generate summary
        successful = len(results['successful'])
        failed = len(results['failed'])
        skipped = len(results['skipped'])
        
        print("\n📊 SIMPLE VACUUM SUMMARY:")
        print("   ✅ Successful: {}/{}".format(successful, total_tables))
        print("   ❌ Failed: {}".format(failed))
        print("   ⏩ Skipped: {}".format(skipped))
        
        # Show details for failed tables
        if results['failed']:
            print("\n❌ Failed tables:")
            for fail in results['failed']:
                table_name = fail['table'].split('/')[-1]
                print("   • {}: {}".format(table_name, fail['error']))
        
        # Show total files processed (if not dry run)
        if not VACUUM_DRY_RUN and successful > 0:
            total_deleted = sum(r.get('files_deleted', 0) for r in results['successful'])
            print("\n🗑️ Total files deleted: {}".format(total_deleted))
        
        # Final message
        if VACUUM_DRY_RUN:
            print("\n🧪 DRY RUN COMPLETE - No files actually deleted")
        else:
            print("\n🧹 VACUUM COMPLETE - Old file versions cleaned up")
            print("💾 Disk space freed up!")
        
        # Determine status
        if failed == 0:
            if successful > 0:
                status = 'success'
            else:
                status = 'no_action_needed'
        elif successful > 0:
            status = 'partial_success'
        else:
            status = 'failed'
        
        return {
            'status': status,
            'successful': successful,
            'failed': failed,
            'skipped': skipped,
            'total_tables': total_tables,
            'retention_hours': VACUUM_RETENTION_HOURS,
            'streaming_retention_hours': STREAMING_RETENTION_HOURS,
            'dry_run': VACUUM_DRY_RUN,
            'method': 'deltalake_python',
            'spark_required': False
        }
        
    except ImportError:
        print("❌ deltalake library not available")
        return {
            'status': 'failed',
            'error': 'deltalake library not installed',
            'suggestion': 'Install with: pip install deltalake'
        }
    except Exception as e:
        error_msg = str(e)
        print("❌ VACUUM operation failed: {}".format(error_msg))
        return {
            'status': 'failed',
            'error': error_msg
        }


# DAG Definition
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 7, 1, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="daily_delta_vacuum",
    default_args=default_args,
    description="Daily Delta Lake VACUUM - Simple Python (no Spark)",
    schedule="45 4 * * *", 
    catchup=False,
    max_active_runs=1,
    tags=["delta-lake", "vacuum", "cleanup", "simple", "no-spark"],
    doc_md="""
    # Daily Delta Lake VACUUM
    
    **Simple Python implementation using deltalake library**
    
    ## 🕐 Schedule
    - Runs daily at 5:30 PM EST (10:30 PM UTC)
    - 30 minutes after analytics DAG completes
    - Perfect timing during batch window
    
    ## 🧹 What it does
    - VACUUMs all 11 Delta tables (Analytics, Bronze, Gold, Silver)
    - Removes file versions older than 24 hours
    - Frees up disk space automatically
    - No Spark session required!
    
    ## 🚀 Method
    - Uses `deltalake` Python library directly
    - Simple, fast, and reliable
    - Connects directly to MinIO/S3
    - No complex Spark configuration
    
    ## 🛡️ Safety
    - 24-hour retention (keeps 1 day of versions)
    - Dry run mode available for testing
    - Skips missing tables gracefully
    - Detailed logging and error handling
    
    ## 🔧 Configuration
    - Set `VACUUM_DRY_RUN = True` for testing
    - Adjust `VACUUM_RETENTION_HOURS` if needed
    - Add/remove tables in `DELTA_TABLE_PATHS`
    """,
)

# Task 1: Check prerequisites
prereq_task = PythonOperator(
    task_id="check_vacuum_prerequisites",
    python_callable=check_vacuum_prerequisites,
    dag=dag,
)

# Task 2: VACUUM tables
vacuum_task = PythonOperator(
    task_id="vacuum_delta_tables_simple",
    python_callable=vacuum_delta_tables_simple,
    execution_timeout=timedelta(minutes=30),  # Should be fast without Spark
    dag=dag,
)

# Task dependencies
prereq_task >> vacuum_task