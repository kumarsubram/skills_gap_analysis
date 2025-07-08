"""
Daily GitHub Bronze Pipeline
"""

import sys
import os
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator

# CONFIGURATION
START_DATE = "2025-07-01"
SKIP_START = None  # Set to "2025-01-15" to skip dates
SKIP_END = None    # Set to "2025-01-20" to skip dates  
MIN_RECORDS_THRESHOLD = 5  # Bronze complete if > 5 records


def check_bronze_complete_for_date(date_str: str) -> bool:
    """Check if Bronze table has > 5 records FOR A SPECIFIC DATE - DEBUG VERSION"""
    try:
        from include.utils.delta_table_utils import get_table_count_for_date
        
        print(f"🔍 DEBUG: Checking Bronze for date {date_str}")
        print(f"🔍 DEBUG: Using threshold: {MIN_RECORDS_THRESHOLD}")
        
        # Get count for this specific date
        count = get_table_count_for_date("keyword_extractions", date_str)
        
        print(f"🔍 DEBUG: Raw count returned: {count} (type: {type(count)})")
        
        if count is None:
            print(f"❌ DEBUG: Count is None for {date_str} - table may not exist or no data")
            return False
        
        is_complete = count > MIN_RECORDS_THRESHOLD
        print(f"🔍 DEBUG: Date {date_str}: {count} records > {MIN_RECORDS_THRESHOLD}? {is_complete}")
        
        # Extra debugging - check total table count
        try:
            from include.utils.delta_table_utils import get_table_count
            total_count = get_table_count("keyword_extractions")
            print(f"🔍 DEBUG: Total table records: {total_count}")
        except Exception as e:
            print(f"⚠️  DEBUG: Could not get total count: {e}")
        
        return is_complete
        
    except ImportError as e:
        print(f"❌ DEBUG: Import error: {e}")
        print("💡 DEBUG: Check if delta_table_utils.py is in include/utils/")
        return False
    except Exception as e:
        print(f"❌ DEBUG: Error checking Bronze for date {date_str}: {e}")
        print(f"❌ DEBUG: Exception type: {type(e)}")
        import traceback
        traceback.print_exc()
        return False


def get_next_unprocessed_bronze_date():
    """Find the next date that needs Bronze processing"""
    from datetime import datetime, timedelta
    
    start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    yesterday = datetime.now() - timedelta(days=1)

    skip_start = datetime.strptime(SKIP_START, "%Y-%m-%d") if SKIP_START else None
    skip_end = datetime.strptime(SKIP_END, "%Y-%m-%d") if SKIP_END else None

    current_date = start_date
    while current_date <= yesterday:
        # Skip dates in the SKIP range (inclusive)
        if skip_start and skip_end and skip_start <= current_date <= skip_end:
            print(f"⏩ Skipping {current_date.strftime('%Y-%m-%d')} (in skip range)")
            current_date += timedelta(days=1)
            continue

        date_str = current_date.strftime("%Y-%m-%d")

        if not check_bronze_complete_for_date(date_str):
            return date_str

        current_date += timedelta(days=1)

    return None


def process_continuous_dates(**context):
    """
    Process all dates continuously from START_DATE to yesterday
    Stops when all dates are complete
    """
    
    print("🔄 CONTINUOUS DATE PROCESSING")
    print("=" * 50)
    print(f"Start date: {START_DATE}")
    print("Target: Process until yesterday")
    print(f"Skip range: {SKIP_START} to {SKIP_END}" if SKIP_START and SKIP_END else "No skips")
    
    processed_dates = []
    failed_dates = []
    max_dates_per_run = 365  # Safety limit
    
    for iteration in range(max_dates_per_run):
        print(f"\n🔍 ITERATION {iteration + 1}: Finding next unprocessed date...")
        
        target_date = get_next_unprocessed_bronze_date()
        
        if not target_date:
            print("🏁 ALL DATES COMPLETE!")
            print(f"✅ Successfully processed: {processed_dates}")
            if failed_dates:
                print(f"❌ Failed dates: {failed_dates}")
            break
        
        print(f"🎯 Processing: {target_date}")
        
        try:
            success = process_single_date_complete(target_date)
            
            if success:
                processed_dates.append(target_date)
                print(f"✅ COMPLETED: {target_date}")
                
                # Verify completion
                if check_bronze_complete_for_date(target_date):
                    print(f"✅ VERIFIED: {target_date} Bronze is complete")
                else:
                    print(f"⚠️  WARNING: {target_date} may not be fully complete")
            else:
                failed_dates.append(target_date)
                print(f"❌ FAILED: {target_date}")
                
        except Exception as e:
            print(f"❌ ERROR processing {target_date}: {e}")
            failed_dates.append(target_date)
    
    print("\n📊 FINAL SUMMARY:")
    print(f"   ✅ Processed: {len(processed_dates)} dates")
    print(f"   ❌ Failed: {len(failed_dates)} dates")
    print(f"   📅 Successful dates: {processed_dates}")
    
    return {
        'processed_count': len(processed_dates),
        'failed_count': len(failed_dates),
        'processed_dates': processed_dates,
        'failed_dates': failed_dates
    }


def process_single_date_complete(date_str: str) -> bool:
    """
    Process a single date: Download → Extract → Bronze Delta → Cleanup
    """
    
    print(f"\n📅 PROCESSING: {date_str}")
    print("=" * 40)
    
    try:
        # Step 1: Download GitHub data
        print(f"📥 Step 1: Downloading {date_str}")
        from include.github.downloader import download_github_date
        download_result = download_github_date(date_str)
        
        if download_result['failed_downloads'] > 12:
            print(f"❌ Too many download failures: {download_result['failed_downloads']}")
            return False
        
        # Step 2: Extract keywords
        print(f"🔍 Step 2: Extracting keywords for {date_str}")
        from include.github.keyword_extractor import extract_keywords_for_date
        extract_result = extract_keywords_for_date(date_str)
        
        if extract_result['failed_extractions'] > 12:
            print(f"❌ Too many extraction failures: {extract_result['failed_extractions']}")
            return False
        
        # Step 3: Create Bronze Delta
        print(f"💾 Step 3: Creating Bronze Delta for {date_str}")
        spark_success = run_spark_bronze_job(date_str)
        
        if not spark_success:
            print(f"❌ Spark Bronze job failed for {date_str}")
            return False
        
        # Step 4: Cleanup temp files
        print(f"🧹 Step 4: Cleaning up temp files for {date_str}")
        cleanup_temp_files(date_str)
        
        # Step 5: Verify completion
        print(f"🔍 Step 5: Verifying Bronze completion for {date_str}")
        is_complete = check_bronze_complete_for_date(date_str)
        
        if is_complete:
            print(f"✅ {date_str} FULLY COMPLETE!")
            return True
        else:
            print(f"❌ {date_str} verification failed")
            return False
            
    except Exception as e:
        print(f"❌ Error processing {date_str}: {e}")
        return False


def run_spark_bronze_job(date_str: str) -> bool:
    """Run the Spark job for Bronze Delta creation"""
    
    spark_cmd = [
        "/home/airflow/.local/bin/spark-submit",
        "--master", "spark://spark-master:7077",
        "--jars", "/opt/spark/jars/hadoop-aws-3.3.6.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.367.jar,/opt/spark/jars/delta-spark_2.13-4.0.0.jar,/opt/spark/jars/delta-storage-4.0.0.jar",
        "--conf", "spark.executor.memory=1g",
        "--conf", "spark.driver.memory=1500m",
        "--conf", "spark.executor.cores=1",
        "--conf", "spark.driver.maxResultSize=256m",
        "--conf", "spark.sql.adaptive.enabled=true",
        "--conf", "spark.sql.adaptive.coalescePartitions.enabled=true",
        "--conf", "spark.serializer=org.apache.spark.serializer.KryoSerializer",
        "--deploy-mode", "client",
        "--conf", "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
        "--conf", "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "--conf", f"spark.hadoop.fs.s3a.endpoint=http://{os.getenv('MINIO_ENDPOINT')}",
        "--conf", f"spark.hadoop.fs.s3a.access.key={os.getenv('MINIO_ACCESS_KEY')}",
        "--conf", f"spark.hadoop.fs.s3a.secret.key={os.getenv('MINIO_SECRET_KEY')}",
        "--conf", "spark.hadoop.fs.s3a.path.style.access=true",
        "--conf", "spark.hadoop.fs.s3a.connection.ssl.enabled=false",
        "--conf", "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
        "--conf", "spark.hadoop.fs.s3a.threads.keepalivetime=60",
        "--conf", "spark.hadoop.fs.s3a.connection.establish.timeout=30000",
        "--conf", "spark.hadoop.fs.s3a.connection.timeout=200000",
        "--conf", "spark.hadoop.fs.s3a.connection.ttl=300000",
        "--conf", "spark.hadoop.fs.s3a.retry.interval=500",
        "--conf", "spark.hadoop.fs.s3a.retry.throttle.interval=100",
        "--conf", "spark.hadoop.fs.s3a.assumed.role.session.duration=1800000",
        "--conf", "spark.hadoop.fs.s3a.multipart.purge.age=86400000",
        "--conf", "spark.hadoop.fs.s3a.connection.maximum=100",
        "--conf", "spark.hadoop.fs.s3a.fast.upload=true",
        "/opt/airflow/include/spark_jobs/github_raw_to_bronze_delta.py",
        date_str
    ]
    
    try:
        print(f"🚀 Running Spark job for {date_str}")
        result = subprocess.run(
            spark_cmd, 
            capture_output=True, 
            text=True, 
            timeout=2700  # 45 minutes timeout
        )
        
        if result.returncode == 0:
            print(f"✅ Spark job succeeded for {date_str}")
            return True
        else:
            print(f"❌ Spark job failed for {date_str}")
            print(f"STDERR: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"❌ Spark job timeout for {date_str}")
        return False
    except Exception as e:
        print(f"❌ Spark job error for {date_str}: {e}")
        return False


def cleanup_temp_files(date_str: str):
    """
    Comprehensive cleanup - same as original post_bronze_cleanup
    """
    print("🧹 POST-BRONZE CLEANUP & VALIDATION")
    print("=" * 40)
    print(f"🎯 Processing Date: {date_str}")
    
    # Import cleanup functions
    from include.storage.file_manager import delete_files_from_layer
    from include.storage.minio_connect import get_minio_client, get_bucket_name
    from datetime import datetime, timedelta
    import re
    
    try:
        # Smart cleanup (temp files only, keep Bronze Delta forever)
        print("\n🗑️  Smart cleanup (preserving Bronze Delta)...")
        
        # Clean parquet files from current date (they're now in Bronze Delta)
        parquet_files = [f"keywords-{date_str}-hour-{hour:02d}.parquet" for hour in range(24)]
        delete_files_from_layer(parquet_files, 'github', 'bronze', force_cleanup=True)
        
        print(f"🗑️  Cleaned temp parquet files for {date_str}")
        
        # Delete files older than yesterday
        yesterday = (datetime.now() - timedelta(days=1)).date()
        client = get_minio_client()
        bucket = get_bucket_name()
        
        response = client.list_objects_v2(Bucket=bucket, Prefix='bronze/github/')
        old_files = []
        
        if 'Contents' in response:
            for obj in response['Contents']:
                filename = obj['Key'].split('/')[-1]
                
                if filename.endswith('.json.gz') or filename.endswith('.parquet'):
                    date_match = re.search(r'(\d{4}-\d{2}-\d{2})', filename)
                    if date_match:
                        try:
                            file_date = datetime.strptime(date_match.group(1), '%Y-%m-%d').date()
                            if file_date < yesterday:
                                old_files.append(filename)
                        except ValueError:
                            pass
        
        if old_files:
            delete_files_from_layer(old_files, 'github', 'bronze', force_cleanup=True)
            print(f"🗑️  Cleaned {len(old_files)} old files")
        
        print("💾 Bronze Delta table preserved (permanent storage)")
        
        print(f"\n🎉 SUCCESS: {date_str} BRONZE PROCESSING COMPLETE!")
        print("💾 Bronze Delta table created")
        print("🗑️  Temp files cleaned up")
        print("♾️  Bronze Delta preserved forever")
        print("🎯 Ready for Silver processing (separate pipeline)")
        
    except Exception as e:
        print(f"\n❌ ERROR IN CLEANUP: {e}")
        raise


# DAG Definition
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2024, 12, 1, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="daily_github_bronze",
    default_args=default_args,
    description=f"GitHub Bronze Pipeline - Continuous from {START_DATE} to yesterday",
    schedule="0 11 * * *",  # Daily at 6 AM EST
    catchup=False,
    max_active_runs=1,  # Prevent concurrent runs
    max_active_tasks=1,  # One task at a time
    tags=["github", "bronze", "delta-lake", "continuous"],
    doc_md=f"""
    # GitHub Bronze Pipeline - Continuous Processing
    
    ## 🔄 **Processing Flow**
    1. Starts from {START_DATE}
    2. Processes each unprocessed date until yesterday
    3. Skips dates in range {SKIP_START} to {SKIP_END} if configured
    4. For each date: Download → Extract → Bronze Delta → Cleanup → Verify
    5. Stops when all dates from {START_DATE} to yesterday are complete
    
    ## 🎯 **Completion Criteria**
    - Bronze complete = table count > {MIN_RECORDS_THRESHOLD} records
    - Processes up to 365 dates per run (safety limit)
    
    ## 🕐 **Runtime**
    - ~45 minutes per date
    - 6-day timeout for long processing runs
    """,
)

# Single task that processes all dates
continuous_task = PythonOperator(
    task_id="process_continuous_dates",
    python_callable=process_continuous_dates,
    execution_timeout=timedelta(days=6),  # 6-day timeout
    dag=dag,
)