"""
GitHub Batch Events Daily DAG

Clean Parquet-first pipeline: Bronze → Silver → Gold
Flow: Download GZ → Extract to Parquet → Aggregate to Parquet → Cleanup → Validate
No CSV files anywhere - pure Parquet efficiency.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone

# Import GitHub processing components
from include.github.github_downloader import download_github_date
from include.github.github_keyword_extractor import extract_keywords_for_date
from include.github.github_aggregator import aggregate_and_save_to_silver
from include.config.env_detection import ENV
from include.delta_lake.file_manager import delete_files_from_layer, file_exists

# START DATE - Starting date for range processing
START_DATE = "2024-12-29"


def get_date_range():
    """Get list of dates from START_DATE to yesterday, excluding specific range"""
    from datetime import datetime, timedelta
    
    start_date = datetime.strptime(START_DATE, "%Y-%m-%d")
    yesterday = datetime.now() - timedelta(days=1)
    
    # Define skip range
    skip_start = datetime.strptime("2024-12-31", "%Y-%m-%d")
    skip_end = datetime.strptime("2025-06-06", "%Y-%m-%d")
    
    dates = []
    current_date = start_date
    while current_date <= yesterday:
        # Skip dates in the specified range
        if skip_start <= current_date <= skip_end:
            current_date += timedelta(days=1)
            continue
            
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=1)
    
    return dates


def download_task(**context):
    """Download GitHub Archive GZ files to Bronze"""
    dates_to_process = get_date_range()
    
    print("DOWNLOAD TASK - GitHub Archive to Bronze")
    print(f"Environment: {ENV.environment.upper()}")
    print(f"Dates to process: {len(dates_to_process)}")
    
    download_results = {}
    
    for date_str in dates_to_process:
        print(f"\nProcessing {date_str}...")
        
        # Check if Silver Parquet files exist - skip if complete
        silver_files = [
            f"tech_trends_summary_{date_str}.parquet",
            f"top_repos_{date_str}.parquet", 
            f"event_summary_{date_str}.parquet",
            f"hourly_trends_{date_str}.parquet"
        ]
        existing_count = sum(1 for f in silver_files if file_exists(f, 'github', 'silver'))
        
        if existing_count == 4:
            print(f"{date_str}: Silver Parquet complete - SKIPPING")
            download_results[date_str] = {'status': 'skipped', 'reason': 'silver_exists'}
            continue
        
        print(f"{date_str}: Downloading GZ files...")
        result = download_github_date(date_str)
        download_results[date_str] = result
        
        if result['failed_downloads'] > 0:
            print(f"{date_str}: Failed hours: {result['failed_hours']}")
        else:
            print(f"{date_str}: Download complete")
    
    return download_results


def extract_task(**context):
    """Extract GZ → Parquet in Bronze layer"""
    download_results = context['task_instance'].xcom_pull(task_ids='download_github_data')
    
    print("EXTRACT TASK - GZ → Parquet (Bronze)")
    extract_results = {}
    
    for date_str, download_result in download_results.items():
        print(f"\nProcessing {date_str}...")
        
        if download_result.get('status') == 'skipped':
            print(f"{date_str}: Skipping extract (Silver exists)")
            extract_results[date_str] = {'status': 'skipped'}
            continue
        
        print(f"{date_str}: Extracting GZ → Parquet...")
        result = extract_keywords_for_date(date_str)
        extract_results[date_str] = result
        
        if result['failed_extractions'] > 0:
            print(f"{date_str}: Failed hours: {result['failed_hours']}")
        else:
            print(f"{date_str}: Extract complete")
    
    return extract_results


def aggregate_task(**context):
    """Aggregate Bronze Parquet → Silver Parquet"""
    extract_results = context['task_instance'].xcom_pull(task_ids='extract_keywords')
    
    print("AGGREGATE TASK - Bronze Parquet → Silver Parquet")
    aggregate_results = {}
    
    for date_str, extract_result in extract_results.items():
        print(f"\nProcessing {date_str}...")
        
        if extract_result.get('status') == 'skipped':
            print(f"{date_str}: Skipping aggregate (Silver exists)")
            aggregate_results[date_str] = {'status': 'skipped'}
            continue
        
        print(f"{date_str}: Aggregating to Silver Parquet...")
        result = aggregate_and_save_to_silver(date_str)
        aggregate_results[date_str] = result
        
        if not result['success']:
            reason = result.get('reason', 'unknown')
            print(f"{date_str}: Aggregation failed - {reason}")
        else:
            print(f"{date_str}: Aggregate complete")
    
    return aggregate_results


def cleanup_task(**context):
    """Clean up Bronze layer (GZ + intermediate Parquet)"""
    aggregate_results = context['task_instance'].xcom_pull(task_ids='aggregate_to_silver')
    
    print("CLEANUP TASK - Bronze GZ + Parquet removal")
    cleanup_results = {}
    
    for date_str, aggregate_result in aggregate_results.items():
        print(f"\nProcessing {date_str}...")
        
        if aggregate_result.get('status') == 'skipped' or not aggregate_result.get('success'):
            print(f"{date_str}: Skipping cleanup (not processed)")
            cleanup_results[date_str] = {'status': 'skipped'}
            continue
        
        print(f"{date_str}: Cleaning Bronze layer...")
        
        # Delete raw GZ files (GitHub Archive data)
        gz_files = [f"{date_str}-{hour:02d}.json.gz" for hour in range(24)]
        delete_files_from_layer(gz_files, 'github', 'bronze', force_cleanup=True)
        
        # Delete intermediate hourly Parquet files
        parquet_files = [f"keywords-{date_str}-hour-{hour:02d}.parquet" for hour in range(24)]
        delete_files_from_layer(parquet_files, 'github', 'bronze', force_cleanup=True)
        
        total_cleaned = len(gz_files) + len(parquet_files)
        cleanup_results[date_str] = {'total_cleaned': total_cleaned}
        print(f"{date_str}: Cleaned {total_cleaned} Bronze files")
    
    return cleanup_results


def validate_task(**context):
    """Validate all dates have Silver Parquet files"""
    dates_to_check = get_date_range()
    
    print("VALIDATE TASK - Silver Parquet completeness")
    print(f"Checking {len(dates_to_check)} dates")
    
    valid_dates = 0
    invalid_dates = []
    
    for date_str in dates_to_check:
        silver_files = [
            f"tech_trends_summary_{date_str}.parquet",
            f"top_repos_{date_str}.parquet",
            f"event_summary_{date_str}.parquet", 
            f"hourly_trends_{date_str}.parquet"
        ]
        
        silver_count = sum(1 for f in silver_files if file_exists(f, 'github', 'silver'))
        
        if silver_count == 4:
            valid_dates += 1
        else:
            invalid_dates.append(f"{date_str} ({silver_count}/4)")
    
    print("\nVALIDATION SUMMARY:")
    print(f"Valid: {valid_dates}/{len(dates_to_check)}")
    
    if invalid_dates:
        print(f"Invalid: {', '.join(invalid_dates)}")
        raise Exception(f"Validation failed for {len(invalid_dates)} dates")
    
    print("ALL DATES VALIDATED - Silver Parquet complete")
    return {'valid_dates': valid_dates, 'total_dates': len(dates_to_check)}


# DAG Definition
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2024, 12, 1, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="github_batch_events_daily",
    default_args=default_args,
    description=f"GitHub Batch pipeline from {START_DATE} to yesterday",
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["github", "batch", "efficient"],
) as dag:

    download = PythonOperator(
        task_id="download_github_data",
        python_callable=download_task,
        execution_timeout=timedelta(hours=2),
    )

    extract = PythonOperator(
        task_id="extract_keywords",
        python_callable=extract_task,
        execution_timeout=timedelta(hours=3),
    )

    aggregate = PythonOperator(
        task_id="aggregate_to_silver",
        python_callable=aggregate_task,
        execution_timeout=timedelta(hours=1),
    )

    cleanup = PythonOperator(
        task_id="cleanup_bronze",
        python_callable=cleanup_task,
        execution_timeout=timedelta(minutes=30),
    )

    validate = PythonOperator(
        task_id="validate_success",
        python_callable=validate_task,
        execution_timeout=timedelta(minutes=15),
    )

    # Clean linear flow
    download >> extract >> aggregate >> cleanup >> validate