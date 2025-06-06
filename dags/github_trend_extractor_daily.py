"""
GitHub Trends Extractor (Daily Sequential Processing)

Processes one date at a time through the complete pipeline:
Date 1: download → process → upload → Date 2: download → process → upload → etc.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import time
from typing import Dict

# Configuration (single source of truth)
DEFAULT_START_DATE = "2025-02-24"

# Import functions directly to avoid circular imports
from include.github_processing.keyword_processor import load_keywords
from include.github_processing.data_collector import is_date_already_processed
from include.github_processing.core_processor import determine_date_range, process_single_date
from include.github_processing.azure_uploader import (
    get_azure_credentials,
    verify_azure_silver_structure,
    upload_date_to_azure_silver
)

def setup_and_validate(default_start_date=None, **context) -> Dict:
    """Task 1: Setup and validate processing environment"""
    print("🔧 TASK 1: Setup and Validation")
    print("=" * 50)
    
    start_time = time.time()
    
    try:
        # Load and validate keywords
        print("📋 Loading keyword configuration...")
        keywords = load_keywords()
        print(f"✅ Loaded {len(keywords)} keywords successfully")
        
        # Get DAG configuration
        conf = context.get("dag_run", {}).conf or {}
        dates_to_process = determine_date_range(conf, default_start_date)
        
        print(f"📅 Date range: {dates_to_process[0]} to {dates_to_process[-1]}")
        print(f"📊 Total dates: {len(dates_to_process)}")
        
        # Validate Azure credentials
        print("☁️  Validating Azure credentials...")
        storage_account, storage_key = get_azure_credentials()
        print(f"✅ Azure credentials valid for account: {storage_account}")
        
        # Verify Azure folder structure
        print("🔍 Verifying Azure Silver folder structure...")
        structure_ok = verify_azure_silver_structure()
        if structure_ok:
            print("✅ Azure Silver structure verified")
        else:
            print("⚠️  Azure Silver structure may need setup")
        
        # Check processing status
        already_processed = sum(1 for date in dates_to_process if is_date_already_processed(date))
        needs_processing = len(dates_to_process) - already_processed
        
        setup_time = time.time() - start_time
        
        # Return configuration for downstream tasks
        config = {
            "dates_to_process": dates_to_process,
            "total_dates": len(dates_to_process),
            "already_processed": already_processed,
            "needs_processing": needs_processing,
            "keyword_count": len(keywords),
            "azure_account": storage_account,
            "setup_time": setup_time
        }
        
        print("\n📊 SETUP SUMMARY:")
        print(f"   📅 Total dates: {len(dates_to_process)}")
        print(f"   ✅ Already processed: {already_processed}")
        print(f"   🔄 Needs processing: {needs_processing}")
        print(f"   ⚡ Processing mode: One date at a time")
        print(f"   ⏱️  Setup time: {setup_time:.1f} seconds")
        
        return config
        
    except Exception as e:
        print(f"❌ Setup failed: {e}")
        raise

def process_dates_one_by_one(**context) -> Dict:
    """Task 2: Process dates one by one through complete pipeline"""
    print("\n⚙️  TASK 2: Sequential One-by-One Processing")
    print("=" * 50)
    
    overall_start_time = time.time()
    
    # Get configuration from previous task
    ti = context['ti']
    config = ti.xcom_pull(task_ids='setup_and_validate')
    dates_to_process = config['dates_to_process']
    
    # Load keywords for processing
    keywords = load_keywords()
    
    processing_stats = {
        "total_dates": len(dates_to_process),
        "successfully_processed": 0,
        "already_processed": 0,
        "failed_processing": 0,
        "successful_uploads": 0,
        "failed_uploads": 0,
        "date_details": []
    }
    
    print(f"🔄 Processing {len(dates_to_process)} dates ONE BY ONE...")
    print(f"🔑 Using {len(keywords)} keywords")
    print(f"📋 Pipeline per date: download → process → upload")
    
    for date_idx, date_str in enumerate(dates_to_process):
        print(f"\n{'='*80}")
        print(f"📅 DATE {date_idx + 1}/{len(dates_to_process)}: {date_str}")
        print(f"{'='*80}")
        
        date_start_time = time.time()
        
        date_result = {
            "date": date_str,
            "processing_success": False,
            "upload_success": False,
            "already_processed": False,
            "processing_time": 0,
            "upload_time": 0
        }
        
        try:
            # STEP 1: Check if already processed
            if is_date_already_processed(date_str):
                print(f"✅ {date_str} already processed locally")
                processing_stats["already_processed"] += 1
                date_result["already_processed"] = True
                date_result["processing_success"] = True
            else:
                # STEP 2: Process this date (includes download + trend extraction)
                print(f"🔄 STEP 1: Download + Process {date_str}...")
                processing_start = time.time()
                
                success = process_single_date(date_str, keywords)
                
                date_result["processing_time"] = time.time() - processing_start
                date_result["processing_success"] = success
                
                if success:
                    processing_stats["successfully_processed"] += 1
                    print(f"   ✅ Processing completed in {date_result['processing_time']/60:.1f} minutes")
                else:
                    processing_stats["failed_processing"] += 1
                    print(f"   ❌ Processing failed")
                    date_result["error"] = "Processing failed"
            
            # STEP 3: Upload to Azure (if processing succeeded or already processed)
            if date_result["processing_success"]:
                print(f"🔄 STEP 2: Upload {date_str} to Azure...")
                upload_start = time.time()
                
                try:
                    upload_result = upload_date_to_azure_silver(date_str)
                    date_result["upload_time"] = time.time() - upload_start
                    
                    if upload_result.get("already_uploaded"):
                        print(f"   ⏩ Already in Azure")
                        date_result["upload_success"] = True
                        processing_stats["successful_uploads"] += 1
                    elif upload_result.get("total_successful") == upload_result.get("total_files"):
                        print(f"   ✅ Upload completed in {date_result['upload_time']:.1f} seconds")
                        date_result["upload_success"] = True
                        processing_stats["successful_uploads"] += 1
                    else:
                        print(f"   ⚠️  Partial upload: {upload_result.get('total_successful')}/{upload_result.get('total_files')} files")
                        processing_stats["failed_uploads"] += 1
                        date_result["upload_error"] = "Partial upload"
                    
                    date_result["upload_details"] = upload_result
                    
                except Exception as upload_error:
                    processing_stats["failed_uploads"] += 1
                    date_result["upload_time"] = time.time() - upload_start
                    date_result["upload_error"] = str(upload_error)
                    print(f"   ❌ Upload failed: {upload_error}")
            else:
                print(f"   ⏩ Skipping upload (processing failed)")
            
            # STEP 4: Progress reporting
            total_date_time = time.time() - date_start_time
            date_result["total_time"] = total_date_time
            
            if date_result["processing_success"] and date_result["upload_success"]:
                status = "✅ COMPLETE"
            elif date_result["processing_success"]:
                status = "⚠️  PROCESSED (upload issue)"
            else:
                status = "❌ FAILED"
            
            print(f"\n📊 {status} {date_str}: Total time {total_date_time/60:.1f} minutes")
            
            # ETA calculation
            remaining_dates = len(dates_to_process) - (date_idx + 1)
            if remaining_dates > 0:
                elapsed_time = time.time() - overall_start_time
                avg_date_time = elapsed_time / (date_idx + 1)
                eta_minutes = (remaining_dates * avg_date_time) / 60
                print(f"📈 Progress: {date_idx + 1}/{len(dates_to_process)} complete. ETA: {eta_minutes:.1f} minutes")
                
        except Exception as e:
            processing_stats["failed_processing"] += 1
            print(f"❌ {date_str} unexpected error: {e}")
            
            date_result.update({
                "total_time": time.time() - date_start_time,
                "error": str(e)
            })
        
        processing_stats["date_details"].append(date_result)
    
    total_processing_time = time.time() - overall_start_time
    processing_stats["total_processing_time"] = total_processing_time
    
    print("\n🏁 SEQUENTIAL PROCESSING COMPLETE")
    print("=" * 50)
    print(f"   ✅ Successfully processed: {processing_stats['successfully_processed']}")
    print(f"   ♻️  Already processed: {processing_stats['already_processed']}")
    print(f"   ❌ Failed processing: {processing_stats['failed_processing']}")
    print(f"   ☁️  Successful uploads: {processing_stats['successful_uploads']}")
    print(f"   ❌ Failed uploads: {processing_stats['failed_uploads']}")
    print(f"   ⏱️  Total time: {total_processing_time/60:.1f} minutes")
    
    return processing_stats

def generate_execution_summary(**context) -> Dict:
    """Task 3: Generate comprehensive execution summary"""
    print("\n🏁 TASK 3: Execution Summary")
    print("=" * 50)
    
    ti = context['ti']
    
    # Gather results from all tasks
    setup_result = ti.xcom_pull(task_ids='setup_and_validate')
    processing_result = ti.xcom_pull(task_ids='process_dates_one_by_one')
    
    summary = {
        "execution_info": {
            "total_dates": setup_result["total_dates"],
            "date_range": f"{setup_result['dates_to_process'][0]} to {setup_result['dates_to_process'][-1]}",
            "processing_mode": "true_sequential_one_by_one"
        },
        "setup_stats": setup_result,
        "processing_stats": processing_result,
        "overall_success": (
            processing_result["failed_processing"] == 0 and 
            processing_result["failed_uploads"] == 0
        ),
        "total_execution_time": (
            setup_result["setup_time"] + 
            processing_result["total_processing_time"]
        )
    }
    
    print("🎯 OVERALL EXECUTION SUMMARY:")
    print(f"   📅 Date range: {summary['execution_info']['date_range']}")
    print(f"   📊 Total dates: {summary['execution_info']['total_dates']}")
    print(f"   ⚡ Processing mode: {summary['execution_info']['processing_mode']}")
    print(f"   ⏱️  Total time: {summary['total_execution_time']/60:.1f} minutes")
    print(f"   ✅ Overall success: {summary['overall_success']}")
    
    return summary

# DAG Definition
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 1, 1, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="github_trends_extractor_daily",
    default_args=default_args,
    description=f"Extract tech trends one date at a time starting {DEFAULT_START_DATE}",
    schedule=None,  # Manual trigger only
    catchup=False,
    tags=["github", "trends", "daily", "azure", "one-by-one"],
    doc_md=f"""
    # GitHub Trends Extractor (Daily Sequential Processing)
    
    Processes one date at a time through the complete pipeline:
    **Date 1**: download → process → upload → **Date 2**: download → process → upload → etc.
    
    ## Configuration
    - **Start Date**: {DEFAULT_START_DATE}
    - **Processing**: One date at a time, sequential
    - **Proven Performance**: ~35 minutes per date
    
    ## Trigger Options
    
    ### Default (no parameters)
    ```json
    {{}}
    ```
    
    ### Single Date
    ```json
    {{"date": "2025-02-24"}}
    ```
    
    ### Date Range
    ```json
    {{"start_date": "2025-02-24", "end_date": "2025-02-28"}}
    ```
    """,
) as dag:

    # Task 1: Setup and validation
    setup_task = PythonOperator(
        task_id="setup_and_validate",
        python_callable=setup_and_validate,
        execution_timeout=timedelta(minutes=5),
        op_kwargs={"default_start_date": DEFAULT_START_DATE},
    )

    # Task 2: Process all dates sequentially (one complete pipeline per date)
    process_sequential_task = PythonOperator(
        task_id="process_dates_one_by_one",
        python_callable=process_dates_one_by_one,
        execution_timeout=timedelta(hours=12),  # Allow for multiple dates
    )

    # Task 3: Generate summary
    summary_task = PythonOperator(
        task_id="generate_execution_summary",
        python_callable=generate_execution_summary,
        execution_timeout=timedelta(minutes=2),
    )

    # Define task dependencies
    setup_task >> process_sequential_task >> summary_task