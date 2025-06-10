"""
Daily Jobs Collector DAG

Truly idempotent pipeline:
1. Bronze: Raw API responses (NEVER deleted)
2. Silver: Parsed job data (regenerated from Bronze)

Key: If Silver exists for date, skip entire DAG unless forced.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone

from include.delta_lake.file_manager import file_exists, ensure_dirs
from include.jobs.job_common_utils import save_raw_api_to_bronze
from include.jobs.jobs_bronze_to_silver import run_bronze_to_silver_pipeline

# Source schedules
SOURCE_SCHEDULES = {
    'hackernews': 'daily',
    'greenhouse': 'daily',
    'remoteok': 'daily', 
}

def should_source_run_today(source_name: str, schedule: str) -> bool:
    """Check if source should run today"""
    today = datetime.now(timezone.utc)
    
    if schedule == 'daily':
        return True
    elif schedule == 'weekly':
        return today.weekday() == 0  # Monday
    else:
        return False

def get_sources_for_today() -> list:
    """Get sources scheduled for today"""
    sources = []
    for source_name, schedule in SOURCE_SCHEDULES.items():
        if should_source_run_today(source_name, schedule):
            sources.append(source_name)
    return sources

def collect_source_data(source_name: str, date_str: str) -> list:
    """Collect raw API data from a source"""
    try:
        module_path = f"include.jobs.{source_name}_collector"
        collector_module = __import__(module_path, fromlist=[f"{source_name}_collector"])
        collect_function = getattr(collector_module, f"collect_{source_name}_jobs")
        return collect_function(date_str)
    except Exception as e:
        print(f"   ❌ Error collecting {source_name}: {e}")
        return []

def bronze_exists(source_name: str, date_str: str) -> bool:
    """Check if Bronze data exists"""
    filename = f"raw_api_{source_name}_{date_str}.parquet"
    return file_exists(filename, 'jobs', 'bronze')

def silver_exists(date_str: str) -> bool:
    """Check if ALL Silver files exist for complete idempotency"""
    required_files = [
        ('job_details', f"job_details_{date_str}.parquet"),
        ('skills_demand', f"skills_demand_{date_str}.parquet"), 
        ('source_summary', f"source_summary_{date_str}.parquet")
    ]
    
    for subfolder, filename in required_files:
        if not file_exists(filename, 'jobs', f'silver/{subfolder}'):
            print(f"   ⚠️  Missing Silver file: {filename}")
            return False
    
    print(f"   ✅ All Silver files exist for {date_str}")
    return True

def run_jobs_pipeline(**context):
    """Main pipeline with true idempotency"""
    print("🚀 JOBS COLLECTION PIPELINE")
    print("=" * 50)
    
    # Setup
    ensure_dirs('jobs')
    dag_run = context.get("dag_run", {})
    conf = dag_run.conf or {}
    
    # Get date
    if "date" in conf:
        date_str = conf["date"]
        print(f"📅 Manual date: {date_str}")
    else:
        date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        print(f"📅 Today: {date_str}")
    
    # Get flags
    force_recollection = conf.get("force_recollection", False)
    force_reprocessing = conf.get("force_reprocessing", False) 
    parse_only = conf.get("parse_only", False)
    
    # IDEMPOTENCY CHECK: Silver first
    print(f"\n🔍 CHECKING IDEMPOTENCY FOR {date_str}")
    
    if silver_exists(date_str) and not force_reprocessing and not force_recollection:
        print("✅ Silver already exists - SKIPPING ENTIRE DAG")
        print("💡 Use force flags to override:")
        print('   {"force_reprocessing": true} - Regenerate Silver')
        print('   {"force_recollection": true} - Recollect everything')
        
        return {
            "status": "skipped_idempotent",
            "date": date_str,
            "message": "Silver exists - no work needed"
        }
    
    # Determine what to do
    if parse_only:
        print("🔄 PARSE-ONLY MODE: Skip API collection")
    elif force_recollection:
        print("🔄 FORCE RECOLLECTION: Will overwrite Bronze")
    elif force_reprocessing:
        print("🔄 FORCE REPROCESSING: Will regenerate Silver")
    else:
        print("📊 NORMAL MODE: Collect missing data")
    
    # STEP 1: Bronze Collection
    bronze_results = {}
    
    if not parse_only:
        print("\n📦 BRONZE COLLECTION")
        print("-" * 30)
        
        # Get sources to run
        if conf.get("force_all_sources", False):
            sources_to_run = list(SOURCE_SCHEDULES.keys())
            print("🔧 Running ALL sources (manual override)")
        else:
            sources_to_run = get_sources_for_today()
            print(f"📅 Scheduled sources: {sources_to_run}")
        
        # Process each source
        for source_name in sources_to_run:
            print(f"\n📡 {source_name}:")
            
            # Check if Bronze exists
            if bronze_exists(source_name, date_str) and not force_recollection:
                print("   📦 Bronze exists - PRESERVING (no collection)")
                bronze_results[source_name] = "preserved"
                continue
            
            # Collect data
            print("   🔄 Collecting raw API data...")
            raw_data = collect_source_data(source_name, date_str)
            
            if raw_data:
                success = save_raw_api_to_bronze(raw_data, source_name, date_str)
                if success:
                    print(f"   ✅ Saved {len(raw_data)} responses to Bronze")
                    bronze_results[source_name] = f"collected_{len(raw_data)}"
                else:
                    print("   ❌ Failed to save to Bronze")
                    bronze_results[source_name] = "failed"
            else:
                print("   📭 No data collected")
                bronze_results[source_name] = "no_data"
        
        # Bronze summary
        print("\n📊 Bronze Summary:")
        for source, result in bronze_results.items():
            if result == "preserved":
                print(f"   📦 {source}: Preserved existing data")
            elif result.startswith("collected_"):
                count = result.split("_")[1]
                print(f"   ✅ {source}: Collected {count} responses")
            else:
                print(f"   ❌ {source}: {result}")
        
        print("   🏛️  Bronze files: NEVER DELETED - preserved forever")
    
    # STEP 2: Silver Processing
    print("\n✨ SILVER PROCESSING")
    print("-" * 30)
    
    try:
        result = run_bronze_to_silver_pipeline(date_str)
        
        if result['success']:
            print("🎉 SUCCESS: Pipeline completed!")
            
            return {
                "status": "success",
                "date": date_str,
                "bronze_results": bronze_results,
                "total_jobs": result['total_jobs'],
                "total_skills": result['total_skills'],
                "unique_skills": result['unique_skills'],
                "files_saved": result['files_saved']
            }
        else:
            print(f"❌ Silver processing failed: {result.get('reason')}")
            return {
                "status": "silver_failed", 
                "date": date_str,
                "reason": result.get('reason'),
                "bronze_results": bronze_results
            }
            
    except Exception as e:
        print(f"💥 Pipeline error: {e}")
        return {
            "status": "error",
            "date": date_str, 
            "error": str(e),
            "bronze_results": bronze_results
        }

# DAG Definition
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 6, 9, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="daily_jobs_collector", 
    default_args=default_args,
    description="Operational jobs collection: Bronze → Silver",
    schedule="0 18 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["jobs", "operational", "guaranteed"],
) as dag:

    process_jobs = PythonOperator(
        task_id="process_jobs",
        python_callable=run_jobs_pipeline,
        pool='operational',        # 🎯 Gets 32 guaranteed slots
        priority_weight=10,        # HIGH priority (runs immediately)
        execution_timeout=timedelta(hours=2),
    )