"""
Daily Jobs Bronze DAG

Clean Bronze Delta pipeline:
1. Collects raw API data from multiple sources
2. Saves directly to Bronze Delta tables using deltalake
3. Truly idempotent - skips if Bronze already exists

Sources: Greenhouse, HackerNews, RemoteOK
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta, timezone

# Add project root to Python path before imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator

# Project imports - after path setup
from include.jobs.jobs_bronze_utils import (
    bronze_delta_exists,
    save_raw_api_to_bronze_delta,
    get_bronze_summary,
    ensure_jobs_directories
)
from include.jobs.greenhouse_collector import collect_greenhouse_jobs
from include.jobs.hackernews_collector import collect_hackernews_jobs
from include.jobs.remoteok_collector import collect_remoteok_jobs

# Source configuration
JOB_SOURCES = {
    'greenhouse': {
        'collector': collect_greenhouse_jobs,
        'schedule': 'daily',
        'description': 'Tech company job boards via Greenhouse API'
    },
    'hackernews': {
        'collector': collect_hackernews_jobs,
        'schedule': 'daily', 
        'description': 'HackerNews Who\'s Hiring monthly threads'
    },
    'remoteok': {
        'collector': collect_remoteok_jobs,
        'schedule': 'daily',
        'description': 'Remote job listings from RemoteOK API'
    }
}


def should_source_run_today(source_name: str, schedule: str) -> bool:
    """Check if source should run today based on schedule"""
    today = datetime.now(timezone.utc)
    
    if schedule == 'daily':
        return True
    elif schedule == 'weekly':
        return today.weekday() == 0  # Monday
    elif schedule == 'monthly':
        return today.day == 1  # First day of month
    else:
        return False


def get_sources_for_today() -> list:
    """Get sources scheduled to run today"""
    sources = []
    for source_name, config in JOB_SOURCES.items():
        if should_source_run_today(source_name, config['schedule']):
            sources.append(source_name)
    return sources


def collect_source_data(source_name: str, date_str: str) -> list:
    """Collect raw API data from a specific source"""
    if source_name not in JOB_SOURCES:
        print(f"   ❌ Unknown source: {source_name}")
        return []
    
    try:
        collector_func = JOB_SOURCES[source_name]['collector']
        return collector_func(date_str)
    except Exception as e:
        print(f"   ❌ Error collecting {source_name}: {e}")
        return []


def run_jobs_bronze_pipeline(**context):
    """Main Bronze Delta pipeline"""
    print("🚀 JOBS BRONZE DELTA PIPELINE")
    print("=" * 50)
    
    # Setup
    ensure_jobs_directories()
    
    # Get configuration from DAG run
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
    dry_run = conf.get("dry_run", False)
    limit_sources = conf.get("sources", None)
    
    print("🔧 Configuration:")
    print(f"   Force recollection: {force_recollection}")
    print(f"   Dry run: {dry_run}")
    print(f"   Source filter: {limit_sources or 'All scheduled'}")
    
    # Determine sources to run
    if limit_sources:
        if isinstance(limit_sources, str):
            sources_to_run = [limit_sources]
        else:
            sources_to_run = limit_sources
        print(f"🔧 Manual source selection: {sources_to_run}")
    else:
        sources_to_run = get_sources_for_today()
        print(f"📅 Scheduled sources: {sources_to_run}")
    
    # IDEMPOTENCY CHECK
    print(f"\n🔍 CHECKING BRONZE DELTA TABLES FOR {date_str}")
    sources_status = {}
    
    for source_name in sources_to_run:
        summary = get_bronze_summary(source_name, date_str)
        sources_status[source_name] = summary
        
        if summary['status'] == 'bronze_complete':
            print(f"   ✅ {source_name}: Bronze Delta complete ({summary.get('record_count', 0)} records)")
        else:
            print(f"   ❌ {source_name}: Missing Bronze Delta data")
    
    # Check if we can skip everything
    if not force_recollection:
        all_complete = all(status['status'] == 'bronze_complete' for status in sources_status.values())
        
        if all_complete:
            print("✅ All sources have Bronze Delta data - SKIPPING COLLECTION")
            print("💡 Use force_recollection=true to override")
            
            return {
                "status": "skipped_idempotent",
                "date": date_str,
                "sources_status": sources_status,
                "message": "All Bronze Delta data exists"
            }
    
    # BRONZE DELTA COLLECTION
    print("\n📦 BRONZE DELTA COLLECTION")
    print("-" * 30)
    
    collection_results = {}
    
    for source_name in sources_to_run:
        print(f"\n📡 {source_name.upper()}: {JOB_SOURCES[source_name]['description']}")
        
        # Check if Bronze Delta exists and not forcing
        if bronze_delta_exists(source_name, date_str) and not force_recollection:
            print("   📦 Bronze Delta exists - PRESERVING (no collection)")
            collection_results[source_name] = "preserved"
            continue
        
        if dry_run:
            print("   🧪 DRY RUN - Would collect data")
            collection_results[source_name] = "dry_run"
            continue
        
        # Collect raw data
        print("   🔄 Collecting raw API data...")
        try:
            raw_data = collect_source_data(source_name, date_str)
            
            if raw_data:
                # Save directly to Bronze Delta
                success = save_raw_api_to_bronze_delta(raw_data, source_name, date_str)
                
                if success:
                    print(f"   ✅ Saved {len(raw_data)} responses to Bronze Delta")
                    collection_results[source_name] = f"collected_{len(raw_data)}"
                else:
                    print("   ❌ Failed to save to Bronze Delta")
                    collection_results[source_name] = "save_failed"
            else:
                print("   📭 No data collected")
                collection_results[source_name] = "no_data"
                
        except Exception as e:
            print(f"   ❌ Collection error: {e}")
            collection_results[source_name] = f"error_{str(e)[:50]}"
    
    # FINAL SUMMARY
    print("\n📊 BRONZE DELTA SUMMARY")
    print("-" * 30)
    
    total_sources = len(sources_to_run)
    successful_collections = 0
    preserved_sources = 0
    
    for source, result in collection_results.items():
        if result == "preserved":
            print(f"   📦 {source}: Preserved existing Bronze Delta")
            preserved_sources += 1
        elif result.startswith("collected_"):
            count = result.split("_")[1]
            print(f"   ✅ {source}: Collected {count} responses → Bronze Delta")
            successful_collections += 1
        elif result == "dry_run":
            print(f"   🧪 {source}: Dry run completed")
        else:
            print(f"   ❌ {source}: {result}")
    
    print("\n🎯 FINAL RESULTS:")
    print(f"   📈 New collections: {successful_collections}/{total_sources}")
    print(f"   📦 Preserved: {preserved_sources}/{total_sources}")
    print("   🏛️  Bronze Delta tables: Clean medallion architecture")
    
    return {
        "status": "success",
        "date": date_str,
        "total_sources": total_sources,
        "successful_collections": successful_collections,
        "preserved_sources": preserved_sources,
        "collection_results": collection_results,
        "sources_status": sources_status
    }


# DAG Definition
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 6, 20, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="daily_jobs_bronze",
    default_args=default_args,
    description="Jobs Bronze Delta Collection: Direct Delta Lake storage",
    schedule="0 19 * * *",  # Daily at 3 PM
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["jobs", "bronze", "delta-lake", "clean"],
)

# Single task
bronze_collection_task = PythonOperator(
    task_id="collect_jobs_bronze_delta",
    python_callable=run_jobs_bronze_pipeline,
    execution_timeout=timedelta(minutes=30),
    dag=dag,
)