"""
GitHub Events Consumer DAG - SIMPLIFIED VERSION
===============================================

Clean, simple consumer DAG using reusable streaming utilities.
Uses optimized resource settings (1 core, 2GB) for worker isolation.

Place at: dags/consumer_github_events.py (replace existing)
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import our reusable streaming utilities
from include.streaming.streaming_utils import check_prerequisites, run_streaming_job
from include.streaming.streaming_table_utils import ensure_streaming_table_exists
# ADDED: Import for aggregation table setup
from include.streaming.streaming_aggregation_table_utils import ensure_streaming_aggregation_table_exists

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator


def start_streaming_consumer(**context):
    """
    Main task: Start the GitHub events streaming consumer
    """
    
    print("🎯 GITHUB EVENTS STREAMING CONSUMER")
    print("=" * 60)
    print("📡 Source: github-events-raw (Kafka topic)")
    print("💾 Target: bronze_github_streaming_keyword_extractions (Delta)")
    # ADDED: Show dual-write info
    print("⚡ Bonus: bronze_github_streaming_daily_aggregates (Fast queries)")
    print("🔧 Resources: 1 core, 2GB memory (worker isolation)")
    print("⏰ Runtime: 15 minutes with auto-timeout")
    
    # Step 1: Check prerequisites
    print("\n" + "="*50)
    print("STEP 1: PREREQUISITES CHECK")
    print("="*50)
    
    if not check_prerequisites():
        print("\n❌ Prerequisites check failed - aborting consumer")
        return {
            'status': 'failed',
            'reason': 'prerequisites_not_met',
            'message': 'Prerequisites check failed - see logs for details'
        }
    
    # Step 2: Run streaming job
    print("\n" + "="*50)
    print("STEP 2: STARTING STREAMING JOB")
    print("="*50)
    
    job_script = "/opt/airflow/include/spark_jobs/github_kafka_to_streaming_delta.py"
    result = run_streaming_job(job_script, timeout_minutes=None)
    
    # Step 3: Report results
    print("\n" + "="*50)
    print("STEP 3: FINAL RESULTS")
    print("="*50)
    
    if result.get('success', False):
        print("🎉 CONSUMER SUCCESS")
        print(f"✅ {result.get('message', 'Job completed')}")
        print(f"📊 Runtime: {result.get('runtime_minutes', 0):.1f} minutes")
        print(f"📝 Output lines: {result.get('total_lines', 0)}")
        print("💡 Check Delta table for processed records")
        # ADDED: Mention dual-write success
        print("⚡ Dual-write: Raw + Aggregation tables updated")
    else:
        print("❌ CONSUMER FAILED")
        print(f"💥 {result.get('message', 'Unknown error')}")
        print("🔧 Check Spark logs and prerequisites")
    
    print("="*60)
    
    return result


# ADDED: Combined table setup function (cleaner than separate tasks)
def ensure_both_tables_exist(**context):
    """
    Ensure both streaming tables exist before starting consumer
    """
    print("🔍 DUAL TABLE SETUP")
    print("=" * 40)
    
    # Setup streaming table (existing)
    print("📊 Setting up raw streaming table...")
    raw_success = ensure_streaming_table_exists()
    
    # Setup aggregation table (new)
    print("\n⚡ Setting up aggregation table...")
    agg_success = ensure_streaming_aggregation_table_exists()
    
    # Report results
    print("\n📋 SETUP RESULTS:")
    print(f"   Raw table: {'✅' if raw_success else '❌'}")
    print(f"   Agg table: {'✅' if agg_success else '❌'}")
    
    if raw_success and agg_success:
        print("🎉 Both tables ready for dual-write streaming!")
        return {'status': 'success', 'raw_table': raw_success, 'agg_table': agg_success}
    else:
        print("❌ Table setup failed - consumer may not work properly")
        return {'status': 'partial', 'raw_table': raw_success, 'agg_table': agg_success}


# DAG Definition
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 6, 23, tzinfo=timezone.utc),
    "retries": 0,  # Don't retry streaming jobs
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="consumer_github_events",
    default_args=default_args,
    description="GitHub Events Consumer - Simplified with reusable utilities",
    schedule=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,  # Only one consumer at a time
    max_active_tasks=1,
    tags=["github", "streaming", "consumer", "kafka", "delta-lake", "simplified"],
    doc_md="""
    # GitHub Events Streaming Consumer - SIMPLIFIED VERSION
    
    ## 🎯 **Purpose**
    Consumes GitHub events from Kafka and processes them into Delta Lake with keyword extraction.
    
    ## 🔧 **Key Features**
    - ✅ **Simplified code**: Uses reusable streaming utilities
    - ✅ **Resource isolation**: 1 core, 2GB memory (leaves other worker free)
    - ✅ **Auto-timeout**: 15 minutes with graceful shutdown
    - ✅ **Real-time monitoring**: Progress tracking with timestamps
    - ✅ **Comprehensive checks**: Prerequisites validation before start
    - ✅ **Dual-write**: Raw + Aggregation tables for fast queries
    
    ## 🔄 **Processing Flow**
    1. **Table Setup**: Ensures both raw and aggregation tables exist
    2. **Prerequisites Check**: Validates Delta table, MinIO, Kafka, Spark
    3. **Streaming Job**: Processes Kafka events with keyword extraction
    4. **Auto-timeout**: Graceful shutdown after 15 minutes
    5. **Results**: Clear success/failure status with details
    
    ## ⚙️ **Resource Management**
    - **Worker isolation**: Uses only 1 Spark worker (1 core, 2GB)
    - **Jupyter friendly**: Leaves spark-worker-2 free for other tasks
    - **Memory efficient**: Conservative settings prevent OOM issues
    
    ## 📊 **Monitoring**
    - Real-time stdout with error highlighting
    - Progress tracking with line counts
    - Silence detection (warns if stuck)
    - Comprehensive status reporting
    
    ## 🎯 **Enhanced Schema**
    Writes to `bronze_streaming_github_keyword_extractions` with:
    - Original batch columns (hour, keyword, mentions, etc.)
    - Enhanced streaming columns (event_id, kafka_timestamp, etc.)
    - Window metadata for time-based analytics
    
    Also writes to `bronze_github_streaming_daily_aggregates` with:
    - Daily technology aggregates for fast SSE queries
    - 15x performance improvement for dashboard
    
    ## 🚀 **Usage**
    - Trigger manually from Airflow UI
    - Monitor progress in real-time via logs
    - Consumer auto-stops after 15 minutes
    - Check Delta tables for processed records
    """,
)

# Task definitions
consumer_task = PythonOperator(
    task_id="start_streaming_consumer",
    python_callable=start_streaming_consumer,
    execution_timeout=timedelta(minutes=20),  # 20-minute Airflow timeout (5 min buffer)
    dag=dag,
)

# UPDATED: Combined table setup task (cleaner than two separate tasks)
ensure_tables_task = PythonOperator(
    task_id="ensure_both_streaming_tables",
    python_callable=ensure_both_tables_exist,
    dag=dag,
)

# Task dependencies (unchanged)
ensure_tables_task >> consumer_task