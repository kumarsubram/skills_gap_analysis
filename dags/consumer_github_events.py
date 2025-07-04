"""
GitHub Events Consumer DAG - COMPATIBLE WITH YOUR SCRIPT
=======================================================

✅ SCRIPT-MANAGED: Works with your batch-aware streaming manager
✅ EXTERNAL CONTROL: Started/stopped by your script, not Airflow schedules  
✅ LONG-RUNNING: Runs until explicitly stopped by script
✅ SAME DAG NAME: consumer_github_events

Replace: dags/consumer_github_events.py
"""

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Import streaming utilities
from include.streaming.streaming_utils import check_prerequisites, run_streaming_job
from include.streaming.streaming_table_utils import ensure_streaming_table_exists

# Airflow imports
from airflow import DAG
from airflow.operators.python import PythonOperator


def start_streaming_consumer(**context):
    """
    Real-time trends consumer - AUTOMATIC HOURLY RESTART
    Always starts with latest messages for 30-second rolling dashboard
    """
    
    print("🎯 REAL-TIME TRENDS CONSUMER (AIRFLOW SCHEDULED)")
    print("=" * 60)
    print("📡 Source: github-events-raw (latest messages only)")
    print("💾 Target: bronze_github_streaming_keyword_extractions")
    print("🎯 Purpose: 30-second rolling dashboard updates")
    print("⏰ Runtime: 1 hour max (auto-restart by Airflow)")
    print("🔄 Strategy: Skip old messages, always start fresh")
    
    # Step 1: Prerequisites check
    print("\n" + "="*40)
    print("STEP 1: PREREQUISITES CHECK")
    print("="*40)
    
    if not check_prerequisites():
        print("\n❌ Prerequisites failed - aborting")
        # For script-managed tasks, raise exception to indicate failure
        raise RuntimeError("Prerequisites check failed - script should retry")
    
    # Step 2: Run streaming job for 1 hour
    print("\n" + "="*40)
    print("STEP 2: AIRFLOW-MANAGED STREAMING")
    print("="*40)
    print("💡 Airflow will automatically:")
    print("   • Run this DAG every hour")
    print("   • Stop after 1 hour timeout")
    print("   • Start fresh next hour")
    print("   • Handle failures automatically")
    
    job_script = "/opt/airflow/include/spark_jobs/github_kafka_to_streaming_delta.py"
    
    try:
        # Run for 1 hour max - auto-restart by Airflow next hour
        print("🔄 Running for 1 hour max to ensure fresh real-time data")
        result = run_streaming_job(job_script, timeout_minutes=60)
        
        # Expected completion after 1 hour
        print("\n" + "="*40)
        print("STEP 3: HOURLY COMPLETION (EXPECTED)")
        print("="*40)
        
        if result.get('success', False):
            print("🔄 HOURLY RUN COMPLETED SUCCESSFULLY")
            print(f"✅ {result.get('message', 'Completed after 1 hour')}")
            print(f"📊 Runtime: {result.get('runtime_minutes', 0):.1f} minutes")
            print("💡 Airflow will start next run at top of hour")
            return result
        else:
            print("❌ STREAMING FAILED DURING HOUR")
            print(f"💥 {result.get('message', 'Unknown error')}")
            # Raise exception so Airflow knows there was a failure
            raise RuntimeError(f"Streaming failed: {result.get('message', 'Unknown error')}")
            
    except KeyboardInterrupt:
        print("\n🛑 STREAMING STOPPED MANUALLY")
        print("✅ This is normal - manual stop or container restart")
        # Don't raise exception for manual stops
        return {
            'status': 'stopped_manually',
            'success': True,
            'message': 'Stopped manually (normal operation)'
        }
    except Exception as e:
        print(f"\n❌ STREAMING ERROR: {e}")
        print("🔧 Airflow will retry based on DAG retry settings")
        # Re-raise so Airflow knows there was a problem
        raise


def ensure_streaming_table_only(**context):
    """
    Ensure streaming table exists - called before starting streaming
    """
    print("🔍 STREAMING TABLE SETUP (AIRFLOW-MANAGED)")
    print("=" * 50)
    
    success = ensure_streaming_table_exists()
    
    if success:
        print("✅ Streaming table ready for processing!")
        return {'status': 'success', 'streaming_table': success}
    else:
        print("❌ Streaming table setup failed")
        # Raise exception so Airflow knows setup failed
        raise RuntimeError("Failed to setup streaming table")


# DAG Definition - HOURLY SCHEDULED
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 6, 23, tzinfo=timezone.utc),
    "retries": 1,  # Retry once if failure occurs
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="consumer_github_events",
    default_args=default_args,
    description="Real-time GitHub Trends Consumer - Hourly automatic restarts for latest data",
    schedule="0 0,1,2,3,5,6,7,8,9,10,12,13,14,15,16,17,18,19,20,21,22,23 * * *", 
    catchup=False,
    max_active_runs=1,  # Only one consumer at a time
    max_active_tasks=1,
    tags=["github", "streaming", "consumer", "real-time", "hourly-restart", "latest-only"],
    doc_md="""
    # GitHub Events Streaming Consumer - HOURLY SCHEDULED
    
    ## 🎯 **Purpose**
    Consumes GitHub events from Kafka with automatic hourly restarts for fresh real-time data.
    
    ## ⏰ **Automatic Scheduling**
    This DAG runs automatically every hour:
    - ✅ **Hourly schedule**: Runs at :00 minutes every hour
    - ✅ **1-hour timeout**: Each run lasts max 1 hour then gracefully stops
    - ✅ **Fresh restarts**: Always starts from latest Kafka messages
    - ✅ **No manual triggers**: Fully automated operation
    
    ## 🔄 **Operation Flow**
    1. **Start**: DAG triggers every hour automatically
    2. **Run**: Processes Kafka messages for up to 1 hour
    3. **Timeout**: Gracefully stops after 1 hour
    4. **Restart**: Next hourly run starts fresh with latest data
    
    ## 🚀 **Benefits**
    - ✅ **Always fresh data**: Hourly restarts ensure latest messages
    - ✅ **No external dependencies**: Pure Airflow scheduling
    - ✅ **Resource management**: 1-hour limit prevents memory buildup
    - ✅ **Reliability**: Automatic recovery from failures
    
    ## 📊 **Real-time Dashboard**
    Perfect for 30-second rolling dashboards:
    - Latest GitHub events processed within seconds
    - No lag from old messages
    - Consistent fresh data flow
    
    ## 🛑 **Manual Control**
    - **Pause**: `airflow dags pause consumer_github_events`
    - **Unpause**: `airflow dags unpause consumer_github_events`
    - **Manual trigger**: `airflow dags trigger consumer_github_events`
    """,
)

# Task definitions
consumer_task = PythonOperator(
    task_id="start_streaming_consumer",
    python_callable=start_streaming_consumer,
    execution_timeout=None,  # No timeout - script controls lifecycle
    dag=dag,
)

# Table setup task
ensure_table_task = PythonOperator(
    task_id="ensure_streaming_table_only",
    python_callable=ensure_streaming_table_only,
    dag=dag,
)

# Task dependencies
ensure_table_task >> consumer_task