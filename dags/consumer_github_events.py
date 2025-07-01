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
    Streaming consumer - MANAGED BY EXTERNAL SCRIPT
    Runs until stopped by your batch-aware streaming manager
    """
    
    print("🎯 SCRIPT-MANAGED GITHUB EVENTS CONSUMER")
    print("=" * 60)
    print("📡 Source: github-events-raw (Kafka topic)")
    print("💾 Target: bronze_github_streaming_keyword_extractions")
    print("🤖 Control: Managed by batch-aware streaming script")
    print("⏰ Runtime: Until stopped by external script")
    print("🛑 Stop method: Script calls 'airflow tasks clear'")
    
    # Step 1: Prerequisites check
    print("\n" + "="*40)
    print("STEP 1: PREREQUISITES CHECK")
    print("="*40)
    
    if not check_prerequisites():
        print("\n❌ Prerequisites failed - aborting")
        # For script-managed tasks, raise exception to indicate failure
        raise RuntimeError("Prerequisites check failed - script should retry")
    
    # Step 2: Run streaming job until externally stopped
    print("\n" + "="*40)
    print("STEP 2: SCRIPT-MANAGED STREAMING")
    print("="*40)
    print("💡 Your script will stop this when needed for:")
    print("   • Batch processing preparation")
    print("   • High disk usage cleanup")
    print("   • Container restarts")
    print("   • Scheduled maintenance")
    
    job_script = "/opt/airflow/include/spark_jobs/github_kafka_to_streaming_delta.py"
    
    try:
        # Run indefinitely - script will kill this task when needed
        result = run_streaming_job(job_script, timeout_minutes=None)
        
        # This should rarely be reached - only if streaming stops naturally
        print("\n" + "="*40)
        print("STEP 3: NATURAL COMPLETION (UNEXPECTED)")
        print("="*40)
        
        if result.get('success', False):
            print("🎉 STREAMING COMPLETED NATURALLY")
            print(f"✅ {result.get('message', 'Completed')}")
            print(f"📊 Runtime: {result.get('runtime_minutes', 0):.1f} minutes")
            print("💡 Script should restart this automatically")
            return result
        else:
            print("❌ STREAMING FAILED")
            print(f"💥 {result.get('message', 'Unknown error')}")
            # Raise exception so script knows to restart/investigate
            raise RuntimeError(f"Streaming failed: {result.get('message', 'Unknown error')}")
            
    except KeyboardInterrupt:
        print("\n🛑 STREAMING STOPPED BY SCRIPT")
        print("✅ This is normal - script-managed shutdown")
        # Don't raise exception for script-managed stops
        return {
            'status': 'stopped_by_script',
            'success': True,
            'message': 'Stopped by external script (normal operation)'
        }
    except Exception as e:
        print(f"\n❌ STREAMING ERROR: {e}")
        print("🔧 Script should investigate and potentially restart")
        # Re-raise so script knows there was a problem
        raise


def ensure_streaming_table_only(**context):
    """
    Ensure streaming table exists - called by script before starting streaming
    """
    print("🔍 STREAMING TABLE SETUP (SCRIPT-MANAGED)")
    print("=" * 50)
    
    success = ensure_streaming_table_exists()
    
    if success:
        print("✅ Streaming table ready for script-managed processing!")
        return {'status': 'success', 'streaming_table': success}
    else:
        print("❌ Streaming table setup failed")
        # Raise exception so script knows setup failed
        raise RuntimeError("Failed to setup streaming table")


# DAG Definition - MANUAL TRIGGER ONLY (Script Controlled)
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 6, 23, tzinfo=timezone.utc),
    "retries": 0,  # No retries - let script handle restarts
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}

dag = DAG(
    dag_id="consumer_github_events",  # SAME DAG NAME
    default_args=default_args,
    description="GitHub Events Consumer - Managed by external batch-aware script",
    schedule=None,  # NO SCHEDULE - Script controls when this runs
    catchup=False,
    max_active_runs=1,  # Only one consumer at a time
    max_active_tasks=1,
    tags=["github", "streaming", "consumer", "script-managed", "external-control"],
    doc_md="""
    # GitHub Events Streaming Consumer - SCRIPT-MANAGED
    
    ## 🎯 **Purpose**
    Consumes GitHub events from Kafka - controlled by external batch-aware streaming manager script.
    
    ## 🤖 **External Script Control**
    This DAG is managed by your `batch_streaming_manager.sh` script:
    - ✅ **Script starts**: `airflow dags trigger consumer_github_events`
    - ✅ **Script stops**: `airflow tasks clear consumer_github_events --only-running`
    - ✅ **Script monitors**: Checks if DAG is running with `airflow dags list-runs`
    - ✅ **Script coordinates**: Stops before batch, restarts after recovery
    
    ## 🔄 **Operation Phases**
    Your script manages these phases:
    1. **Streaming Mode**: This DAG runs continuously
    2. **Prep Phase**: Script stops this DAG 30min before batch
    3. **Batch Phase**: This DAG is stopped, batch jobs run
    4. **Recovery Phase**: Script restarts containers and this DAG
    
    ## 🚀 **Integration Benefits**
    - ✅ **Resource coordination**: Script ensures batch jobs get full resources
    - ✅ **Disk management**: Script stops streaming when disk usage high
    - ✅ **Clean restarts**: Script restarts containers between phases
    - ✅ **Monitoring**: Script tracks streaming status and health
    
    ## 📊 **Script Configuration**
    Your script controls:
    - **Batch timing**: When to stop/start streaming
    - **Disk thresholds**: When to stop for cleanup
    - **Restart logic**: How to recover from failures
    - **Health monitoring**: Continuous status checks
    
    ## 🛑 **Manual Control**
    While script is running:
    - **Monitor script**: `./batch_streaming_manager.sh status`
    - **Emergency stop**: `./batch_streaming_manager.sh stop`
    - **Manual cleanup**: `./batch_streaming_manager.sh cleanup`
    - **Restart streaming**: `./batch_streaming_manager.sh start`
    
    ## 💡 **Why This Approach Works**
    - **External orchestration**: Script handles complex coordination
    - **Airflow for execution**: DAGs focus on data processing
    - **Resource management**: Script prevents resource conflicts
    - **Operational simplicity**: One script controls everything
    
    ## 🎯 **Usage Pattern**
    1. Start script: `screen -S streaming -dm ./batch_streaming_manager.sh monitor`
    2. Script automatically triggers this DAG during streaming phases
    3. Script stops this DAG during batch phases
    4. Monitor with: `./batch_streaming_manager.sh status`
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