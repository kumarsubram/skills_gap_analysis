from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging

# Default arguments for the DAG
default_args = {
    'owner': 'skills-gap-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 6),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'simple_test_dag',
    default_args=default_args,
    description='DAG to verify local and remote deployment',
    schedule=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['test', 'deployment', 'simple']
)

def print_environment_info(**context):
    """Print basic environment and execution info"""
    import os
    import platform
    import sys
    from datetime import datetime
    
    # Log environment details
    logging.info("=== ENVIRONMENT INFO ===")
    logging.info(f"Python version: {sys.version}")
    logging.info(f"Platform: {platform.platform()}")
    logging.info(f"Current time: {datetime.now()}")
    
    # Use logical_date instead of execution_date (Airflow 2.4+)
    logical_date = context.get('logical_date') or context.get('execution_date')
    logging.info(f"Logical date: {logical_date}")
    logging.info(f"DAG run ID: {context['dag_run'].run_id}")
    logging.info(f"Task instance: {context['task_instance'].task_id}")
    
    # Check some environment variables
    airflow_home = os.getenv('AIRFLOW_HOME', 'Not set')
    logging.info(f"AIRFLOW_HOME: {airflow_home}")
    
    # Return some data for downstream tasks
    return {
        'timestamp': datetime.now().isoformat(),
        'logical_date': str(logical_date),
        'python_version': sys.version,
        'platform': platform.platform(),
        'dag_run_id': context['dag_run'].run_id
    }

def test_data_processing(**context):
    """Simulate some basic data processing"""
    import json
    import time
    
    # Get data from previous task
    upstream_data = context['task_instance'].xcom_pull(task_ids='print_env_info')
    logging.info(f"Received upstream data: {upstream_data}")
    
    # Simulate processing
    logging.info("Starting data processing simulation...")
    time.sleep(2)  # Simulate work
    
    # Create some mock processed data
    processed_data = {
        'processed_at': datetime.now().isoformat(),
        'input_data': upstream_data,
        'processing_status': 'success',
        'records_processed': 1000,
        'technologies_found': ['Python', 'Airflow', 'Docker', 'VPS']
    }
    
    logging.info(f"Processing complete: {json.dumps(processed_data, indent=2)}")
    return processed_data

def validate_results(**context):
    """Validate the processing results"""
    processed_data = context['task_instance'].xcom_pull(task_ids='process_data')
    
    # Simple validation
    if not processed_data:
        raise ValueError("No processed data received from upstream task")
    
    if processed_data.get('processing_status') != 'success':
        raise ValueError("Processing status indicates failure")
    
    records_count = processed_data.get('records_processed', 0)
    if records_count < 1:
        raise ValueError("No records were processed")
    
    logging.info(f"✅ Validation passed! Processed {records_count} records")
    logging.info(f"✅ Technologies detected: {processed_data.get('technologies_found', [])}")
    
    return "validation_passed"

# Task 1: Print environment information
print_env_task = PythonOperator(
    task_id='print_env_info',
    python_callable=print_environment_info,
    dag=dag,
)

# Task 2: Check system info with bash
check_system_task = BashOperator(
    task_id='check_system_info',
    bash_command='''
    echo "=== SYSTEM INFO ==="
    echo "Date: $(date)"
    echo "Hostname: $(hostname)"
    echo "Disk usage:"
    df -h | head -5
    echo "Memory info:"
    free -h || echo "Memory info not available (likely macOS)"
    echo "=== END SYSTEM INFO ==="
    ''',
    dag=dag,
)

# Task 3: Process some mock data
process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=test_data_processing,
    dag=dag,
)

# Task 4: Validate results
validate_task = PythonOperator(
    task_id='validate_results',
    python_callable=validate_results,
    dag=dag,
)

# Task 5: Final success message
success_task = BashOperator(
    task_id='deployment_success',
    bash_command='''
    echo "🎉 SUCCESS! Simple test DAG completed successfully"
    echo "✅ Local development: Working"
    echo "✅ Remote deployment: Working" 
    echo "✅ Task dependencies: Working"
    echo "✅ XCom data passing: Working"
    echo "Ready for complex DAG development!"
    ''',
    dag=dag,
)

# Define task dependencies
print_env_task >> [check_system_task, process_data_task]
process_data_task >> validate_task >> success_task