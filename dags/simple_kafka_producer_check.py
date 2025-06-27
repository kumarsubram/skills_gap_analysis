"""
Simple Kafka Test DAG

Tests Kafka connectivity by sending a hello message.
Uses environment detection to identify Mac vs VPS.
"""

import os
import json
from datetime import datetime, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator

# DAG Configuration
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 6, 23),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

def detect_environment():
    """Detect if running on Mac or VPS"""
    airflow_env = os.getenv('AIRFLOW_ENV', 'mac').lower()
    
    if airflow_env in ['vps', 'prod', 'production']:
        return 'VPS'
    else:
        return 'Mac'

def get_kafka_bootstrap_servers():
    """Kafka is always localhost for container-to-container communication"""
    return 'kafka:29092'

def send_hello_message(**context):
    """Send a simple hello message to Kafka"""
    
    print("🚀 SIMPLE KAFKA TEST")
    print("=" * 40)
    
    # Environment detection
    environment = detect_environment()
    bootstrap_servers = get_kafka_bootstrap_servers()
    
    print(f"🔍 Environment: {environment}")
    print(f"🔗 Kafka servers: {bootstrap_servers}")
    
    try:
        from confluent_kafka import Producer
        
        # Create Kafka producer
        config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': f'simple-test-{environment.lower()}',
            'acks': 'all',
            'retries': 3,
            'retry.backoff.ms': 1000,
        }
        
        print("🔧 Creating Kafka producer...")
        producer = Producer(config)
        print("✅ Kafka producer created successfully")
        
        # Create hello message
        message = {
            'message': f'Hello from {environment}!',
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'environment': environment,
            'bootstrap_servers': bootstrap_servers,
            'airflow_run_id': context.get('run_id', 'unknown'),
            'execution_date': context.get('execution_date', datetime.now()).isoformat()
        }
        
        # Topic name
        topic = 'test-topic'
        key = f'{environment.lower()}-hello'
        
        print(f"📤 Sending message to topic '{topic}'...")
        print(f"🔑 Message key: {key}")
        print(f"📝 Message content: {json.dumps(message, indent=2)}")
        
        # Send message
        producer.produce(
            topic=topic,
            key=key,
            value=json.dumps(message),
            callback=delivery_callback
        )
        
        # Flush messages
        print("📨 Flushing messages...")
        unflushed = producer.flush(timeout=30)
        
        if unflushed == 0:
            print("🎉 SUCCESS: Message sent successfully!")
            return {
                'status': 'success',
                'environment': environment,
                'topic': topic,
                'message_sent': True
            }
        else:
            print(f"⚠️ WARNING: {unflushed} messages still pending")
            return {
                'status': 'partial',
                'environment': environment,
                'topic': topic,
                'unflushed_count': unflushed
            }
            
    except ImportError:
        error_msg = "confluent-kafka not installed. Add 'confluent-kafka' to your requirements."
        print(f"❌ {error_msg}")
        raise ImportError(error_msg)
        
    except Exception as e:
        print(f"❌ ERROR: {e}")
        print(f"🔍 Error type: {type(e).__name__}")
        import traceback
        print("📋 Full traceback:")
        traceback.print_exc()
        
        return {
            'status': 'failed',
            'environment': environment,
            'error': str(e)
        }

def delivery_callback(err, msg):
    """Callback for message delivery confirmation"""
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def test_environment_detection():
    """Test environment detection logic"""
    
    print("🔍 ENVIRONMENT DETECTION TEST")
    print("=" * 40)
    
    environment = detect_environment()
    bootstrap_servers = get_kafka_bootstrap_servers()
    
    # Get environment variables for debugging
    airflow_env = os.getenv('AIRFLOW_ENV', 'not_set')
    vps_ip = os.getenv('VPS_IP', 'not_set')
    
    print("📋 Environment Variables:")
    print(f"   AIRFLOW_ENV: {airflow_env}")
    print(f"   VPS_IP: {vps_ip}")
    print()
    print(f"🎯 Detected Environment: {environment}")
    print(f"🔗 Kafka Bootstrap Servers: {bootstrap_servers}")
    print("=" * 40)
    
    return {
        'detected_environment': environment,
        'bootstrap_servers': bootstrap_servers,
        'airflow_env': airflow_env,
        'vps_ip': vps_ip
    }

# Create DAG
dag = DAG(
    dag_id='simple_kafka_producer_check',
    default_args=default_args,
    description='Simple Kafka connectivity test with environment detection',
    schedule=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['test', 'kafka', 'simple', 'connectivity'],
)

# Tasks
env_detection_task = PythonOperator(
    task_id='test_environment_detection',
    python_callable=test_environment_detection,
    dag=dag,
)

kafka_test_task = PythonOperator(
    task_id='send_hello_message',
    python_callable=send_hello_message,
    dag=dag,
)

# Task dependencies
env_detection_task >> kafka_test_task