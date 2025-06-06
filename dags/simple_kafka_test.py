"""
Most basic Kafka test - just try to create a producer and send one message
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'skills-gap-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def simple_kafka_test():
    """
    Simplest possible Kafka test
    """
    try:
        from confluent_kafka import Producer
        import json
        
        print("🔧 Creating Kafka producer...")
        
        # Minimal configuration
        config = {
            'bootstrap.servers': '31.220.22.47:9092',
            'client.id': 'simple-test',
            'debug': 'broker,topic,msg',  # Enable debug output
        }
        
        print(f"📋 Config: {config}")
        
        # Create producer
        producer = Producer(config)
        
        print("✅ Producer created successfully")
        
        # Simple message
        message = {'test': 'hello kumar', 'timestamp': datetime.now().isoformat()}
        
        print("📤 Attempting to send message...")
        
        # Send message without callback first
        producer.produce('test-topic', json.dumps(message))
        
        print("📨 Message queued, flushing...")
        
        # Try to flush with a longer timeout
        result = producer.flush(30)  # 30 second timeout
        
        if result == 0:
            print("🎉 SUCCESS: Message sent and flushed successfully!")
            return "SUCCESS"
        else:
            print(f"⚠️ Flush completed but {result} messages still in queue")
            return f"PARTIAL: {result} messages pending"
            
    except Exception as e:
        print(f"❌ Error: {e}")
        print(f"🔍 Error type: {type(e).__name__}")
        import traceback
        print(f"📋 Full traceback: {traceback.format_exc()}")
        return f"FAILED: {e}"

# Create DAG
dag = DAG(
    'simple_kafka_test',
    default_args=default_args,
    description='Simplest possible Kafka test',
    schedule=None,
    catchup=False,
    tags=['test', 'kafka', 'simple'],
)

test_task = PythonOperator(
    task_id='simple_test',
    python_callable=simple_kafka_test,
    dag=dag,
)

test_task