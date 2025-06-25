"""
GitHub Events Producer DAG
==========================

Streams GitHub events to Kafka every 3 seconds for 1 hour.
Place at: dags/producer_github_events.py
"""

import os
import json
import time
import requests
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def get_kafka_bootstrap_servers():
    """Environment detection (same as your test DAG)"""
    vps_ip = os.getenv('VPS_IP', 'localhost')
    if vps_ip and vps_ip != 'localhost':
        return f'{vps_ip}:9092'
    else:
        return 'localhost:9092'

def produce_github_events(**context):
    """Produce GitHub events to Kafka"""
    
    print("🚀 GITHUB EVENTS PRODUCER")
    print("=" * 50)
    
    # Import Kafka producer
    try:
        from confluent_kafka import Producer
    except ImportError:
        print("❌ confluent-kafka not installed")
        return {'status': 'failed', 'error': 'confluent-kafka missing'}
    
    # Setup
    bootstrap_servers = get_kafka_bootstrap_servers()
    topic = 'github-events-raw'
    
    print(f"🔗 Kafka: {bootstrap_servers}")
    print(f"📤 Topic: {topic}")
    
    # Create Kafka producer
    producer = Producer({
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'github-events-producer',
        'acks': 'all',
        'retries': 3
    })
    
    # GitHub API setup
    headers = {
        'Accept': 'application/vnd.github.v3+json',
        'User-Agent': 'JobOrbit-Producer/1.0'
    }
    
    # Add GitHub token if available
    github_token = os.getenv('GITHUB_TOKEN')
    if github_token:
        headers['Authorization'] = f'token {github_token}'
        print("✅ Using GitHub token for higher rate limits")
    else:
        print("⚠️ No GitHub token - limited to 60 requests/hour")
    
    # Stream for 10 minutes (testing)
    start_time = datetime.now()
    end_time = start_time + timedelta(hours=1)
    
    total_events = 0
    cycles = 0
    
    print(f"⏰ Producing until: {end_time.strftime('%H:%M:%S')}")
    
    try:
        while datetime.now() < end_time:
            cycles += 1
            
            # Fetch GitHub events
            url = 'https://api.github.com/events'
            
            try:
                response = requests.get(url, headers=headers, timeout=15)
                
                if response.status_code == 200:
                    events = response.json()
                    
                    # Send each event to Kafka
                    for event in events:
                        producer.produce(topic, json.dumps(event))
                        total_events += 1
                    
                    # Flush messages to ensure delivery
                    producer.flush()
                    
                    # Log progress
                    remaining = response.headers.get('X-RateLimit-Remaining', 'Unknown')
                    print(f"📡 Cycle {cycles}: {len(events)} events, Rate limit: {remaining}")
                    
                elif response.status_code == 403:
                    print("❌ Rate limited! Waiting 60 seconds...")
                    time.sleep(3)
                    continue
                    
                else:
                    print(f"❌ HTTP {response.status_code}: {response.text[:100]}")
                
            except Exception as e:
                print(f"❌ Request error: {e}")
            
            # Wait 30 seconds before next poll
            print(f"😴 Waiting 3... (Cycle {cycles} complete)")
            time.sleep(3)
        
        print("🏁 1-hour production complete!")
        
    finally:
        producer.flush()
        
        runtime = datetime.now() - start_time
        
        print("=" * 50)
        print("📊 PRODUCTION SUMMARY:")
        print(f"   Total events sent: {total_events}")
        print(f"   Cycles completed: {cycles}")
        print(f"   Runtime: {runtime}")
        print(f"   Topic: {topic}")
        print("=" * 50)
        
        return {
            'status': 'success',
            'total_events': total_events,
            'cycles': cycles,
            'runtime_minutes': runtime.total_seconds() / 60
        }

# DAG definition
default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2025, 6, 23),
    'email_on_failure': False,
    'retries': 0,  # Don't retry for testing
}

dag = DAG(
    dag_id='producer_github_events',
    default_args=default_args,
    description='GitHub events producer - streams to Kafka for 10 minutes',
    schedule=None,  # Manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=['github', 'kafka', 'producer', 'streaming'],
)

produce_task = PythonOperator(
    task_id='produce_github_events',
    python_callable=produce_github_events,
    dag=dag,
)