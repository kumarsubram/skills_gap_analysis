"""
GitHub Events Streaming DAG - Idempotent with Deduplication
===========================================================

Streams GitHub events for CURRENT DAY ONLY with deduplication.
Safe to re-run - tracks processed event IDs to avoid duplicates.

Place this file at: /usr/local/airflow/dags/github_simple_streaming.py
"""

from datetime import datetime, timedelta, timezone
import time
import logging
import os
import json
from airflow import DAG
from airflow.operators.python import PythonOperator

from include.config.settings import get_default_dag_args
from include.utils.connections import get_github_connection
from include.utils.kafka_producer_utils import create_kafka_producer, send_to_kafka, close_kafka_producer
from include.utils.api_utils import safe_api_request

# ============================================================================
# CONFIGURATION
# ============================================================================

POLL_INTERVAL_SECONDS = 15
KAFKA_TOPIC = "github-events"

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = get_default_dag_args()

dag = DAG(
    'github_events_streaming',
    default_args=default_args,
    description='Stream GitHub events for CURRENT DAY ONLY - Idempotent',
    schedule='0 0 * * *',  # Daily at midnight
    catchup=False,
    max_active_runs=1,
    tags=['github', 'kafka', 'daily', 'idempotent'],
)

# ============================================================================
# IDEMPOTENCY FUNCTIONS
# ============================================================================

def get_processed_events_file(date_str):
    """Get path to file tracking processed event IDs for idempotency"""
    data_dir = "/usr/local/airflow/data/streaming"
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, f"processed_events_{date_str}.json")

def load_processed_events(date_str):
    """Load set of already processed event IDs (idempotency check)"""
    processed_file = get_processed_events_file(date_str)
    
    if os.path.exists(processed_file):
        try:
            with open(processed_file, 'r') as f:
                data = json.load(f)
                processed_ids = set(data.get('event_ids', []))
                logging.info(f"📋 Loaded {len(processed_ids)} previously processed event IDs")
                return processed_ids
        except Exception as e:
            logging.warning(f"⚠️ Error loading processed events: {e}")
    
    logging.info("📋 Starting fresh - no previous processed events found")
    return set()

def save_processed_events(date_str, processed_ids, total_streamed):
    """Save processed event IDs for idempotency"""
    processed_file = get_processed_events_file(date_str)
    
    data = {
        'date': date_str,
        'event_ids': list(processed_ids),
        'total_streamed': total_streamed,
        'last_updated': datetime.now(timezone.utc).isoformat()
    }
    
    try:
        with open(processed_file, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info(f"💾 Saved {len(processed_ids)} processed event IDs")
    except Exception as e:
        logging.error(f"❌ Error saving processed events: {e}")

# ============================================================================
# MAIN STREAMING FUNCTION
# ============================================================================

def stream_raw_github_events(**context):
    """Stream GitHub events for CURRENT DAY ONLY - with deduplication"""
    
    # Get current day boundaries
    now = datetime.now(timezone.utc)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + timedelta(days=1)
    date_str = start_of_day.strftime('%Y-%m-%d')
    
    logging.info(f"🚀 Streaming GitHub events for CURRENT DAY: {date_str}")
    logging.info(f"⏱️ Will run until: {end_of_day.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logging.info(f"📡 Topic: {KAFKA_TOPIC}")
    
    # Load previously processed events for idempotency
    processed_event_ids = load_processed_events(date_str)
    
    github_config = get_github_connection()
    producer = create_kafka_producer('github-raw')
    
    total_events = 0
    current_day_events = 0
    duplicate_events = 0
    new_events = 0
    cycles = 0
    
    try:
        # Run until end of current day
        while datetime.now(timezone.utc) < end_of_day:
            cycles += 1
            
            # Fetch events from GitHub
            url = f"{github_config['base_url']}/events"
            response = safe_api_request(url, github_config['headers'])
            
            if response:
                events = response.json()
                
                # Filter and deduplicate events for current day
                todays_new_events = []
                for event in events:
                    event_id = event.get('id')
                    event_time_str = event.get('created_at')
                    
                    if not event_id or not event_time_str:
                        continue
                    
                    try:
                        event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
                        
                        # Only include events from current day
                        if start_of_day <= event_time < end_of_day:
                            current_day_events += 1
                            
                            # Check for duplicates (idempotency)
                            if event_id in processed_event_ids:
                                duplicate_events += 1
                            else:
                                # New event - add to stream
                                todays_new_events.append(event)
                                processed_event_ids.add(event_id)
                                new_events += 1
                    except Exception as e:
                        print(f"❌ Failed to load keywords: {e}")
                        continue
                
                logging.info(f"📥 Cycle {cycles}: {len(todays_new_events)} new/{len(events)} total events")
                if duplicate_events > 0:
                    logging.info(f"   🔄 Skipped {duplicate_events} duplicate events (idempotency)")
                
                # Send only new events to Kafka
                if todays_new_events:
                    sent_count = send_to_kafka(producer, KAFKA_TOPIC, todays_new_events)
                    total_events += sent_count
                
                # Log rate limit
                remaining = response.headers.get('X-RateLimit-Remaining', 'Unknown')
                logging.info(f"📊 Rate limit remaining: {remaining}")
            else:
                logging.warning(f"❌ Cycle {cycles}: API request failed")
            
            # Save progress periodically for crash recovery
            if cycles % 20 == 0:  # Every ~5 minutes
                save_processed_events(date_str, processed_event_ids, total_events)
            
            # Sleep until next poll
            time.sleep(POLL_INTERVAL_SECONDS)
            
            # Progress logging
            if cycles % 100 == 0:
                runtime_hours = (datetime.now(timezone.utc) - start_of_day).total_seconds() / 3600
                logging.info(f"⏰ Runtime: {runtime_hours:.1f}h, New: {new_events}, Duplicates: {duplicate_events}")
        
        # End of day reached
        logging.info("🏁 End of current day reached. Stopping gracefully.")
        
    finally:
        close_kafka_producer(producer)
        
        # Final save of processed events
        save_processed_events(date_str, processed_event_ids, total_events)
        
        runtime_hours = (datetime.now(timezone.utc) - start_of_day).total_seconds() / 3600
        
        logging.info("=" * 60)
        logging.info(f"📊 DAILY STREAMING SUMMARY for {date_str}")
        logging.info(f"   New events streamed: {new_events}")
        logging.info(f"   Duplicate events skipped: {duplicate_events}")
        logging.info(f"   Total events sent to Kafka: {total_events}")
        logging.info(f"   Runtime: {runtime_hours:.1f} hours")
        logging.info(f"   Cycles completed: {cycles}")
        logging.info(f"   📁 Idempotency file: {get_processed_events_file(date_str)}")
        logging.info("=" * 60)

# ============================================================================
# TASK DEFINITION
# ============================================================================

stream_task = PythonOperator(
    task_id='stream_raw_events',
    python_callable=stream_raw_github_events,
    dag=dag,
)