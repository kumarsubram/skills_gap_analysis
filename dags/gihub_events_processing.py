"""
GitHub Events Processing DAG - Idempotent with Deduplication
===========================================================

Processes GitHub events for CURRENT DAY ONLY with deduplication.
Safe to re-run - tracks processed event IDs in output to avoid duplicates.

Place this file at: /usr/local/airflow/dags/github_events_processing.py
"""

from datetime import datetime, timezone, timedelta
import json
import logging
import time
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

from include.utils.kafka_producer_utils import create_kafka_producer, send_to_kafka, close_kafka_producer
from include.utils.kafka_consumer_utils import create_kafka_consumer, close_kafka_consumer
from include.utils.github_utils import load_keywords, extract_technologies_from_event, process_github_event_detailed

# ============================================================================
# CONFIGURATION
# ============================================================================

RAW_TOPIC = "github-events"
PROCESSED_TOPIC = "github-processed"

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = {
    'owner': 'skills-gap-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'retries': 1,  # Safe to retry with deduplication
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'github_events_processing',
    default_args=default_args,
    description='Process GitHub events for CURRENT DAY ONLY - Idempotent',
    schedule='15 0 * * *',  # Daily at 00:15 (15 min after streaming starts)
    catchup=False,
    max_active_runs=1,
    tags=['github', 'kafka', 'daily', 'idempotent'],
)

# ============================================================================
# IDEMPOTENCY FUNCTIONS
# ============================================================================

def get_processed_output_file(date_str):
    """Get path to file tracking processed output for idempotency"""
    data_dir = "/usr/local/airflow/data/processing"
    os.makedirs(data_dir, exist_ok=True)
    return os.path.join(data_dir, f"processed_output_{date_str}.json")

def load_processed_output_ids(date_str):
    """Load set of already processed event IDs from output (idempotency check)"""
    processed_file = get_processed_output_file(date_str)
    
    if os.path.exists(processed_file):
        try:
            with open(processed_file, 'r') as f:
                data = json.load(f)
                processed_ids = set(data.get('event_ids', []))
                logging.info(f"📋 Loaded {len(processed_ids)} previously processed output event IDs")
                return processed_ids
        except Exception as e:
            logging.warning(f"⚠️ Error loading processed output: {e}")
    
    logging.info(f"📋 Starting fresh - no previous processed output found")
    return set()

def save_processed_output_ids(date_str, processed_ids, metrics):
    """Save processed output event IDs for idempotency"""
    processed_file = get_processed_output_file(date_str)
    
    data = {
        'date': date_str,
        'event_ids': list(processed_ids),
        'metrics': metrics,
        'last_updated': datetime.now(timezone.utc).isoformat()
    }
    
    try:
        with open(processed_file, 'w') as f:
            json.dump(data, f, indent=2)
        logging.info(f"💾 Saved {len(processed_ids)} processed output event IDs")
    except Exception as e:
        logging.error(f"❌ Error saving processed output: {e}")

# ============================================================================
# MAIN PROCESSING FUNCTION
# ============================================================================

def process_github_events(**context):
    """Process GitHub events for CURRENT DAY ONLY using historical DAG approach with deduplication"""
    
    # Get current day boundaries
    now = datetime.now(timezone.utc)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + timedelta(days=1)
    date_str = start_of_day.strftime('%Y-%m-%d')
    
    logging.info(f"🚀 Processing GitHub events for CURRENT DAY: {date_str}")
    logging.info(f"⏱️ Will run until: {end_of_day.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    logging.info(f"📥 Consuming from: {RAW_TOPIC}")
    logging.info(f"📤 Producing to: {PROCESSED_TOPIC}")
    
    # Load keywords
    keywords = load_keywords()
    logging.info(f"📋 Loaded {len(keywords)} keywords")
    
    # Load previously processed output for idempotency
    processed_output_ids = load_processed_output_ids(date_str)
    
    # Create consumer and producer
    consumer = create_kafka_consumer('github-processor', [RAW_TOPIC])
    producer = create_kafka_producer('github-simple')
    
    metrics = {
        'total_processed': 0,
        'events_with_tech': 0,
        'total_tech_mentions': 0,
        'current_day_events': 0,
        'other_day_events': 0,
        'duplicate_events': 0,
        'new_events': 0,
    }
    
    try:
        # Run until end of current day
        while datetime.now(timezone.utc) < end_of_day:
            message = consumer.poll(timeout=5.0)
            
            if message is None:
                # Log every few minutes to show it's alive
                if metrics['total_processed'] % 20 == 0 and metrics['total_processed'] > 0:
                    current_time = datetime.now(timezone.utc)
                    remaining_hours = (end_of_day - current_time).total_seconds() / 3600
                    logging.info(f"⏳ Waiting for messages... {remaining_hours:.1f}h remaining")
                continue
            
            if message.error():
                logging.error(f"❌ Consumer error: {message.error()}")
                continue
            
            try:
                # Parse raw event
                raw_event = json.loads(message.value().decode('utf-8'))
                event_id = raw_event.get('id')
                
                if not event_id:
                    consumer.commit(message)
                    continue
                
                # Check if event is from current day
                event_time_str = raw_event.get('created_at')
                if event_time_str:
                    try:
                        event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
                        
                        # Skip events not from current day
                        if not (start_of_day <= event_time < end_of_day):
                            metrics['other_day_events'] += 1
                            consumer.commit(message)
                            continue
                    except:
                        # If time parsing fails, process anyway
                        pass
                
                metrics['current_day_events'] += 1
                
                # Check for duplicates in output (idempotency)
                if event_id in processed_output_ids:
                    metrics['duplicate_events'] += 1
                    consumer.commit(message)
                    continue
                
                # New event - process it
                metrics['new_events'] += 1
                
                # Process event using same approach as historical DAG
                processed_event = process_github_event_detailed(raw_event)
                
                # Extract technologies using same logic as historical
                technologies = extract_technologies_from_event(raw_event, keywords)
                
                # Add technology data matching historical CSV structure
                processed_event.update({
                    'technologies': technologies,
                    'technology_count': len(technologies),
                    'processing_date': date_str,  # Mark which day this belongs to
                })
                
                # Add metadata for detected technologies (same as historical)
                if technologies:
                    tech_metadata = []
                    for tech in technologies:
                        if tech in keywords:
                            meta = keywords[tech]
                            tech_metadata.append({
                                'keyword': tech,
                                'type': meta.get('type', ''),
                                'language': meta.get('language', ''),
                                'framework': meta.get('framework', '')
                            })
                    processed_event['technology_metadata'] = tech_metadata
                    
                    logging.info(f"✅ {raw_event.get('type')} from {processed_event.get('repo_name')} - {len(technologies)} techs: {technologies[:3]}")
                
                # Send to Kafka
                success = send_to_kafka(producer, PROCESSED_TOPIC, [processed_event])
                
                if success > 0:
                    metrics['total_processed'] += 1
                    processed_output_ids.add(event_id)  # Track for idempotency
                    
                    if technologies:
                        metrics['events_with_tech'] += 1
                        metrics['total_tech_mentions'] += len(technologies)
                
                # Commit message
                consumer.commit(message)
                
                # Save progress periodically for crash recovery
                if metrics['total_processed'] % 100 == 0:
                    save_processed_output_ids(date_str, processed_output_ids, metrics)
                    logging.info(f"📊 Processed: {metrics['total_processed']} events, With tech: {metrics['events_with_tech']}")
                
                time.sleep(0.01)  # Small delay
                
            except Exception as e:
                logging.error(f"❌ Processing error: {str(e)}")
                consumer.commit(message)  # Skip problematic message
        
        # End of day reached
        logging.info("🏁 End of current day reached. Stopping processing.")
    
    except KeyboardInterrupt:
        logging.info("🛑 Processing stopped manually")
    
    finally:
        close_kafka_consumer(consumer)
        close_kafka_producer(producer)
        
        # Final save of processed output
        save_processed_output_ids(date_str, processed_output_ids, metrics)
        
        runtime_hours = (datetime.now(timezone.utc) - start_of_day).total_seconds() / 3600
        
        logging.info("=" * 60)
        logging.info(f"📊 DAILY PROCESSING SUMMARY for {date_str}")
        logging.info(f"   Current day events seen: {metrics['current_day_events']}")
        logging.info(f"   Other day events skipped: {metrics['other_day_events']}")
        logging.info(f"   New events processed: {metrics['new_events']}")
        logging.info(f"   Duplicate events skipped: {metrics['duplicate_events']} (idempotency)")
        logging.info(f"   Total processed: {metrics['total_processed']}")
        logging.info(f"   Events with tech: {metrics['events_with_tech']}")
        logging.info(f"   Total tech mentions: {metrics['total_tech_mentions']}")
        logging.info(f"   Runtime: {runtime_hours:.1f} hours")
        logging.info(f"   📁 Idempotency file: {get_processed_output_file(date_str)}")
        logging.info("=" * 60)

# ============================================================================
# TASK DEFINITION
# ============================================================================

process_events_task = PythonOperator(
    task_id='process_github_events',
    python_callable=process_github_events,
    dag=dag,
)