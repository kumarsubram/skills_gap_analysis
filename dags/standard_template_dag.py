"""
Standard DAG Template - Copy and Customize
==========================================

Simple template for creating new DAGs in the Skills Gap Analysis project.

TO USE THIS TEMPLATE:
1. Copy this file to a new name (e.g., my_new_dag.py)
2. Change DAG_NAME and description below
3. Replace example_task() with your logic
4. Update task dependencies as needed

Save as: /usr/local/airflow/dags/YOUR_DAG_NAME.py
"""

from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
import json
import logging

# Import working utilities
from include.config.settings import get_default_dag_args
from include.utils.connections import get_github_connection
from include.utils.kafka_producer_utils import create_kafka_producer, send_to_kafka, close_kafka_producer
from include.utils.kafka_consumer_utils import create_kafka_consumer, close_kafka_consumer
from include.utils.api_utils import safe_api_request
from include.utils.github_utils import process_github_event_detailed

# ============================================================================
# CONFIGURATION - CUSTOMIZE THIS
# ============================================================================

DAG_NAME = 'standard_template_dag'  # ← CHANGE THIS
DESCRIPTION = 'Standard template DAG for Skills Gap Analysis project'  # ← CHANGE THIS
TAGS = ['template', 'standard']  # ← CHANGE THIS

# ============================================================================
# DAG DEFINITION
# ============================================================================

default_args = get_default_dag_args()

dag = DAG(
    DAG_NAME,
    default_args=default_args,
    description=DESCRIPTION,
    schedule=None,  # Change to '@daily', '0 */6 * * *', etc.
    catchup=False,
    max_active_runs=1,
    tags=TAGS,
)

# ============================================================================
# TASK FUNCTIONS - REPLACE WITH YOUR LOGIC
# ============================================================================

def example_task(**context):
    """
    Replace this function with your actual task logic.
    
    This example shows the basic pattern for:
    - Getting connections
    - Creating Kafka producers/consumers
    - Processing data
    - Proper cleanup
    """
    
    logging.info("🚀 Starting example task")
    
    # Example: Stream GitHub events to Kafka
    try:
        # Get connections
        github_config = get_github_connection()
        
        # Create Kafka producer
        producer = create_kafka_producer('example-producer')
        
        # Fetch data from API
        url = f"{github_config['base_url']}/events"
        response = safe_api_request(url, github_config['headers'])
        
        if response:
            events = response.json()
            logging.info(f"📥 Fetched {len(events)} events")
            
            # Send to Kafka
            sent_count = send_to_kafka(producer, 'github-events', events)
            logging.info(f"📤 Sent {sent_count} messages to Kafka")
        else:
            logging.error("❌ Failed to fetch data")
    
    except Exception as e:
        logging.error(f"❌ Task failed: {str(e)}")
        raise
    
    finally:
        # Always clean up
        try:
            close_kafka_producer(producer)
        except:
            pass
    
    logging.info("✅ Example task completed")

def example_processing_task(**context):
    """
    Example of processing events from one topic to another.
    Replace with your processing logic.
    """
    
    logging.info("🔄 Starting processing task")
    
    SOURCE_TOPIC = "github-events"
    TARGET_TOPIC = "processed-events"
    MAX_MESSAGES = 10  # Process first 10 messages
    
    try:
        # Create consumer and producer
        consumer = create_kafka_consumer('example-consumer', [SOURCE_TOPIC])
        producer = create_kafka_producer('example-processor')
        
        processed_count = 0
        
        while processed_count < MAX_MESSAGES:
            # Poll for message
            message = consumer.poll(timeout=5.0)
            
            if message is None:
                logging.info("⏳ No more messages")
                break
            
            if message.error():
                logging.error(f"❌ Consumer error: {message.error()}")
                continue
            
            try:
                # Parse message
                raw_event = json.loads(message.value().decode('utf-8'))
                
                # Process the event (customize this part)
                processed_event = process_github_event_detailed(raw_event)
                processed_event['template_processed'] = True
                processed_event['processed_at'] = datetime.now(timezone.utc).isoformat()
                
                # Send to target topic
                success = send_to_kafka(producer, TARGET_TOPIC, [processed_event])
                
                if success > 0:
                    processed_count += 1
                    logging.info(f"✅ Processed message {processed_count}")
                
                # Commit message
                consumer.commit(message)
                
            except Exception as e:
                logging.error(f"❌ Error processing message: {str(e)}")
                continue
    
    except Exception as e:
        logging.error(f"❌ Processing task failed: {str(e)}")
        raise
    
    finally:
        # Clean up
        try:
            close_kafka_consumer(consumer)
            close_kafka_producer(producer)
        except:
            pass
    
    logging.info(f"✅ Processing completed: {processed_count} messages")

def simple_test_task(**context):
    """
    Simple test task to verify everything works.
    Good for testing new DAGs.
    """
    
    logging.info("🧪 Running simple test")
    
    # Test connections
    from include.utils.connections import test_connections
    results = test_connections()
    logging.info(f"🔗 Connection results: {results}")
    
    # Test reading from a topic
    try:
        consumer = create_kafka_consumer('test-consumer', ['github-events'])
        message = consumer.poll(timeout=5.0)
        
        if message and not message.error():
            logging.info("✅ Successfully read from Kafka")
        else:
            logging.info("ℹ️ No messages or error reading from Kafka")
        
        close_kafka_consumer(consumer)
    except Exception as e:
        logging.error(f"❌ Kafka test failed: {str(e)}")
    
    logging.info("✅ Test completed")

# ============================================================================
# TASK DEFINITIONS - CUSTOMIZE AS NEEDED
# ============================================================================

# Replace these with your actual tasks
task1 = PythonOperator(
    task_id='example_task',  # ← CHANGE THIS
    python_callable=example_task,  # ← CHANGE THIS
    dag=dag,
)

task2 = PythonOperator(
    task_id='processing_task',  # ← CHANGE THIS
    python_callable=example_processing_task,  # ← CHANGE THIS
    dag=dag,
)

test_task = PythonOperator(
    task_id='test_task',
    python_callable=simple_test_task,
    dag=dag,
)

# ============================================================================
# TASK DEPENDENCIES - CUSTOMIZE AS NEEDED
# ============================================================================

# Simple linear dependency
test_task >> task1 >> task2

# OR parallel execution:
# test_task >> [task1, task2]

# OR more complex:
# test_task >> task1
# task1 >> task2

# ============================================================================
# NOTES FOR CUSTOMIZATION
# ============================================================================
"""
COMMON PATTERNS:

1. GITHUB STREAMING:
   - Use: get_github_connection(), safe_api_request()
   - Pattern: Fetch from GitHub API → send_to_kafka()

2. KAFKA PROCESSING:
   - Use: create_kafka_consumer(), create_kafka_producer()
   - Pattern: consumer.poll() → process → send_to_kafka()

3. TOPIC ROUTING:
   - Read from one topic, route to multiple topics based on content

4. AI DETECTION:
   - Use: extract_ai_technologies() from github_utils
   - Route AI repos to special topics

WORKING IMPORTS:
✅ from include.utils.kafka_producer_utils import create_kafka_producer, send_to_kafka, close_kafka_producer
✅ from include.utils.kafka_consumer_utils import create_kafka_consumer, close_kafka_consumer
✅ from include.utils.connections import get_github_connection, test_connections
✅ from include.utils.api_utils import safe_api_request
✅ from include.utils.github_utils import process_github_event_detailed, extract_ai_technologies

CONSUMER PATTERN (reads existing data):
consumer = create_kafka_consumer('my-group', ['topic-name'])
while True:
    message = consumer.poll(timeout=5.0)  # Single message
    if message is None: break
    if message.error(): continue
    # Process message.value().decode('utf-8')
    consumer.commit(message)

PRODUCER PATTERN (sends data):
producer = create_kafka_producer('my-producer')
data = [{'key': 'value'}]  # List of dictionaries
sent = send_to_kafka(producer, 'topic-name', data)
close_kafka_producer(producer)

KAFKA TOPICS AVAILABLE:
- github-events (raw GitHub events)
- github-processed-* (processed events)
- test-topic (for testing)

File Structure Used:
├── dags/
│   ├── standard_template_dag.py   # This template file
│   ├── github_events_streaming.py # Working GitHub streaming
│   ├── github_processing_full.py  # Full event processing
│   └── your_new_dag.py            # Copy this template here
│
├── include/
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py            # ✅ SkillsGapConfig, get_default_dag_args()
│   │
│   └── utils/
│       ├── __init__.py
│       ├── connections.py         # ✅ get_kafka_connection(), get_github_connection()
│       ├── kafka_producer_utils.py # ✅ create_kafka_producer(), send_to_kafka()
│       ├── kafka_consumer_utils.py # ✅ create_kafka_consumer(), close_kafka_consumer()
│       ├── api_utils.py           # ✅ safe_api_request(), extract_text_content()
│       ├── github_utils.py        # ✅ process_github_event_detailed(), AI detection
│       ├── job_utils.py           # 💼 Job posting processing utilities
│       └── rate_limiting.py       # ⏱️ Rate limit management
│
├── airflow_settings.yaml          # ✅ Kafka & GitHub connections
└── requirements.txt               # Python dependencies

AIRFLOW CONFIGURATION (airflow_settings.yaml):
connections:
  - conn_id: kafka_default
    conn_type: kafka
    conn_host: 31.220.22.47
    conn_port: 9092
    conn_extra: |
      {
        "security.protocol": "PLAINTEXT",
        "bootstrap.servers": "31.220.22.47:9092",
        "client.id": "airflow-skills-gap"
      }
  - conn_id: github_api
    conn_type: http
    conn_host: https://api.github.com
    conn_extra: |
      {
        "Authorization": "Bearer YOUR_GITHUB_TOKEN"
      }

variables:
  - variable_name: github_token
    variable_value: "YOUR_GITHUB_TOKEN"
  - variable_name: kafka_bootstrap_servers
    variable_value: "31.220.22.47:9092"
  - variable_name: project_name
    variable_value: "skills-gap-analysis"

KAFKA TOPICS SETUP:
📥 Source Topics:
- github-events           # Raw GitHub events (18MB data available)
- test-topic              # For testing

📤 Processed Topics:
- github-processed-active-development  # PushEvents
- github-processed-ai-signals         # AI/ML repos
- github-processed-collaboration      # PRs & reviews
- github-processed-community          # Stars, forks, issues
- github-processed-emerging-tech      # New tech patterns
- github-processed-new-projects       # CreateEvents
- github-processed-releases           # ReleaseEvents

ACCESS KAFKA UI: http://31.220.22.47:8085/
"""