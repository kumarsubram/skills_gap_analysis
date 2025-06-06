"""
Kafka Producer Utilities - COMPLETE VERSION
===========================================

Complete utilities for Kafka producer operations.
Place this at: include/utils/kafka_producer_utils.py
"""

from confluent_kafka import Producer
from .connections import get_kafka_connection
import json
import logging
from typing import List, Dict, Any

def create_kafka_producer(client_id_suffix: str = "producer") -> Producer:
    """
    Create a Kafka producer with proper configuration.
    
    Args:
        client_id_suffix: Suffix to add to client ID
        
    Returns:
        Configured Kafka Producer
    """
    try:
        # Get the connection config
        config = get_kafka_connection()
        
        # Update client ID if suffix provided
        if client_id_suffix:
            base_client_id = config.get('client.id', 'airflow')
            config['client.id'] = f"{base_client_id}-{client_id_suffix}"
        
        # CRITICAL: Validate bootstrap.servers before creating producer
        bootstrap_servers = config.get('bootstrap.servers')
        if not bootstrap_servers or bootstrap_servers.strip() == '':
            raise ValueError(f"Invalid bootstrap.servers: '{bootstrap_servers}'")
        
        logging.info(f"🔧 Creating Kafka producer with config: {config}")
        
        # Create producer
        producer = Producer(config)
        
        logging.info(f"✅ Kafka producer created: {config.get('client.id')}")
        return producer
        
    except Exception as e:
        logging.error(f"❌ Failed to create Kafka producer: {str(e)}")
        import traceback
        logging.error(f"📋 Full traceback: {traceback.format_exc()}")
        raise

def send_to_kafka(producer: Producer, topic: str, messages: List[Dict[str, Any]]) -> int:
    """
    Send messages to Kafka topic.
    
    Args:
        producer: Kafka producer instance
        topic: Target topic name
        messages: List of message dictionaries
        
    Returns:
        Number of messages successfully sent
    """
    if not messages:
        logging.warning("📭 No messages to send")
        return 0
    
    sent_count = 0
    failed_count = 0
    
    try:
        for message in messages:
            try:
                # Convert to JSON
                message_json = json.dumps(message, default=str)
                
                # Send to Kafka (without callback for simplicity)
                producer.produce(
                    topic=topic,
                    value=message_json
                )
                sent_count += 1
                
            except Exception as e:
                logging.error(f"❌ Failed to send individual message: {str(e)}")
                failed_count += 1
                continue
        
        # Force delivery of all messages
        logging.info(f"📤 Flushing {sent_count} messages to {topic}...")
        
        # Use a longer timeout for flush
        pending = producer.flush(timeout=30.0)
        
        if pending == 0:
            logging.info(f"✅ Successfully sent {sent_count} messages to {topic}")
        else:
            logging.warning(f"⚠️ {pending} messages still pending after flush")
            logging.warning(f"⚠️ Sent {sent_count - pending}/{sent_count} messages to {topic}")
        
        return sent_count - pending
        
    except Exception as e:
        logging.error(f"❌ Failed to send messages to {topic}: {str(e)}")
        import traceback
        logging.error(f"📋 Full traceback: {traceback.format_exc()}")
        return 0

def close_kafka_producer(producer: Producer, timeout: float = 5.0):
    """
    Close Kafka producer properly.
    
    Args:
        producer: Kafka producer instance
        timeout: Timeout for flush operation in seconds
    """
    try:
        # Flush any remaining messages
        pending = producer.flush(timeout=timeout)
        if pending > 0:
            logging.warning(f"⚠️ {pending} messages still pending when closing producer")
        logging.info("✅ Kafka producer closed")
    except Exception as e:
        logging.error(f"❌ Error closing producer: {str(e)}")

def delivery_callback(err, msg, topic):
    """Callback for message delivery confirmation."""
    if err is not None:
        logging.error(f"❌ Message delivery failed to {topic}: {err}")
    else:
        logging.debug(f"✅ Message delivered to {topic} [{msg.partition()}]")
