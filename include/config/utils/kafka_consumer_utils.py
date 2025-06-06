"""
Kafka Consumer Utilities
========================

Separate utilities for Kafka consumer operations.
Place this at: include/utils/kafka_consumer_utils.py
"""

from confluent_kafka import Consumer
from .connections import get_kafka_connection
import logging
from typing import List

def create_kafka_consumer(group_id: str, topics: List[str], **kwargs) -> Consumer:
    """
    Create a Kafka consumer with proper configuration.
    
    Args:
        group_id: Consumer group ID
        topics: List of topics to subscribe to
        **kwargs: Additional Kafka configuration
        
    Returns:
        Configured Kafka Consumer
    """
    try:
        # Get base connection config
        config = get_kafka_connection()
        
        # Add consumer-specific settings
        config.update({
            'group.id': group_id,
            'auto.offset.reset': 'earliest',  # FIXED: Start from beginning
            'enable.auto.commit': False,
            'max.poll.interval.ms': 300000,  # 5 minutes
        })
        
        # Apply any additional kwargs
        config.update(kwargs)
        
        # Validate bootstrap.servers
        bootstrap_servers = config.get('bootstrap.servers')
        if not bootstrap_servers or bootstrap_servers.strip() == '':
            raise ValueError(f"Invalid bootstrap.servers: '{bootstrap_servers}'")
        
        logging.info(f"🔧 Creating Kafka consumer with config: {config}")
        
        # Create consumer
        consumer = Consumer(config)
        
        # Subscribe to topics
        consumer.subscribe(topics)
        
        logging.info(f"✅ Kafka consumer created: {group_id}, topics: {topics}")
        return consumer
        
    except Exception as e:
        logging.error(f"❌ Failed to create Kafka consumer: {str(e)}")
        import traceback
        logging.error(f"📋 Full traceback: {traceback.format_exc()}")
        raise

def close_kafka_consumer(consumer: Consumer):
    """Close Kafka consumer properly."""
    try:
        consumer.close()
        logging.info("✅ Kafka consumer closed")
    except Exception as e:
        logging.error(f"❌ Error closing consumer: {str(e)}")

def test_kafka_consumer_connection(test_topic: str = "test-topic"):
    """
    Test Kafka consumer connection.
    
    Args:
        test_topic: Topic to test with
        
    Returns:
        Dict with test results
    """
    result = {
        'status': 'unknown',
        'details': {},
        'error': None
    }
    
    try:
        # Test consumer creation
        logging.info("🧪 Testing Kafka consumer creation...")
        consumer = create_kafka_consumer("test-consumer", [test_topic])
        result['details']['consumer_created'] = True
        
        # Test polling (quick test)
        logging.info("🧪 Testing consumer poll...")
        messages = consumer.poll(timeout=2.0)  # 2 second timeout
        result['details']['poll_attempted'] = True
        result['details']['messages_received'] = len(messages) if messages else 0
        
        # Clean up
        close_kafka_consumer(consumer)
        result['details']['consumer_closed'] = True
        
        result['status'] = 'success'
        logging.info(f"✅ Consumer test completed: {result}")
        
    except Exception as e:
        result['status'] = 'failed'
        result['error'] = str(e)
        logging.error(f"❌ Consumer test failed: {str(e)}")
        import traceback
        logging.error(f"📋 Full traceback: {traceback.format_exc()}")
    
    return result