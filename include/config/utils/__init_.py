"""
Utilities Package
================

Import commonly used utilities for easy access in DAGs.
Updated to include GitHub processing utilities and consumer functions.
"""

from .connections import get_kafka_connection, get_github_connection
from .kafka_producer_utils import (
    create_kafka_producer, 
    create_kafka_consumer,  # Added missing consumer
    send_to_kafka
)
from .api_utils import safe_api_request, process_github_event
from .rate_limiting import check_rate_limit_safety

# GitHub utilities for enhanced processing
from .github_utils import (
    process_github_event_detailed,
    extract_technologies_from_repo,
    extract_ai_technologies,
    detect_emerging_tech_patterns,
    calculate_technology_confidence,
    get_github_event_priority
)

__all__ = [
    # Connection utilities
    'get_kafka_connection',
    'get_github_connection',
    
    # Kafka utilities
    'create_kafka_producer',
    'create_kafka_consumer',  # Added missing consumer
    'send_to_kafka',
    
    # API utilities
    'safe_api_request',
    'process_github_event',
    'check_rate_limit_safety',
    
    # GitHub processing utilities
    'process_github_event_detailed',
    'extract_technologies_from_repo',
    'extract_ai_technologies',
    'detect_emerging_tech_patterns',
    'calculate_technology_confidence',
    'get_github_event_priority'
]