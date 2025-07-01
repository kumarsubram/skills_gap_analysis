"""
Streaming utilities module

Provides utilities for managing real-time streaming data pipelines, 
including Delta table management and Kafka integration.

UPDATED: Removed aggregation table utilities for ultra-fast performance.
"""

from .streaming_table_utils import (
    ensure_streaming_table_exists,
    check_streaming_table_exists,
    get_streaming_table_info,
    print_streaming_table_summary,
    get_streaming_table_path,
    get_streaming_table_schema
)

from .streaming_utils import (
    check_prerequisites,
    run_streaming_job,
    monitor_spark_process,
    build_spark_submit_command
)

__all__ = [
    # Streaming table utilities
    'ensure_streaming_table_exists',
    'check_streaming_table_exists',
    'get_streaming_table_info',
    'print_streaming_table_summary',
    'get_streaming_table_path',
    'get_streaming_table_schema',
    
    # Streaming job utilities
    'check_prerequisites',
    'run_streaming_job',
    'monitor_spark_process',
    'build_spark_submit_command'
]