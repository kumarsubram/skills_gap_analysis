"""
Streaming utilities module

Provides utilities for managing real-time streaming data pipelines, 
including enhanced Delta table management, aggregation tables, and Kafka integration.
"""

from .streaming_table_utils import (
    ensure_streaming_table_exists,
    check_streaming_table_exists,
    get_streaming_table_info,
    print_streaming_table_summary,
    get_streaming_table_path,
    get_streaming_table_schema
)

# FIXED: Updated to use the correct function names from streaming_aggregation_table_utils
from .streaming_aggregation_table_utils import (
    ensure_streaming_aggregation_table_exists,
    check_streaming_aggregation_table_exists,
    get_streaming_aggregation_table_info,
    print_streaming_aggregation_table_summary,
    get_streaming_aggregation_table_path,
    get_streaming_aggregation_schema
)

from .streaming_utils import (
    check_prerequisites,
    run_streaming_job,
    monitor_spark_process,
    build_spark_submit_command
)

__all__ = [
    # Raw streaming table utilities
    'ensure_streaming_table_exists',
    'check_streaming_table_exists',
    'get_streaming_table_info',
    'print_streaming_table_summary',
    'get_streaming_table_path',
    'get_streaming_table_schema',
    
    # FIXED: Updated aggregation table utility names
    'ensure_streaming_aggregation_table_exists',
    'check_streaming_aggregation_table_exists',
    'get_streaming_aggregation_table_info',
    'print_streaming_aggregation_table_summary',
    'get_streaming_aggregation_table_path',
    'get_streaming_aggregation_schema',
    
    # Streaming job utilities
    'check_prerequisites',
    'run_streaming_job',
    'monitor_spark_process',
    'build_spark_submit_command'
]