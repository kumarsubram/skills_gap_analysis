"""
Streaming utilities module

Provides utilities for managing real-time streaming data pipelines,
including enhanced Delta table management and Kafka integration.
"""

from .streaming_table_utils import (
    ensure_streaming_table_exists,
    check_streaming_table_exists,
    get_streaming_table_info,
    print_streaming_table_summary,
    get_streaming_table_path,
    get_streaming_table_schema
)

__all__ = [
    'ensure_streaming_table_exists',
    'check_streaming_table_exists', 
    'get_streaming_table_info',
    'print_streaming_table_summary',
    'get_streaming_table_path',
    'get_streaming_table_schema'
]