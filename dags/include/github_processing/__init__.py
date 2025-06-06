"""
GitHub Processing Module

This module provides functionality for processing GitHub Archive data,
extracting technology trends, and uploading results to Azure Data Lake.

Components:
- data_collector: Download and cache GitHub Archive files
- keyword_processor: Extract technology keywords and generate trends
- azure_uploader: Upload processed data to Azure Silver layer
- core_processor: Core processing and date range utilities
- sequential_tasks: Sequential DAG task implementations
"""

from include.github_processing.data_collector import (
    is_date_already_processed,
    download_github_hour,
    ensure_output_directory,
    get_file_size_mb,
    cleanup_temp_files
)

from include.github_processing.keyword_processor import (
    load_keywords,
    create_keyword_patterns,
    extract_trend_counts_from_events,
    save_trend_summary,
    get_hourly_trend_stats
)

from include.github_processing.azure_uploader import (
    upload_date_to_azure_silver,
    upload_date_batch_to_azure,
    is_date_uploaded_to_azure,
    verify_azure_silver_structure,
    get_azure_silver_stats
)

from include.github_processing.core_processor import (
    process_single_date,
    determine_date_range
)

# Task functions are now in the DAG file to avoid circular imports

__version__ = "2.0.0"
__author__ = "Data Engineering Team"