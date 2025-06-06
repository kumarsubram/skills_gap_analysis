"""
Core Processing Functions

Main processing logic for GitHub trends extraction.
Contains single-date processing and date range utilities.
"""

import time
from datetime import datetime, timedelta, timezone
from collections import defaultdict, Counter
from typing import List, Dict

from include.github_processing.data_collector import (
    is_date_already_processed,
    download_github_hour,
)
from include.github_processing.keyword_processor import (
    load_keywords,
    create_keyword_patterns,
    extract_trend_counts_from_events,
    save_trend_summary
)


def determine_date_range(conf: Dict, default_start_date: str) -> List[str]:
    """Determine which dates to process based on configuration"""
    if "date" in conf:
        # Single date mode
        dates_to_process = [conf["date"]]
        print(f"🎯 Single date mode: {conf['date']}")
        
    elif "start_date" in conf and "end_date" in conf:
        # Custom date range mode
        start_date = datetime.strptime(conf["start_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_date = datetime.strptime(conf["end_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        dates_to_process = []
        current = start_date
        while current <= end_date:
            dates_to_process.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        print(f"📅 Custom range: {conf['start_date']} to {conf['end_date']} ({len(dates_to_process)} days)")
        
    else:
        # Default: process from configured start date to yesterday
        start_date = datetime.strptime(default_start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        yesterday = datetime.now(timezone.utc) - timedelta(days=1)
        dates_to_process = []
        current = start_date
        while current <= yesterday:
            dates_to_process.append(current.strftime("%Y-%m-%d"))
            current += timedelta(days=1)
        print(f"🚀 Default mode: {default_start_date} to yesterday ({len(dates_to_process)} days)")
    
    return dates_to_process


def process_single_date(date_str: str, keywords: Dict) -> bool:
    """Process GitHub Archive data for a single date - optimized for trends"""
    print(f"\n📅 Processing {date_str}")
    print("=" * 50)
    
    # Check if already processed
    if is_date_already_processed(date_str):
        return True
    
    # Pre-compile regex patterns once per date
    print("🔧 Pre-compiling keyword patterns...")
    keyword_patterns = create_keyword_patterns(keywords)
    print(f"✅ Compiled {len(keyword_patterns)} patterns")
    
    # Aggregate counters for the entire day
    all_keyword_counts = Counter()
    all_repo_counts = defaultdict(Counter)
    all_event_counts = Counter()
    
    # Track hourly counts for timeseries
    hourly_counts = defaultdict(Counter)
    
    downloaded_count = 0
    cached_count = 0
    failed_count = 0
    
    # Process each hour of the day
    for hour in range(24):
        file_path, was_cached = download_github_hour(date_str, hour)
        
        if file_path is None:
            failed_count += 1
            continue
            
        if was_cached:
            cached_count += 1
        else:
            downloaded_count += 1
        
        # Extract trend counts from this hour
        hour_keyword_counts, hour_repo_counts, hour_event_counts = extract_trend_counts_from_events(
            file_path, keyword_patterns, date_str, hour
        )
        
        # Store hourly counts for timeseries
        hourly_counts[hour] = hour_keyword_counts
        
        # Aggregate into daily totals
        all_keyword_counts.update(hour_keyword_counts)
        all_event_counts.update(hour_event_counts)
        
        for repo, keyword_counts in hour_repo_counts.items():
            all_repo_counts[repo].update(keyword_counts)
    
    # Save aggregated trend summary
    if sum(all_keyword_counts.values()) > 0:
        save_trend_summary(all_keyword_counts, all_repo_counts, all_event_counts, date_str, hourly_counts)
        total_mentions = sum(all_keyword_counts.values())
        print(f"🎯 Total keyword mentions: {total_mentions:,}")
        print(f"🔥 Top trends: {dict(all_keyword_counts.most_common(10))}")
    else:
        # Still create empty files to mark as processed
        save_trend_summary(Counter(), defaultdict(Counter), Counter(), date_str, defaultdict(Counter))
        print(f"⚠️  No keyword matches found for {date_str}")
    
    print(f"📊 Files: {downloaded_count} downloaded, {cached_count} cached, {failed_count} failed")
    print(f"📁 Created 4 summary files for {date_str} (including hourly trends)")
    return sum(all_keyword_counts.values()) > 0