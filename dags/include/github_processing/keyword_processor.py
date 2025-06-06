"""
Keyword Processing Module

Handles keyword loading, pattern compilation, event processing,
and trend summary generation for GitHub Archive data.
"""

import os
import json
import re
import csv
import gzip
from collections import defaultdict, Counter


def load_keywords():
    """Load keywords from configuration file"""
    # Fixed path for modular structure
    keyword_path = "/usr/local/airflow/include/config/keywords.json"
    print(f"🔍 Loading keywords from: {keyword_path}")
    
    with open(keyword_path, "r", encoding="utf-8") as f:
        keywords = json.load(f)
    
    print(f"📋 Loaded {len(keywords)} keywords")
    return keywords


def create_keyword_patterns(keywords):
    """Pre-compile regex patterns for better performance"""
    patterns = {}
    for keyword in keywords:
        # Create word boundary pattern for each keyword
        patterns[keyword] = re.compile(rf"\b{re.escape(keyword.lower())}\b", re.IGNORECASE)
    return patterns


def extract_trend_counts_from_events(file_path, keyword_patterns, date_str, hour):
    """Extract keyword trend counts from GitHub events - OPTIMIZED VERSION"""
    
    # Use counters for aggregation instead of storing individual matches
    keyword_counts = Counter()
    repo_keyword_counts = defaultdict(Counter)
    event_type_counts = Counter()
    
    try:
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            events_processed = 0
            relevant_events = 0
            
            for line in f:
                if not line.strip():
                    continue
                    
                events_processed += 1
                
                # Parse event
                try:
                    event = json.loads(line)
                except:
                    continue
                
                event_type = event.get("type")
                
                # Only process PushEvent and PullRequestEvent
                if event_type not in ["PushEvent", "PullRequestEvent"]:
                    continue
                    
                relevant_events += 1
                repo = event.get("repo", {}).get("name", "unknown")
                
                # Collect messages to search - but don't store them
                messages = []
                if event_type == "PushEvent":
                    commits = event.get("payload", {}).get("commits", [])
                    for commit in commits:
                        if commit.get("message"):
                            messages.append(commit["message"].lower())
                
                elif event_type == "PullRequestEvent":
                    pr = event.get("payload", {}).get("pull_request", {})
                    if pr.get("title"):
                        messages.append(pr["title"].lower())
                    if pr.get("body"):
                        # Only take first 500 chars of body for performance
                        body = pr.get("body", "")[:500].lower()
                        if body:
                            messages.append(body)
                
                # 🚀 PERFORMANCE FIX: Search for keywords in messages - MUCH FASTER
                found_keywords = set()  # Track found keywords to avoid double counting
                
                for message in messages:
                    # Search all patterns against this message once
                    for keyword, pattern in keyword_patterns.items():
                        if keyword not in found_keywords and pattern.search(message):
                            found_keywords.add(keyword)
                            keyword_counts[keyword] += 1
                            repo_keyword_counts[repo][keyword] += 1
                            event_type_counts[event_type] += 1
                    
                    # Early exit if we found many keywords in this message
                    if len(found_keywords) > 10:  # Reasonable limit
                        break
        
        total_matches = sum(keyword_counts.values())
        print(f"      ✅ Hour {hour:02d}: {total_matches} matches from {relevant_events:,}/{events_processed:,} events")
        
        return keyword_counts, repo_keyword_counts, event_type_counts
        
    except Exception as e:
        print(f"      ❌ Error processing file: {e}")
        return Counter(), defaultdict(Counter), Counter()


def save_trend_summary(all_keyword_counts, all_repo_counts, all_event_counts, date_str, hourly_counts=None):
    """Save aggregated trend data to compact CSV files"""
    output_dir = f"/usr/local/airflow/data/output/{date_str}"
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Keyword summary (main trends file)
    keyword_file = os.path.join(output_dir, f"tech_trends_summary_{date_str}.csv")
    with open(keyword_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "keyword", "mentions", "type", "language", "framework"])
        
        keywords_config = load_keywords()  # Get metadata
        for keyword, count in all_keyword_counts.most_common():
            meta = keywords_config.get(keyword, {})
            writer.writerow([
                date_str,
                keyword,
                count,
                meta.get("type", ""),
                meta.get("language", ""),
                meta.get("framework", "")
            ])
    
    # 2. Top repos per keyword (compact)
    repo_file = os.path.join(output_dir, f"top_repos_{date_str}.csv")
    with open(repo_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "keyword", "repo", "mentions"])
        
        for repo, keyword_counts in all_repo_counts.items():
            for keyword, count in keyword_counts.most_common(5):  # Top 5 keywords per repo
                if count >= 2:  # Only include if mentioned at least twice
                    writer.writerow([date_str, keyword, repo, count])
    
    # 3. Event type summary
    event_file = os.path.join(output_dir, f"event_summary_{date_str}.csv")
    with open(event_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "event_type", "keyword_mentions"])
        
        for event_type, count in all_event_counts.items():
            writer.writerow([date_str, event_type, count])
    
    # 4. NEW: Hourly trends file for timeseries analysis
    hourly_file = os.path.join(output_dir, f"hourly_trends_{date_str}.csv")
    if hourly_counts:
        with open(hourly_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "keyword", "mentions"])
            
            for hour in range(24):
                timestamp = f"{date_str} {hour:02d}:00:00"  # Format: "2025-01-15 14:00:00"
                for keyword, count in hourly_counts[hour].items():
                    if count > 0:
                        writer.writerow([timestamp, keyword, count])
        
        hourly_records = sum(len(hourly_counts[h]) for h in range(24))
        print(f"   📊 {hourly_file} ({os.path.getsize(hourly_file)} bytes, {hourly_records} records)")
    else:
        # Create empty file if no hourly data
        with open(hourly_file, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["timestamp", "keyword", "mentions"])
    
    total_matches = sum(all_keyword_counts.values())
    total_keywords = len(all_keyword_counts)
    total_repos = len(all_repo_counts)
    
    print("💾 Saved trend summary:")
    print(f"   📊 {total_matches:,} total mentions of {total_keywords} keywords across {total_repos} repos")
    print(f"   📄 {keyword_file} ({os.path.getsize(keyword_file)} bytes)")
    print(f"   📄 {repo_file} ({os.path.getsize(repo_file)} bytes)")
    print(f"   📄 {event_file} ({os.path.getsize(event_file)} bytes)")
    
    return keyword_file


def get_hourly_trend_stats(date_str):
    """Get statistics about hourly trends for a processed date"""
    hourly_file = f"/usr/local/airflow/data/output/{date_str}/hourly_trends_{date_str}.csv"
    
    if not os.path.exists(hourly_file):
        return None
    
    try:
        with open(hourly_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            records = list(reader)
            
        return {
            "total_records": len(records),
            "unique_keywords": len(set(r["keyword"] for r in records if r["keyword"])),
            "total_mentions": sum(int(r["mentions"]) for r in records if r["mentions"]),
            "file_size_mb": os.path.getsize(hourly_file) / (1024 * 1024)
        }
    except Exception as e:
        print(f"⚠️  Error reading hourly trends stats: {e}")
        return None