"""
GitHub Keyword Extractor

Extracts technology trends from GitHub Archive files.
Unified Bronze layer processing for both Mac and VPS environments.
Memory-efficient processing with Parquet Bronze intermediate storage.
"""

import os
import json
import re
import gzip
import tempfile
import io
import pandas as pd
import sys
from pathlib import Path
from collections import defaultdict, Counter
from typing import Dict
from include.config.env_detection import ENV
from include.storage.file_manager import file_exists, read_binary_from_layer, save_files_to_layer

# Add project root to Python path for imports
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def load_keywords() -> Dict:
    """Load technology keywords from configuration"""
    keywords_file = ENV.config['keywords_file']
    
    try:
        with open(keywords_file, "r", encoding="utf-8") as f:
            keywords = json.load(f)
        print(f"📋 Loaded {len(keywords)} keywords from {os.path.basename(keywords_file)}")
        return keywords
    except FileNotFoundError:
        print(f"❌ Keywords file not found: {keywords_file}")
        print("Please make sure tech_keywords.json is in include/jsons/")
        return {}
    except Exception as e:
        print(f"❌ Error loading keywords: {e}")
        return {}


def create_keyword_patterns(keywords: Dict) -> Dict[str, re.Pattern]:
    """Pre-compile regex patterns for better performance"""
    patterns = {}
    for keyword in keywords:
        # Create word boundary pattern for better matching
        patterns[keyword] = re.compile(rf"\b{re.escape(keyword.lower())}\b", re.IGNORECASE)
    
    print(f"🔧 Compiled {len(patterns)} regex patterns")
    return patterns


def extract_and_save_hour(date_str: str, hour: int, keyword_patterns: Dict[str, re.Pattern]) -> bool:
    """
    Extract keywords from Bronze .gz file and save results to Bronze layer as Parquet
    Unified behavior for both Mac and VPS environments
    
    Args:
        date_str: Date in YYYY-MM-DD format
        hour: Hour (0-23)
        keyword_patterns: Pre-compiled regex patterns
    
    Returns:
        bool: True if successful, False if failed
    """
    # Output Parquet filename in Bronze layer
    output_filename = f"keywords-{date_str}-hour-{hour:02d}.parquet"
    
    # CHECK IF KEYWORD PARQUET ALREADY EXISTS - SKIP IF IT DOES
    if file_exists(output_filename, 'github', 'bronze'):
        print(f"      ✅ Hour {hour:02d}: Keywords already exist - SKIPPING")
        return True
    
    # Input .gz filename in Bronze layer
    input_filename = f"{date_str}-{hour:02d}.json.gz"
    
    # Check if .gz file exists in Bronze layer
    if not file_exists(input_filename, 'github', 'bronze'):
        print(f"      ⚠️  Hour {hour:02d}: File not found in Bronze")
        return False
    
    # Read binary data from Bronze layer (unified for Mac and VPS)
    binary_data = read_binary_from_layer(input_filename, 'github', 'bronze')
    if not binary_data:
        print(f"      ❌ Hour {hour:02d}: Failed to read from Bronze")
        return False
    
    # Initialize counters
    keyword_counts = Counter()
    repo_keyword_counts = defaultdict(Counter)
    event_type_counts = Counter()
    
    try:
        # Process gzipped data in memory using BytesIO
        with gzip.open(io.BytesIO(binary_data), "rt", encoding="utf-8") as f:
            events_processed = 0
            relevant_events = 0
            
            for line in f:
                if not line.strip():
                    continue
                    
                events_processed += 1
                
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                
                event_type = event.get("type")
                
                # Only process PushEvent and PullRequestEvent
                if event_type not in ["PushEvent", "PullRequestEvent"]:
                    continue
                    
                relevant_events += 1
                repo = event.get("repo", {}).get("name", "unknown")
                
                # Collect text content to search
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
                        body = pr.get("body", "")[:500].lower()  # Limit body text
                        if body:
                            messages.append(body)
                
                # Search for keywords in messages
                found_keywords = set()
                
                for message in messages:
                    for keyword, pattern in keyword_patterns.items():
                        if keyword not in found_keywords and pattern.search(message):
                            found_keywords.add(keyword)
                            keyword_counts[keyword] += 1
                            repo_keyword_counts[repo][keyword] += 1
                            event_type_counts[event_type] += 1
                    
                    # Limit keywords per message to avoid over-counting
                    if len(found_keywords) > 10:
                        break
        
        # Save hourly results to Bronze as Parquet
        success = _save_hourly_results_to_bronze_parquet(
            date_str, hour, keyword_counts, repo_keyword_counts, event_type_counts
        )
        
        total_matches = sum(keyword_counts.values())
        print(f"      ✅ Hour {hour:02d}: {total_matches} matches from {relevant_events:,}/{events_processed:,} events")
        
        return success
        
    except Exception as e:
        print(f"      ❌ Hour {hour:02d}: Error - {e}")
        return False


def _save_hourly_results_to_bronze_parquet(date_str: str, hour: int, keyword_counts: Counter, 
                                          repo_counts: dict, event_counts: Counter) -> bool:
    """Save hourly extraction results to Bronze layer as Parquet"""
    temp_filename = f"keywords-{date_str}-hour-{hour:02d}.parquet"
    temp_path = os.path.join(tempfile.gettempdir(), temp_filename)
    
    try:
        # Prepare data for DataFrame
        rows = []
        for keyword, mentions in keyword_counts.items():
            # Find top repo for this keyword
            top_repo = "unknown"
            repo_mentions = 0
            for repo, repo_keyword_counts in repo_counts.items():
                if keyword in repo_keyword_counts and repo_keyword_counts[keyword] > repo_mentions:
                    top_repo = repo
                    repo_mentions = repo_keyword_counts[keyword]
            
            # Event mentions for this keyword (simplified)
            event_mentions = sum(count for event_type, count in event_counts.items())
            
            rows.append({
                'hour': hour,
                'keyword': keyword,
                'mentions': mentions,
                'top_repo': top_repo,
                'repo_mentions': repo_mentions,
                'event_mentions': event_mentions
            })
        
        # Create DataFrame and save as Parquet
        if rows:
            df = pd.DataFrame(rows)
            df.to_parquet(temp_path, index=False, compression='snappy')
        else:
            # Create empty DataFrame with proper schema
            df = pd.DataFrame(columns=['hour', 'keyword', 'mentions', 'top_repo', 'repo_mentions', 'event_mentions'])
            df.to_parquet(temp_path, index=False, compression='snappy')
        
        # Save to Bronze layer (unified for Mac and VPS)
        result = save_files_to_layer([temp_path], 'github', 'bronze')
        
        # Clean up temp file
        if os.path.exists(temp_path):
            os.remove(temp_path)
        
        return result['successful'] > 0
        
    except Exception as e:
        print(f"      ❌ Failed to save hourly Parquet results: {e}")
        return False


def extract_keywords_for_date(date_str: str) -> Dict:
    """
    Extract keywords from all available hours for a date and save to Bronze as Parquet
    Unified behavior for both Mac and VPS environments
    
    Args:
        date_str: Date in YYYY-MM-DD format
    
    Returns:
        Dict with extraction statistics
    """
    print(f"\n🔍 EXTRACTING KEYWORDS FOR {date_str}")
    print("=" * 50)
    print(f"Environment: {ENV.environment.upper()}")
    print(f"Storage: {ENV.config['storage_backend']}")
    print("Output Format: Parquet (Bronze)")
    
    # Load keywords and create patterns ONCE per date
    keywords = load_keywords()
    if not keywords:
        return {
            'date': date_str,
            'successful_extractions': 0,
            'failed_extractions': 24,
            'failed_hours': list(range(24))
        }
    
    patterns = create_keyword_patterns(keywords)
    
    results = {
        'date': date_str,
        'successful_extractions': 0,
        'failed_extractions': 0,
        'failed_hours': []
    }
    
    # Process each hour with pre-compiled patterns
    for hour in range(24):
        success = extract_and_save_hour(date_str, hour, patterns)
        if success:
            results['successful_extractions'] += 1
        else:
            results['failed_extractions'] += 1
            results['failed_hours'].append(hour)
    
    print(f"\n📊 EXTRACTION SUMMARY FOR {date_str}:")
    print(f"   ✅ Successful: {results['successful_extractions']}")
    print(f"   ❌ Failed: {results['failed_extractions']}")
    
    if results['failed_hours']:
        print(f"   ⚠️  Failed hours: {results['failed_hours']}")
    
    print("=" * 50)
    return results


# Test keyword extraction
if __name__ == "__main__":
    # Test unified keyword extraction with Parquet
    test_date = "2024-12-31"
    
    print(f"🧪 Testing keyword extraction (Parquet) for {test_date}")
    print("This will process the downloaded GitHub files...")
    
    # Test environment detection
    from include.config.env_detection import print_environment_info
    print_environment_info()
    
    # Test keyword loading
    print("\n🧪 Testing keyword loading...")
    keywords = load_keywords()
    if keywords:
        sample_keywords = list(keywords.keys())[:5]
        print(f"Sample keywords: {sample_keywords}")
        
        # Test single hour extraction
        print(f"\n🧪 Testing single hour extraction for {test_date} hour 0...")
        patterns = create_keyword_patterns(keywords)
        success = extract_and_save_hour(test_date, 0, patterns)
        print(f"Single hour extraction success: {success}")
    else:
        print("❌ No keywords loaded - please check tech_keywords.json location")
    
    print("\n✅ Keyword extractor tests completed!")