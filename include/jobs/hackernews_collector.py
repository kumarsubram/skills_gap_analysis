"""
HackerNews Raw API Collector

Collects RAW API responses from HackerNews "Who's Hiring" threads.
NO PARSING - just stores raw API data.
"""

import requests
import time
from typing import List, Dict, Any
from datetime import datetime, timezone

HN_API_BASE = "https://hacker-news.firebaseio.com/v0"

def collect_hackernews_jobs(date_str: str) -> List[Dict[str, Any]]:
    """Collect RAW HackerNews API responses"""
    print("   🎯 Starting HackerNews RAW API collection...")
    
    raw_responses = []
    
    # Get user data
    try:
        user_url = f"{HN_API_BASE}/user/whoishiring.json"
        user_response = requests.get(user_url, timeout=10)
        
        raw_responses.append({
            'api_type': 'user_data',
            'url': user_url,
            'status_code': user_response.status_code,
            'response_data': user_response.json() if user_response.status_code == 200 else None,
            'error_text': user_response.text if user_response.status_code != 200 else None,
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
        
        if user_response.status_code != 200:
            return raw_responses
            
        user_data = user_response.json()
        submitted_items = user_data.get('submitted', [])
        
    except Exception as e:
        raw_responses.append({
            'api_type': 'user_data',
            'url': f"{HN_API_BASE}/user/whoishiring.json",
            'status_code': None,
            'response_data': None,
            'error_text': str(e),
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
        return raw_responses
    
    # Find hiring thread
    hiring_thread_data = None
    for item_id in submitted_items[:18]:
        try:
            item_url = f"{HN_API_BASE}/item/{item_id}.json"
            item_response = requests.get(item_url, timeout=10)
            
            raw_responses.append({
                'api_type': 'thread_candidate',
                'item_id': item_id,
                'url': item_url,
                'status_code': item_response.status_code,
                'response_data': item_response.json() if item_response.status_code == 200 else None,
                'error_text': item_response.text if item_response.status_code != 200 else None,
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
            
            if item_response.status_code == 200:
                item_data = item_response.json()
                title = item_data.get('title', '').lower()
                if 'who is hiring' in title and item_data.get('type') == 'story':
                    hiring_thread_data = item_data
                    break
                    
        except Exception as e:
            raw_responses.append({
                'api_type': 'thread_candidate',
                'item_id': item_id,
                'url': f"{HN_API_BASE}/item/{item_id}.json",
                'status_code': None,
                'response_data': None,
                'error_text': str(e),
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
            continue
    
    if not hiring_thread_data:
        return raw_responses
    
    # Collect all comments
    kids = hiring_thread_data.get('kids', [])
    for i, comment_id in enumerate(kids):
        try:
            if i % 25 == 0 and i > 0:
                print(f"   📈 Progress: {i}/{len(kids)} comments")
            
            comment_url = f"{HN_API_BASE}/item/{comment_id}.json"
            comment_response = requests.get(comment_url, timeout=10)
            
            raw_responses.append({
                'api_type': 'job_comment',
                'comment_id': comment_id,
                'url': comment_url,
                'status_code': comment_response.status_code,
                'response_data': comment_response.json() if comment_response.status_code == 200 else None,
                'error_text': comment_response.text if comment_response.status_code != 200 else None,
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
            
            if (i + 1) % 10 == 0:
                time.sleep(2)
                
        except Exception as e:
            raw_responses.append({
                'api_type': 'job_comment',
                'comment_id': comment_id,
                'url': f"{HN_API_BASE}/item/{comment_id}.json",
                'status_code': None,
                'response_data': None,
                'error_text': str(e),
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
            continue
    
    print(f"   ✅ Collected {len(raw_responses)} raw API responses")
    return raw_responses