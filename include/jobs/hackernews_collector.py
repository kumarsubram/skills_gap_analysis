"""
HackerNews Raw API Collector

Collects RAW API responses from HackerNews "Who's Hiring" threads.
NO PARSING - just stores raw API data.
"""

import requests
import time
import json
from typing import List, Dict, Any, Optional
from datetime import datetime, timezone

HN_API_BASE = "https://hacker-news.firebaseio.com/v0"


def collect_hackernews_jobs(date_str: str) -> List[Dict[str, Any]]:
    """Collect RAW HackerNews API responses"""
    print("   🎯 Starting HackerNews RAW API collection...")
    
    raw_responses = []
    
    # Step 1: Get user data
    user_data = _get_user_data(raw_responses, date_str)
    if not user_data:
        return raw_responses
    
    # Step 2: Find hiring thread
    hiring_thread_data = _find_hiring_thread(user_data, raw_responses, date_str)
    if not hiring_thread_data:
        return raw_responses
    
    # Step 3: Collect all job comments
    _collect_job_comments(hiring_thread_data, raw_responses, date_str)
    
    print(f"   ✅ Collected {len(raw_responses)} raw API responses")
    return raw_responses


def _get_user_data(raw_responses: List[Dict], date_str: str) -> Optional[Dict]:
    """Get whoishiring user data"""
    try:
        user_url = f"{HN_API_BASE}/user/whoishiring.json"
        user_response = requests.get(user_url, timeout=10)
        
        response_data = None
        error_text = None
        
        if user_response.status_code == 200:
            try:
                response_data = user_response.json()
            except json.JSONDecodeError:
                error_text = "Invalid JSON response"
        else:
            error_text = user_response.text[:500]
        
        raw_responses.append({
            'api_type': 'user_data',
            'url': user_url,
            'status_code': user_response.status_code,
            'response_data': response_data,
            'error_text': error_text,
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
        
        return response_data if user_response.status_code == 200 else None
        
    except requests.exceptions.RequestException as e:
        raw_responses.append({
            'api_type': 'user_data',
            'url': f"{HN_API_BASE}/user/whoishiring.json",
            'status_code': None,
            'response_data': None,
            'error_text': f"Request error: {str(e)[:200]}",
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
        return None


def _find_hiring_thread(user_data: Dict, raw_responses: List[Dict], date_str: str) -> Optional[Dict]:
    """Find the current hiring thread"""
    submitted_items = user_data.get('submitted', [])
    
    for item_id in submitted_items[:18]:  # Check recent submissions
        try:
            item_url = f"{HN_API_BASE}/item/{item_id}.json"
            item_response = requests.get(item_url, timeout=10)
            
            response_data = None
            error_text = None
            
            if item_response.status_code == 200:
                try:
                    response_data = item_response.json()
                except json.JSONDecodeError:
                    error_text = "Invalid JSON response"
            else:
                error_text = item_response.text[:500]
            
            raw_responses.append({
                'api_type': 'thread_candidate',
                'item_id': item_id,
                'url': item_url,
                'status_code': item_response.status_code,
                'response_data': response_data,
                'error_text': error_text,
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
            
            # Check if this is the hiring thread
            if response_data and item_response.status_code == 200:
                title = response_data.get('title', '').lower()
                if 'who is hiring' in title and response_data.get('type') == 'story':
                    return response_data
                    
        except requests.exceptions.RequestException as e:
            raw_responses.append({
                'api_type': 'thread_candidate',
                'item_id': item_id,
                'url': f"{HN_API_BASE}/item/{item_id}.json",
                'status_code': None,
                'response_data': None,
                'error_text': f"Request error: {str(e)[:200]}",
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
    
    return None


def _collect_job_comments(hiring_thread_data: Dict, raw_responses: List[Dict], date_str: str) -> None:
    """Collect all job comments from the hiring thread"""
    kids = hiring_thread_data.get('kids', [])
    
    for i, comment_id in enumerate(kids):
        try:
            if i % 25 == 0 and i > 0:
                print(f"   📈 Progress: {i}/{len(kids)} comments")
            
            comment_url = f"{HN_API_BASE}/item/{comment_id}.json"
            comment_response = requests.get(comment_url, timeout=10)
            
            response_data = None
            error_text = None
            
            if comment_response.status_code == 200:
                try:
                    response_data = comment_response.json()
                except json.JSONDecodeError:
                    error_text = "Invalid JSON response"
            else:
                error_text = comment_response.text[:500]
            
            raw_responses.append({
                'api_type': 'job_comment',
                'comment_id': comment_id,
                'url': comment_url,
                'status_code': comment_response.status_code,
                'response_data': response_data,
                'error_text': error_text,
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
            
            # Rate limiting every 10 requests
            if (i + 1) % 10 == 0:
                time.sleep(2)
                
        except requests.exceptions.RequestException as e:
            raw_responses.append({
                'api_type': 'job_comment',
                'comment_id': comment_id,
                'url': f"{HN_API_BASE}/item/{comment_id}.json",
                'status_code': None,
                'response_data': None,
                'error_text': f"Request error: {str(e)[:200]}",
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })