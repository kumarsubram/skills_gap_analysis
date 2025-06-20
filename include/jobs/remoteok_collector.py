"""
RemoteOK Raw API Collector

Collects RAW API responses from RemoteOK.
NO PARSING - just stores raw API data.
"""

import requests
import time
import json
from typing import List, Dict, Any
from datetime import datetime, timezone


def collect_remoteok_jobs(date_str: str) -> List[Dict[str, Any]]:
    """Collect RAW RemoteOK API responses"""
    print("   🎯 Starting RemoteOK RAW API collection...")
    
    raw_responses = []
    api_url = "https://remoteok.com/api"
    
    # Set user agent to avoid blocking
    headers = {
        'User-Agent': 'Mozilla/5.0 (compatible; JobsBot/1.0)',
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip, deflate'
    }
    
    try:
        print(f"   📡 Fetching from: {api_url}")
        response = requests.get(api_url, headers=headers, timeout=30)
        
        response_data = None
        error_text = None
        job_count = 0
        
        if response.status_code == 200:
            try:
                response_data = response.json()
                job_count = len(response_data) if isinstance(response_data, list) else 0
                print(f"   ✅ Successfully collected {job_count} jobs from RemoteOK")
            except json.JSONDecodeError:
                error_text = "Invalid JSON response"
                print("   ❌ Invalid JSON response from RemoteOK")
        else:
            error_text = response.text[:500]  # Limit error text length
            print(f"   ⚠️  API returned status {response.status_code}")
        
        raw_responses.append({
            'api_type': 'job_listings',
            'url': api_url,
            'status_code': response.status_code,
            'response_data': response_data,
            'error_text': error_text,
            'job_count': job_count,
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
        
        # Rate limiting - be respectful to RemoteOK
        time.sleep(1)
        
    except requests.exceptions.Timeout:
        print("   ⚠️  Request timeout for RemoteOK API")
        raw_responses.append({
            'api_type': 'job_listings',
            'url': api_url,
            'status_code': None,
            'response_data': None,
            'error_text': 'Request timeout (30s)',
            'job_count': 0,
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
        
    except requests.exceptions.RequestException as e:
        print(f"   ⚠️  Request error for RemoteOK: {e}")
        raw_responses.append({
            'api_type': 'job_listings',
            'url': api_url,
            'status_code': None,
            'response_data': None,
            'error_text': f"Request error: {str(e)[:200]}",
            'job_count': 0,
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
    
    print(f"   ✅ Collected {len(raw_responses)} raw API responses")
    return raw_responses