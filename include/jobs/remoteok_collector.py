"""
RemoteOK Raw API Collector

Collects RAW API responses from RemoteOK.
NO PARSING - just stores raw API data.

Location: include/jobs/remoteok_collector.py
"""

import requests
import time
from typing import List, Dict, Any
from datetime import datetime, timezone

def collect_remoteok_jobs(date_str: str) -> List[Dict[str, Any]]:
    """Collect RAW RemoteOK API responses"""
    print("   🎯 Starting RemoteOK RAW API collection...")
    
    raw_responses = []
    
    try:
        # RemoteOK API endpoint - returns JSON array of jobs
        api_url = "https://remoteok.com/api"
        
        # Set user agent to avoid blocking
        headers = {
            'User-Agent': 'Mozilla/5.0 (compatible; JobsBot/1.0)'
        }
        
        print(f"   📡 Fetching from: {api_url}")
        response = requests.get(api_url, headers=headers, timeout=30)
        
        raw_responses.append({
            'api_type': 'job_listings',
            'url': api_url,
            'status_code': response.status_code,
            'response_data': response.json() if response.status_code == 200 else None,
            'error_text': response.text if response.status_code != 200 else None,
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
        
        if response.status_code == 200:
            data = response.json()
            job_count = len(data) if isinstance(data, list) else 0
            print(f"   ✅ Successfully collected {job_count} jobs from RemoteOK")
        else:
            print(f"   ⚠️  API returned status {response.status_code}")
            
        # Rate limiting - be respectful to RemoteOK
        time.sleep(1)
        
    except requests.exceptions.Timeout:
        print("   ⚠️  Request timeout for RemoteOK API")
        raw_responses.append({
            'api_type': 'job_listings',
            'url': api_url,
            'status_code': None,
            'response_data': None,
            'error_text': 'Request timeout',
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
            'error_text': str(e),
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
        
    except Exception as e:
        print(f"   ❌ Unexpected error collecting RemoteOK: {e}")
        raw_responses.append({
            'api_type': 'job_listings',
            'url': api_url,
            'status_code': None,
            'response_data': None,
            'error_text': str(e),
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
    
    print(f"   ✅ Collected {len(raw_responses)} raw API responses")
    return raw_responses