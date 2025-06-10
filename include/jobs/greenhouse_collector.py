"""
Greenhouse Raw API Collector

Collects RAW API responses from Greenhouse companies.
NO PARSING - just stores raw API data.
"""

import requests
import time
import json
from typing import List, Dict, Any
from datetime import datetime, timezone

def collect_greenhouse_jobs(date_str: str) -> List[Dict[str, Any]]:
    """Collect RAW Greenhouse API responses"""
    print("   🎯 Starting Greenhouse RAW API collection...")
    
    # Load companies
    with open("/usr/local/airflow/include/jsons/greenhouse_companies.json", "r") as f:
        companies = json.load(f)
    
    print(f"   📋 Loaded {len(companies)} companies to process")
    
    raw_responses = []
    successful_companies = 0
    failed_companies = 0
    
    for i, company in enumerate(companies):
        try:
            # Progress every 10 companies (keeps same frequency as before)
            if i % 10 == 0 and i > 0:
                print(f"   📈 Progress: {i}/{len(companies)} companies, {successful_companies} successful")
            
            url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
            response = requests.get(url, timeout=15)
            
            raw_responses.append({
                'api_type': 'company_jobs',
                'company': company,
                'url': url,
                'status_code': response.status_code,
                'response_data': response.json() if response.status_code == 200 else None,
                'error_text': response.text if response.status_code != 200 else None,
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
            
            # Count success/failure for progress tracking
            if response.status_code == 200:
                successful_companies += 1
            else:
                failed_companies += 1
            
            # Same rate limiting as before
            time.sleep(1)
            
        except Exception as e:
            raw_responses.append({
                'api_type': 'company_jobs',
                'company': company,
                'url': f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs",
                'status_code': None,
                'response_data': None,
                'error_text': str(e),
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
            failed_companies += 1
            continue
    
    print(f"   ✅ Collected {len(raw_responses)} raw API responses")
    print(f"   📊 Success rate: {successful_companies}/{len(companies)} companies ({(successful_companies/len(companies)*100):.1f}%)")
    return raw_responses