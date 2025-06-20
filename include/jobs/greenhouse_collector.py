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
    try:
        with open("/opt/airflow/include/jsons/greenhouse_companies.json", "r", encoding="utf-8") as f:
            companies = json.load(f)
    except FileNotFoundError:
        print("   ❌ greenhouse_companies.json not found")
        return []
    except json.JSONDecodeError as e:
        print(f"   ❌ Error parsing greenhouse_companies.json: {e}")
        return []
    
    print(f"   📋 Loaded {len(companies)} companies to process")
    
    raw_responses = []
    successful_companies = 0
    failed_companies = 0
    
    for i, company in enumerate(companies):
        try:
            # Progress every 10 companies
            if i % 10 == 0 and i > 0:
                print(f"   📈 Progress: {i}/{len(companies)} companies, {successful_companies} successful")
            
            url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
            response = requests.get(url, timeout=15)
            
            # Store raw response
            response_data = None
            error_text = None
            
            if response.status_code == 200:
                try:
                    response_data = response.json()
                    successful_companies += 1
                except json.JSONDecodeError:
                    error_text = "Invalid JSON response"
                    failed_companies += 1
            else:
                error_text = response.text[:500]  # Limit error text length
                failed_companies += 1
            
            raw_responses.append({
                'api_type': 'company_jobs',
                'company': company,
                'url': url,
                'status_code': response.status_code,
                'response_data': response_data,
                'error_text': error_text,
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
            
            # Rate limiting
            time.sleep(1)
            
        except requests.exceptions.Timeout:
            print(f"   ⚠️  Timeout for company: {company}")
            raw_responses.append({
                'api_type': 'company_jobs',
                'company': company,
                'url': f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs",
                'status_code': None,
                'response_data': None,
                'error_text': 'Request timeout',
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
            failed_companies += 1
            
        except requests.exceptions.RequestException as e:
            print(f"   ⚠️  Request error for company {company}: {e}")
            raw_responses.append({
                'api_type': 'company_jobs',
                'company': company,
                'url': f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs",
                'status_code': None,
                'response_data': None,
                'error_text': f"Request error: {str(e)[:200]}",
                'collected_at': datetime.now(timezone.utc).isoformat(),
                'collection_date': date_str
            })
            failed_companies += 1
    
    success_rate = (successful_companies / len(companies) * 100) if companies else 0
    print(f"   ✅ Collected {len(raw_responses)} raw API responses")
    print(f"   📊 Success rate: {successful_companies}/{len(companies)} companies ({success_rate:.1f}%)")
    
    return raw_responses