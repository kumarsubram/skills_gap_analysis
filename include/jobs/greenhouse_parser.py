"""
Greenhouse Parser

Parses raw Greenhouse API data from Bronze into structured job data.
Handles only Greenhouse-specific parsing logic.

Location: include/jobs/greenhouse_parser.py
"""

import html
import re
from typing import Dict, List

# Import shared location classification function
from include.jobs.job_common_utils import classify_location

def clean_html_text(text: str) -> str:
    """Clean and decode HTML text"""
    if not text:
        return ""
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

def parse_greenhouse_raw_to_jobs(raw_responses: List[Dict], location_data: Dict) -> List[Dict]:
    """Parse raw Greenhouse API responses into structured job data"""
    jobs = []
    
    # Filter for successful company responses
    company_responses = [
        r for r in raw_responses 
        if r.get('api_type') == 'company_jobs' 
        and r.get('status_code') == 200 
        and r.get('response_data')
    ]
    
    print(f"   📋 Parsing {len(company_responses)} Greenhouse company responses...")
    
    for response in company_responses:
        try:
            company = response['company']
            data = response['response_data']
            jobs_data = data.get('jobs', [])
            
            for job_data in jobs_data:
                location = job_data.get('location', {})
                location_name = location.get('name', '') if location else ''
                
                # Clean description
                content = job_data.get('content', '')
                clean_content = clean_html_text(content)
                
                job = {
                    'job_id': f"greenhouse_{company}_{job_data.get('id', '')}",
                    'source': 'greenhouse',
                    'title': job_data.get('title', ''),
                    'company': company.replace('-', ' ').title(),
                    'location': location_name,
                    'location_type': classify_location(location_name, location_data),
                    'url': job_data.get('absolute_url', ''),
                    'description': clean_content[:1000],
                    'collected_date': response.get('collection_date', '')
                }
                
                jobs.append(job)
                
        except Exception as e:
            print(f"   ⚠️  Error parsing company {response.get('company', 'unknown')}: {e}")
            continue
    
    print(f"   ✅ Parsed {len(jobs)} Greenhouse jobs")
    return jobs