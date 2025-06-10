"""
HackerNews Parser

Parses raw HackerNews API data from Bronze into structured job data.
Handles only HackerNews-specific parsing logic.

Location: include/jobs/hackernews_parser.py
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

def parse_hackernews_raw_to_jobs(raw_responses: List[Dict], location_data: Dict) -> List[Dict]:
    """Parse raw HackerNews API responses into structured job data"""
    jobs = []
    
    # Filter for successful job comment responses
    job_comments = [
        r for r in raw_responses 
        if r.get('api_type') == 'job_comment' 
        and r.get('status_code') == 200 
        and r.get('response_data')
    ]
    
    print(f"   📋 Parsing {len(job_comments)} HackerNews comments...")
    
    for response in job_comments:
        try:
            comment_data = response['response_data']
            comment_id = response.get('comment_id')
            
            if comment_data.get('deleted') or not comment_data.get('text'):
                continue
                
            raw_text = comment_data.get('text', '')
            if len(raw_text) < 100:
                continue
            
            text = clean_html_text(raw_text)
            
            # Extract company
            company_patterns = [
                r'^([A-Z][A-Za-z\s&.,-]+?)(?:\s*[-|:]|\s+is\s+|\s+\()',
                r'(?:^|\n)([A-Z][A-Za-z\s&.,-]+?)\s*[-:]?\s*(?:is\s+)?(?:hiring|seeking|looking)',
            ]
            
            company = "Unknown"
            for pattern in company_patterns:
                company_match = re.search(pattern, text, re.MULTILINE)
                if company_match:
                    potential_company = company_match.group(1).strip()
                    if len(potential_company) > 2 and not re.match(r'^(?:We|Our|The|A|An)\s', potential_company):
                        company = potential_company
                        break
            
            # Extract location
            location_patterns = [
                r'(?:Location|Based in|Office in|Located):\s*([^.\n|]+?)(?:\s*[|.]|$)',
                r'(?:^|\n)\s*Location:\s*([^.\n|]+?)(?:\s*[|.]|$)',
                r'(?:Remote|Onsite|On-site|Hybrid)(?:\s*[-:]?\s*([^.\n|]+?))?(?:\s*[|.]|$)',
                r'(?:^|\n)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2,3})\s*(?:[|.]|$)',
            ]
            
            location = "Not specified"
            for pattern in location_patterns:
                location_match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if location_match:
                    potential_location = location_match.group(1) if location_match.group(1) else location_match.group(0)
                    potential_location = potential_location.strip()
                    
                    if potential_location and len(potential_location) < 100:
                        job_title_indicators = ['engineer', 'developer', 'manager', 'analyst', 'director', 'senior', 'junior', 'lead']
                        if not any(indicator in potential_location.lower() for indicator in job_title_indicators):
                            location = potential_location
                            break
            
            # Extract title
            title_patterns = [
                r'(?:hiring|seeking|looking for)[\s:]*([^.\n|]+?)(?:\s*[|.]|$)',
                r'(?:Position|Role|Job title)[\s:]*([^.\n|]+?)(?:\s*[|.]|$)',
                r'(?:^|\n)\s*([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Engineer|Developer|Manager|Analyst|Director|Designer|Architect))\s*(?:[|.]|$)',
            ]
            
            title = "Software Engineer"
            for pattern in title_patterns:
                title_match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
                if title_match:
                    potential_title = title_match.group(1).strip()
                    if potential_title and len(potential_title) < 100:
                        title = potential_title
                        break
            
            job = {
                'job_id': f"hn_{comment_id}",
                'source': 'hackernews',
                'title': title[:100],
                'company': company[:100],
                'location': location[:100],
                'location_type': classify_location(location, location_data),
                'url': f"https://news.ycombinator.com/item?id={comment_id}",
                'description': text[:1000],
                'collected_date': response.get('collection_date', '')
            }
            
            jobs.append(job)
            
        except Exception as e:
            print(f"   ⚠️  Error parsing comment {response.get('comment_id', 'unknown')}: {e}")
            continue
    
    print(f"   ✅ Parsed {len(jobs)} HackerNews jobs")
    return jobs