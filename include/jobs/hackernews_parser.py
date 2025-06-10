"""
HackerNews Parser - CLEAN REWRITE

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

def parse_hn_structured_line(first_line: str) -> Dict[str, str]:
    """
    Parse HN's structured first line format
    Common patterns:
    - Company | URL | Title | Location | Type
    - Company | URL | Title | Type
    - Company | Title | Location
    """
    parts = [part.strip() for part in first_line.split('|') if part.strip()]
    
    result = {'company': '', 'title': '', 'location': ''}
    
    if not parts:
        return result
    
    # First part is almost always company
    company_candidate = parts[0]
    # Remove URLs if accidentally included
    company_candidate = re.sub(r'^(https?://|www\.)', '', company_candidate)
    if company_candidate and not company_candidate.lower().startswith(('looking', 'hiring', 'seeking')):
        result['company'] = company_candidate
    
    # Analyze remaining parts
    remaining_parts = parts[1:]
    
    # First pass: find the best location (prioritize parts with strong location indicators)
    best_location = ""
    best_location_score = 0
    
    # Second pass: find the best title
    best_title = ""
    best_title_score = 0
    
    for part in remaining_parts:
        part_lower = part.lower()
        
        # Skip URLs
        if part_lower.startswith(('http', 'www')):
            continue
        
        # Calculate location score
        location_score = 0
        strong_location_words = ['remote', 'hybrid', 'onsite', 'sf', 'nyc', 'seattle', 'austin', 'boston', 'usa', 'us', 'canada', 'uk', 'europe']
        city_state_pattern = re.search(r'\b[A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2,3}\b', part)
        
        for word in strong_location_words:
            if word in part_lower:
                location_score += 2
        
        if city_state_pattern:
            location_score += 3
            
        if any(word in part_lower for word in ['california', 'texas', 'new york', 'florida']):
            location_score += 2
            
        # Reduce score for employment types in location context
        if any(word in part_lower for word in ['full-time', 'part-time', 'contract']):
            location_score -= 1
            
        # Calculate title score
        title_score = 0
        title_words = ['engineer', 'developer', 'manager', 'analyst', 'director', 'designer', 'architect', 'scientist', 'specialist', 'devrel', 'platform']
        
        for word in title_words:
            if word in part_lower:
                title_score += 1
                
        # Reduce score for parts that are clearly employment types
        if part_lower.strip() in ['full-time', 'part-time', 'contract', 'remote', 'onsite', 'hybrid']:
            title_score = 0
            
        # Update best candidates
        if location_score > best_location_score:
            best_location = part
            best_location_score = location_score
            
        if title_score > best_title_score and location_score < 2:  # Don't use high-scoring locations as titles
            best_title = part
            best_title_score = title_score
    
    result['location'] = best_location
    result['title'] = best_title
    
    return result

def extract_company(text: str, structured_company: str) -> str:
    """Extract company name with fallbacks"""
    # Use structured result if available
    if structured_company:
        return structured_company
    
    # Fallback patterns
    lines = text.split('\n')
    first_line = lines[0] if lines else ""
    
    patterns = [
        r'^([A-Z][A-Za-z0-9\s&.,-]{2,50}?)(?:\s*[-|]|\s+is\s+)',
        r'([A-Z][A-Za-z0-9\s&.,-]{2,50}?)\s+(?:is\s+)?(?:hiring|seeking|looking)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, first_line)
        if match:
            company = match.group(1).strip()
            # Filter false positives
            if (company and 
                not company.lower().startswith(('we', 'our', 'the', 'a ', 'an ')) and
                ',' not in company):
                return company
    
    return "Unknown"

def extract_title(text: str, structured_title: str) -> str:
    """Extract job title with fallbacks"""
    # Use structured result if available
    if structured_title:
        return structured_title
    
    lines = text.split('\n')
    
    # Look for Roles: section
    for line in lines:
        if re.match(r'^\s*Roles?:\s*', line, re.IGNORECASE):
            title = re.sub(r'^\s*Roles?:\s*', '', line, flags=re.IGNORECASE).strip()
            if title and len(title) < 150:
                return title
    
    # Look for hiring patterns
    full_text = ' '.join(lines)
    patterns = [
        r'(?:looking for|hiring|seeking)\s+(?:a\s+|an\s+)?([^.\n|]+?(?:engineer|developer|manager|director|analyst|designer))',
        r'(?:we need|we want)\s+(?:a\s+|an\s+)?([^.\n|]+?(?:engineer|developer|manager))',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, full_text, re.IGNORECASE)
        if match:
            title = match.group(1).strip()
            title = re.sub(r'^(a|an|the)\s+', '', title, flags=re.IGNORECASE)
            if title and 5 < len(title) < 150:
                return title
    
    return "Software Engineer"

def extract_location(text: str, structured_location: str) -> str:
    """Extract location with fallbacks"""
    # Use structured result if available  
    if structured_location:
        return structured_location
    
    # Special case: look for location in the first line parts
    lines = text.split('\n')
    first_line = lines[0] if lines else ""
    
    if '|' in first_line:
        parts = [part.strip() for part in first_line.split('|')]
        # Look for parts with strong location indicators
        for part in parts:
            part_lower = part.lower()
            strong_location_words = ['remote', 'hybrid', 'onsite', 'sf', 'nyc', 'usa', 'us', 'canada', 'uk', 'europe']
            if any(word in part_lower for word in strong_location_words):
                return part
    
    # Look for location patterns in full text
    patterns = [
        r'(?:Location|Based|Office):\s*([^.\n|]+)',
        r'(?:Remote|Onsite|Hybrid)(?:\s*[-:]?\s*([^.\n|]+))?',
        r'\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*,\s*[A-Z]{2,3})\b',  # City, State
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            location = match.group(1) if match.lastindex and match.group(1) else match.group(0)
            location = location.strip()
            if location and len(location) < 100:
                # Filter out job titles
                job_words = ['engineer', 'developer', 'manager', 'director', 'analyst']
                if not any(word in location.lower() for word in job_words):
                    return location
    
    return "Not specified"

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
            
            # Skip deleted or empty comments
            if comment_data.get('deleted') or not comment_data.get('text'):
                continue
                
            raw_text = comment_data.get('text', '')
            if len(raw_text) < 100:
                continue
            
            # Clean the text
            text = clean_html_text(raw_text)
            lines = text.split('\n')
            first_line = lines[0] if lines else ""
            
            # Parse structured format
            structured = parse_hn_structured_line(first_line)
            
            # Extract fields with fallbacks
            company = extract_company(text, structured.get('company', ''))
            title = extract_title(text, structured.get('title', ''))
            location = extract_location(text, structured.get('location', ''))
            
            # Ensure proper data types and length limits
            # Handle comment_id conversion to avoid .0 in URLs
            if isinstance(comment_id, float):
                comment_id = int(comment_id)
            job_id = str(comment_id) if comment_id else ""
            
            company = str(company)[:100]
            title = str(title)[:100] 
            location = str(location)[:100]
            url = f"https://news.ycombinator.com/item?id={comment_id}"
            description = str(text)[:1000]
            collected_date = str(response.get('collection_date', ''))
            
            # Classify location
            location_type = classify_location(location, location_data)
            
            # Create job record
            job = {
                'job_id': f"hn_{job_id}",
                'source': 'hackernews',
                'title': title,
                'company': company,
                'location': location,
                'location_type': location_type,
                'url': url,
                'description': description,
                'collected_date': collected_date
            }
            
            jobs.append(job)
            
        except Exception as e:
            print(f"   ⚠️  Error parsing comment {response.get('comment_id', 'unknown')}: {e}")
            continue
    
    print(f"   ✅ Parsed {len(jobs)} HackerNews jobs")
    return jobs