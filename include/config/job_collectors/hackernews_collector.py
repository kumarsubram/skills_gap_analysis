"""
HackerNews "Who's Hiring" Job Collector

Collects jobs from HackerNews monthly "Who's Hiring" threads.
These are posted on the first weekday of each month.
"""

import requests
import time
import re
from typing import List, Dict

# HackerNews API base URL
HN_API_BASE = "https://hacker-news.firebaseio.com/v0"

def get_latest_hiring_thread():
    """Find the most recent 'Who's Hiring' thread"""
    
    # Search for recent "Who's Hiring" posts
    # HN search is limited, so we'll check recent stories from 'whoishiring' user
    try:
        # Get whoishiring user's recent submissions
        user_url = f"{HN_API_BASE}/user/whoishiring.json"
        response = requests.get(user_url, timeout=10)
        
        if response.status_code != 200:
            print(f"   ❌ Failed to get whoishiring user data: {response.status_code}")
            return None
            
        user_data = response.json()
        submitted_items = user_data.get('submitted', [])
        
        # Check recent submissions for hiring threads
        for item_id in submitted_items[:10]:  # Check last 10 posts
            item_url = f"{HN_API_BASE}/item/{item_id}.json"
            item_response = requests.get(item_url, timeout=10)
            
            if item_response.status_code == 200:
                item_data = item_response.json()
                title = item_data.get('title', '').lower()
                
                # Look for hiring-related posts
                if 'who is hiring' in title and item_data.get('type') == 'story':
                    print(f"   📝 Found hiring thread: {item_data.get('title')}")
                    return item_data
                    
        print("   ⚠️  No recent hiring thread found")
        return None
        
    except Exception as e:
        print(f"   ❌ Error finding hiring thread: {e}")
        return None

def is_usa_location(location_text):
    """Check if a location is in the USA"""
    if not location_text:
        return False
        
    location_lower = location_text.lower()
    
    # USA indicators
    usa_keywords = [
        'usa', 'united states', 'america', 'us only', 'u.s.', 'american',
        'remote usa', 'remote us', 'us remote', 'usa remote',
        'nationwide', 'anywhere in us', 'anywhere in usa'
    ]
    
    # US states (abbreviated and full names)
    us_states = [
        'alabama', 'al', 'alaska', 'ak', 'arizona', 'az', 'arkansas', 'ar',
        'california', 'ca', 'colorado', 'co', 'connecticut', 'ct', 'delaware', 'de',
        'florida', 'fl', 'georgia', 'ga', 'hawaii', 'hi', 'idaho', 'id',
        'illinois', 'il', 'indiana', 'in', 'iowa', 'ia', 'kansas', 'ks',
        'kentucky', 'ky', 'louisiana', 'la', 'maine', 'me', 'maryland', 'md',
        'massachusetts', 'ma', 'michigan', 'mi', 'minnesota', 'mn', 'mississippi', 'ms',
        'missouri', 'mo', 'montana', 'mt', 'nebraska', 'ne', 'nevada', 'nv',
        'new hampshire', 'nh', 'new jersey', 'nj', 'new mexico', 'nm', 'new york', 'ny',
        'north carolina', 'nc', 'north dakota', 'nd', 'ohio', 'oh', 'oklahoma', 'ok',
        'oregon', 'or', 'pennsylvania', 'pa', 'rhode island', 'ri', 'south carolina', 'sc',
        'south dakota', 'sd', 'tennessee', 'tn', 'texas', 'tx', 'utah', 'ut',
        'vermont', 'vt', 'virginia', 'va', 'washington', 'wa', 'west virginia', 'wv',
        'wisconsin', 'wi', 'wyoming', 'wy', 'washington dc', 'dc'
    ]
    
    # Major US cities
    us_cities = [
        'new york', 'nyc', 'los angeles', 'chicago', 'houston', 'phoenix', 'philadelphia',
        'san antonio', 'san diego', 'dallas', 'san jose', 'austin', 'jacksonville',
        'fort worth', 'columbus', 'charlotte', 'san francisco', 'indianapolis',
        'seattle', 'denver', 'washington', 'boston', 'el paso', 'nashville',
        'detroit', 'oklahoma city', 'portland', 'las vegas', 'memphis', 'louisville',
        'baltimore', 'milwaukee', 'albuquerque', 'tucson', 'fresno', 'sacramento',
        'mesa', 'kansas city', 'atlanta', 'long beach', 'colorado springs', 'raleigh',
        'miami', 'virginia beach', 'omaha', 'oakland', 'minneapolis', 'tulsa',
        'tampa', 'arlington', 'new orleans', 'wichita', 'cleveland', 'bakersfield',
        'santa ana', 'anaheim', 'honolulu', 'riverside', 'corpus christi', 'lexington',
        'stockton', 'henderson', 'saint paul', 'st paul', 'cincinnati', 'pittsburgh'
    ]
    
    # Check for USA keywords
    for keyword in usa_keywords:
        if keyword in location_lower:
            return True
    
    # Check for US states
    for state in us_states:
        if f' {state} ' in f' {location_lower} ' or location_lower.endswith(f' {state}') or location_lower.startswith(f'{state} '):
            return True
    
    # Check for US cities
    for city in us_cities:
        if city in location_lower:
            return True
    
    # Exclude non-USA indicators
    non_usa_keywords = [
        'europe', 'european', 'eu ', 'uk', 'england', 'london', 'germany', 'berlin',
        'france', 'paris', 'canada', 'toronto', 'vancouver', 'australia', 'sydney',
        'india', 'bangalore', 'mumbai', 'asia', 'japan', 'tokyo', 'china', 'beijing',
        'singapore', 'hong kong', 'netherlands', 'amsterdam', 'belgium', 'switzerland',
        'sweden', 'norway', 'denmark', 'finland', 'poland', 'spain', 'italy', 'brazil',
        'mexico', 'argentina', 'chile', 'israel', 'tel aviv', 'south africa', 'egypt',
        'gmt+', 'gmt-', 'cet', 'bst', 'utc+', 'utc-', 'timezone', 'timezones'
    ]
    
    for keyword in non_usa_keywords:
        if keyword in location_lower:
            return False
    
    # If location mentions "remote" without country specification, assume it could be USA
    if 'remote' in location_lower and len(location_lower) < 20:
        return True
    
    # Default: if we can't determine, exclude it (conservative approach)
    return False

def extract_job_from_comment(comment_data) -> Dict:
    """Extract job details from a HN comment"""
    
    if not comment_data or comment_data.get('deleted') or not comment_data.get('text'):
        return None
        
    text = comment_data.get('text', '')
    comment_id = comment_data.get('id')
    
    # Skip if comment is too short (likely not a job posting)
    if len(text) < 100:
        return None
    
    # Basic extraction patterns
    # Many HN job posts start with company name or "COMPANY:"
    company_match = re.search(r'^([A-Z][A-Za-z\s&.,-]+)(?:\s*[-|:]|\s+is\s+|\.)', text)
    company = company_match.group(1).strip() if company_match else "Unknown"
    
    # Look for location patterns
    location_patterns = [
        r'(?:Location|Based):\s*([^<\n]+)',
        r'(?:Remote|On-site|Hybrid)(?:\s*[-:]?\s*([^<\n]+))?',
        r'\b([A-Z][a-z]+,\s*[A-Z]{2,})\b',  # City, State/Country
    ]
    
    location = "Not specified"
    for pattern in location_patterns:
        location_match = re.search(pattern, text, re.IGNORECASE)
        if location_match:
            location = location_match.group(1).strip() if location_match.group(1) else location_match.group(0)
            break
    
    # Extract title/role (often after company name)
    title_patterns = [
        r'(?:hiring|seeking|looking for)[\s:]*([^<\n\.]+)',
        r'(?:Position|Role|Job)[\s:]*([^<\n\.]+)',
        r'We are hiring\s+([^<\n\.]+)',
    ]
    
    title = "Software Engineer"  # Default
    for pattern in title_patterns:
        title_match = re.search(pattern, text, re.IGNORECASE)
        if title_match:
            title = title_match.group(1).strip()
            break
    
    # Clean up extracted text (remove HTML tags)
    def clean_text(text):
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', '', text)
        # Remove extra whitespace
        text = re.sub(r'\s+', ' ', text)
        return text.strip()
    
    # Extract key requirements/description
    # Look for common sections
    description_sections = []
    
    # Split text into lines and look for key sections
    lines = text.split('\n')
    current_section = []
    
    for line in lines:
        clean_line = clean_text(line)
        if not clean_line:
            continue
            
        # Look for requirement/tech sections
        if any(keyword in clean_line.lower() for keyword in 
               ['requirements', 'looking for', 'tech stack', 'technologies', 'skills', 'experience']):
            if current_section:
                description_sections.extend(current_section)
            current_section = [clean_line]
        else:
            current_section.append(clean_line)
    
    if current_section:
        description_sections.extend(current_section)
    
    description = ' '.join(description_sections[:5])  # First 5 relevant lines
    if not description:
        description = clean_text(text[:500])  # Fallback to first 500 chars
    
    # 🇺🇸 USA LOCATION FILTER
    if not is_usa_location(location):
        return None  # Skip non-USA jobs
    
    return {
        'job_id': f"hn_{comment_id}",
        'source': 'hackernews',
        'title': clean_text(title)[:100],  # Limit length
        'company': clean_text(company)[:100],
        'location': clean_text(location)[:100],
        'url': f"https://news.ycombinator.com/item?id={comment_id}",
        'description': description[:1000],  # Limit description length
        'raw_text': text[:2000]  # Keep some raw text for debugging
    }

def collect_hackernews_jobs(date_str: str) -> List[Dict]:
    """
    Collect jobs from HackerNews 'Who's Hiring' threads
    
    Args:
        date_str: Date string (YYYY-MM-DD) - used for consistency but HN posts are monthly
        
    Returns:
        List of job dictionaries
    """
    
    print("   🔍 Searching for HackerNews jobs...")
    
    # Find the latest hiring thread
    hiring_thread = get_latest_hiring_thread()
    if not hiring_thread:
        return []
    
    thread_id = hiring_thread.get('id')
    kids = hiring_thread.get('kids', [])
    
    print(f"   📊 Found hiring thread with {len(kids)} top-level comments")
    
    jobs = []
    processed = 0
    
    # Process comments (job postings)
    for comment_id in kids[:100]:  # Limit to first 100 comments for speed
        try:
            # Get comment data
            comment_url = f"{HN_API_BASE}/item/{comment_id}.json"
            response = requests.get(comment_url, timeout=10)
            
            if response.status_code == 200:
                comment_data = response.json()
                job = extract_job_from_comment(comment_data)
                
                if job:
                    jobs.append(job)
                
                processed += 1
                
                # Rate limiting
                if processed % 20 == 0:
                    print(f"   ⏳ Processed {processed} comments, found {len(jobs)} jobs")
                    time.sleep(1)  # Be nice to HN API
                    
            else:
                print(f"   ⚠️  Failed to get comment {comment_id}: {response.status_code}")
                
        except Exception as e:
            print(f"   ❌ Error processing comment {comment_id}: {e}")
            continue
    
    print(f"   ✅ HackerNews: {len(jobs)} USA jobs from {processed} comments (filtered for USA locations)")
    
    # Add some metadata
    for job in jobs:
        job['collected_date'] = date_str
        job['thread_id'] = thread_id
        job['thread_title'] = hiring_thread.get('title', '')
        job['location_filter'] = 'USA_only'  # Mark that we filtered for USA
    
    return jobs