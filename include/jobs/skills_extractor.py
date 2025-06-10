"""
Skills Extractor

Simple skill extraction from job descriptions.
Uses same keywords as GitHub processing.
"""

import json
import re
from collections import Counter
from typing import Dict, List, Tuple

def load_keywords() -> Dict:
    """Load keywords from config"""
    keywords_file = "/usr/local/airflow/include/jsons/tech_keywords.json"
    
    with open(keywords_file, "r", encoding="utf-8") as f:
        keywords = json.load(f)
    
    return keywords

def create_keyword_patterns(keywords: Dict) -> Dict[str, re.Pattern]:
    """Create regex patterns for keywords"""
    patterns = {}
    for keyword in keywords:
        patterns[keyword] = re.compile(rf"\b{re.escape(keyword.lower())}\b", re.IGNORECASE)
    return patterns

def extract_skills_from_jobs(jobs: List[Dict], keyword_patterns: Dict[str, re.Pattern]) -> Tuple[List[Dict], Counter]:
    """Extract skills from jobs"""
    enhanced_jobs = []
    total_skill_counts = Counter()
    
    for job in jobs:
        # Search in title and description
        search_text = f"{job.get('title', '')} {job.get('description', '')}".lower()
        
        found_skills = []
        for keyword, pattern in keyword_patterns.items():
            if pattern.search(search_text):
                found_skills.append(keyword)
                total_skill_counts[keyword] += 1
        
        # Add skills to job
        enhanced_job = job.copy()
        enhanced_job['skills_found'] = ','.join(found_skills)
        enhanced_job['skill_count'] = len(found_skills)
        enhanced_jobs.append(enhanced_job)
    
    return enhanced_jobs, total_skill_counts