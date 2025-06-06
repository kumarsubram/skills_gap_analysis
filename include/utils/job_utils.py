"""
Job Posting Utilities
=====================

Utilities for processing job posting data from various job APIs.
"""

from datetime import datetime, timezone
from typing import Dict, List, Any
from .api_utils import extract_text_content, standardize_timestamp

def process_job_posting(job_data: Dict[str, Any], additional_fields: Dict[str, Any] = None,
                       source_api: str = "unknown") -> Dict[str, Any]:
    """
    Standard job posting processing for multiple job APIs.
    
    Args:
        job_data: Raw job posting data from API
        additional_fields: Optional dict of additional fields to include
        source_api: Source API ('linkedin', 'stackoverflow', 'indeed', etc.)
        
    Returns:
        Processed job posting dictionary ready for Kafka
    """
    # Handle different field names across APIs
    processed_job = {
        'id': (job_data.get('id') or job_data.get('jobkey') or 
               job_data.get('job_id') or job_data.get('jk')),
        'title': (job_data.get('title') or job_data.get('jobtitle') or 
                 job_data.get('job_title')),
        'company': (job_data.get('company') or job_data.get('company_name') or 
                   job_data.get('companyName')),
        'location': (job_data.get('location') or job_data.get('formattedLocation') or 
                    job_data.get('job_location')),
        'description': (job_data.get('description') or job_data.get('snippet') or 
                       job_data.get('job_description')),
        'posted_date': standardize_timestamp(
            job_data.get('posted_date') or job_data.get('date') or 
            job_data.get('postedDate') or job_data.get('formattedRelativeTime')
        ),
        'salary': (job_data.get('salary') or job_data.get('salary_min') or 
                  job_data.get('salaryRange')),
        'url': (job_data.get('url') or job_data.get('job_url') or 
               job_data.get('link')),
        'remote': (job_data.get('remote') or job_data.get('is_remote') or 
                  job_data.get('remoteFriendly')),
        'source_api': source_api,
        'processed_at': datetime.now(timezone.utc).isoformat(),
    }
    
    # Extract technologies from job description
    description = processed_job.get('description', '')
    if description:
        processed_job['technologies'] = extract_technologies_from_job(description)
        processed_job['seniority_level'] = extract_seniority_level(description)
        processed_job['job_type'] = extract_job_type(description)
    
    # Add any additional fields
    if additional_fields:
        processed_job.update(additional_fields)
    
    return processed_job

def extract_technologies_from_job(job_description: str) -> List[str]:
    """
    Extract required technologies from job posting description.
    
    Args:
        job_description: Job description text
        
    Returns:
        List of detected technologies/skills
    """
    if not job_description:
        return []
    
    description_lower = job_description.lower()
    technologies = []
    
    # Technology keywords for job postings
    job_tech_keywords = {
        # Programming Languages
        'python': ['python', 'django', 'flask', 'fastapi', 'pandas', 'numpy'],
        'javascript': ['javascript', 'js', 'node.js', 'nodejs', 'typescript'],
        'java': ['java', 'spring', 'maven', 'gradle', 'springboot'],
        'react': ['react', 'reactjs', 'react.js', 'jsx'],
        'angular': ['angular', 'angularjs'],
        'vue': ['vue', 'vuejs', 'vue.js'],
        'go': ['golang', 'go'],
        'rust': ['rust'],
        'php': ['php', 'laravel', 'symfony'],
        'ruby': ['ruby', 'rails', 'ruby on rails'],
        'csharp': ['c#', 'csharp', '.net', 'dotnet'],
        
        # Databases
        'postgresql': ['postgresql', 'postgres', 'psql'],
        'mysql': ['mysql'],
        'mongodb': ['mongodb', 'mongo'],
        'redis': ['redis'],
        'elasticsearch': ['elasticsearch', 'elastic'],
        'oracle': ['oracle'],
        'sqlite': ['sqlite'],
        
        # Cloud & DevOps
        'aws': ['aws', 'amazon web services', 'ec2', 's3', 'lambda'],
        'azure': ['azure', 'microsoft azure'],
        'gcp': ['gcp', 'google cloud', 'firebase'],
        'docker': ['docker', 'containerization'],
        'kubernetes': ['kubernetes', 'k8s'],
        'terraform': ['terraform'],
        'jenkins': ['jenkins'],
        'gitlab': ['gitlab ci', 'gitlab'],
        'github': ['github actions'],
        
        # Data & Analytics
        'spark': ['spark', 'pyspark', 'apache spark'],
        'kafka': ['kafka', 'apache kafka'],
        'airflow': ['airflow', 'apache airflow'],
        'hadoop': ['hadoop'],
        'snowflake': ['snowflake'],
        'databricks': ['databricks'],
        
        # Frontend
        'html': ['html', 'html5'],
        'css': ['css', 'css3', 'sass', 'scss'],
        'tailwind': ['tailwind', 'tailwindcss'],
        'bootstrap': ['bootstrap'],
        
        # Tools & Methodologies
        'git': ['git', 'github', 'gitlab'],
        'api': ['api', 'rest api', 'restful', 'graphql'],
        'microservices': ['microservices', 'microservice'],
        'agile': ['agile', 'scrum', 'kanban'],
        'ci-cd': ['ci/cd', 'cicd', 'continuous integration']
    }
    
    for tech, keywords in job_tech_keywords.items():
        if any(keyword in description_lower for keyword in keywords):
            technologies.append(tech)
    
    return list(set(technologies))

def extract_seniority_level(job_description: str) -> str:
    """
    Extract seniority level from job description.
    
    Args:
        job_description: Job description text
        
    Returns:
        Detected seniority level
    """
    if not job_description:
        return 'unknown'
    
    description_lower = job_description.lower()
    
    # Seniority indicators
    senior_keywords = ['senior', 'sr.', 'lead', 'principal', 'staff', 'architect']
    mid_keywords = ['mid-level', 'intermediate', '3-5 years', '4-6 years']
    junior_keywords = ['junior', 'jr.', 'entry-level', 'graduate', 'intern', 'new grad']
    
    if any(keyword in description_lower for keyword in senior_keywords):
        return 'senior'
    elif any(keyword in description_lower for keyword in mid_keywords):
        return 'mid'
    elif any(keyword in description_lower for keyword in junior_keywords):
        return 'junior'
    else:
        return 'unknown'

def extract_job_type(job_description: str) -> str:
    """
    Extract job type from job description.
    
    Args:
        job_description: Job description text
        
    Returns:
        Detected job type
    """
    if not job_description:
        return 'unknown'
    
    description_lower = job_description.lower()
    
    # Job type indicators
    if any(keyword in description_lower for keyword in ['full-time', 'full time', 'permanent']):
        return 'full-time'
    elif any(keyword in description_lower for keyword in ['part-time', 'part time']):
        return 'part-time'
    elif any(keyword in description_lower for keyword in ['contract', 'contractor', 'freelance']):
        return 'contract'
    elif any(keyword in description_lower for keyword in ['intern', 'internship']):
        return 'internship'
    else:
        return 'unknown'

def calculate_job_tech_demand_score(technologies: List[str]) -> int:
    """
    Calculate demand score based on technologies mentioned.
    
    Args:
        technologies: List of technologies from job posting
        
    Returns:
        Demand score (higher = more in-demand tech stack)
    """
    # Technology demand weights (based on general market demand)
    tech_weights = {
        'python': 10, 'javascript': 10, 'react': 9, 'aws': 9,
        'docker': 8, 'kubernetes': 8, 'java': 8, 'nodejs': 8,
        'postgresql': 7, 'mongodb': 7, 'typescript': 7,
        'angular': 6, 'vue': 6, 'redis': 6, 'mysql': 6,
        'go': 5, 'rust': 5, 'terraform': 7, 'kafka': 6
    }
    
    score = 0
    for tech in technologies:
        score += tech_weights.get(tech, 1)  # Default weight of 1
    
    return score