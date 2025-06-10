"""
RemoteOK Parser

Parses raw RemoteOK API data from Bronze into structured job data.
Handles only RemoteOK-specific parsing logic.

Location: include/jobs/remoteok_parser.py
"""

from typing import Dict, List
from include.jobs.job_common_utils import classify_location

def parse_remoteok_raw_to_jobs(raw_responses: List[Dict], location_data: Dict) -> List[Dict]:
    """Parse raw RemoteOK API responses into structured job data"""
    jobs = []
    
    # Filter for successful responses
    valid_responses = [
        r for r in raw_responses 
        if r.get('api_type') == 'job_listings'
        and r.get('status_code') == 200 
        and r.get('response_data')
    ]
    
    print(f"   📋 Parsing {len(valid_responses)} RemoteOK responses...")
    
    for response in valid_responses:
        try:
            data = response['response_data']
            
            # RemoteOK returns an array of jobs directly
            if isinstance(data, list):
                for job_data in data:
                    # Skip the first item if it's metadata (RemoteOK sometimes includes this)
                    if isinstance(job_data, dict) and job_data.get('id'):
                        try:
                            # Extract job details based on RemoteOK API structure
                            job_id = job_data.get('id', '')
                            title = job_data.get('position', '') or job_data.get('title', '')
                            company = job_data.get('company', '')
                            
                            # RemoteOK location handling
                            location = job_data.get('location', '')
                            if not location or location.lower() in ['anywhere', 'worldwide', '']:
                                location = 'Remote'
                            
                            # Build job URL
                            slug = job_data.get('slug', '')
                            url = f"https://remoteok.com/remote-jobs/{slug}" if slug else job_data.get('url', '')
                            
                            # Get description - RemoteOK uses 'description' field
                            description = job_data.get('description', '')
                            if not description:
                                # Fallback to other possible description fields
                                description = job_data.get('summary', '') or job_data.get('job_description', '')
                            
                            # Parse salary if available
                            salary_min = job_data.get('salary_min')
                            salary_max = job_data.get('salary_max')
                            salary_info = ''
                            if salary_min or salary_max:
                                if salary_min and salary_max:
                                    salary_info = f"${salary_min:,} - ${salary_max:,}"
                                elif salary_min:
                                    salary_info = f"${salary_min:,}+"
                                elif salary_max:
                                    salary_info = f"Up to ${salary_max:,}"
                            
                            # Add salary to description if available
                            if salary_info:
                                description = f"Salary: {salary_info}\n\n{description}"
                            
                            job = {
                                'job_id': f"remoteok_{job_id}",
                                'source': 'remoteok',
                                'title': title,
                                'company': company,
                                'location': location,
                                'location_type': classify_location(location, location_data),
                                'url': url,
                                'description': description[:1000],  # Truncate to 1000 chars
                                'collected_date': response.get('collection_date', '')
                            }
                            
                            # Only add jobs with required fields
                            if job_id and title and company:
                                jobs.append(job)
                            else:
                                print(f"   ⚠️  Skipping incomplete job: id={job_id}, title='{title}', company='{company}'")
                                
                        except Exception as e:
                            print(f"   ⚠️  Error parsing individual job: {e}")
                            continue
            else:
                print(f"   ⚠️  Unexpected data format: expected list, got {type(data)}")
                
        except Exception as e:
            print(f"   ⚠️  Error parsing RemoteOK response: {e}")
            continue
    
    print(f"   ✅ Parsed {len(jobs)} RemoteOK jobs")
    return jobs