"""
Greenhouse Job Collector - Maximum Data Collection

Collects jobs from popular tech companies using Greenhouse job boards.
Each company has their own boards.greenhouse.io/{company} endpoint.
"""

import requests
import time
import re
import json
from pathlib import Path
from typing import List, Dict

# 🔁 Load Greenhouse company slugs from JSON
def load_greenhouse_companies() -> List[str]:
    json_path = Path(__file__).parent / "greenhouse_companies.json"
    with open(json_path, "r", encoding="utf-8") as f:
        return json.load(f)

GREENHOUSE_COMPANIES = load_greenhouse_companies()

def collect_greenhouse_jobs(date_str: str) -> List[Dict]:
    """
    Collect jobs from Greenhouse company job boards

    Args:
        date_str: Date string for file naming (all jobs are current)

    Returns:
        List of job dictionaries
    """
    print(f"   🔍 Collecting from {len(GREENHOUSE_COMPANIES)} Greenhouse companies...")

    all_jobs = []
    successful_companies = 0
    failed_companies = 0

    for company in GREENHOUSE_COMPANIES:
        try:
            url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
            print(f"   📡 {company}...", end=" ", flush=True)

            response = requests.get(url, timeout=15)

            if response.status_code == 200:
                data = response.json()
                jobs_data = data.get('jobs', [])

                company_jobs = 0
                for job_data in jobs_data:
                    location = job_data.get('location', {})
                    location_name = location.get('name', '') if location else ''

                    content = job_data.get('content', '')
                    clean_content = re.sub(r'<[^>]+>', ' ', content)
                    clean_content = re.sub(r'\s+', ' ', clean_content).strip()

                    job = {
                        'job_id': f"greenhouse_{company}_{job_data.get('id', '')}",
                        'source': 'greenhouse',
                        'title': job_data.get('title', ''),
                        'company': company.replace('-', ' ').title(),
                        'location': location_name,
                        'url': job_data.get('absolute_url', ''),
                        'description': clean_content[:1000],
                        'departments': ','.join([dept.get('name', '') for dept in job_data.get('departments', [])]),
                        'offices': ','.join([office.get('name', '') for office in job_data.get('offices', [])]),
                        'collected_date': date_str,
                        'company_slug': company
                    }

                    all_jobs.append(job)
                    company_jobs += 1

                print(f"{company_jobs} jobs")
                successful_companies += 1

            elif response.status_code == 404:
                print("not found")
                failed_companies += 1
            else:
                print(f"error {response.status_code}")
                failed_companies += 1

            time.sleep(0.5)

        except Exception as e:
            print(f"error: {e}")
            failed_companies += 1
            continue

    print(f"   ✅ Greenhouse: {len(all_jobs)} jobs from {successful_companies} companies")
    print(f"   📊 Success: {successful_companies}, Failed: {failed_companies}")

    return all_jobs

def get_top_greenhouse_companies():
    """Get list of companies with most job postings (for debugging)"""
    company_job_counts = {}

    print("🔍 Scanning companies for job counts...")

    for company in GREENHOUSE_COMPANIES[:20]:
        try:
            url = f"https://boards-api.greenhouse.io/v1/boards/{company}/jobs"
            response = requests.get(url, timeout=10)

            if response.status_code == 200:
                data = response.json()
                job_count = len(data.get('jobs', []))
                company_job_counts[company] = job_count
                print(f"   {company}: {job_count} jobs")
            else:
                print(f"   {company}: API error")

            time.sleep(0.5)
        except:
            print(f"   {company}: Failed")

    sorted_companies = sorted(company_job_counts.items(), key=lambda x: x[1], reverse=True)
    return sorted_companies

def test_greenhouse_collector():
    """Test the Greenhouse collector with a few companies"""
    test_companies = ['airbnb', 'stripe', 'notion', 'figma', 'discord']

    print(f"🧪 Testing Greenhouse collector with {len(test_companies)} companies...")

    global GREENHOUSE_COMPANIES
    original_companies = GREENHOUSE_COMPANIES
    GREENHOUSE_COMPANIES = test_companies

    try:
        jobs = collect_greenhouse_jobs("2025-06-04")
        print(f"\n📊 Test Results: {len(jobs)} jobs found")

        if jobs:
            print("\nSample job:")
            sample = jobs[0]
            for key, value in sample.items():
                print(f"  {key}: {str(value)[:100]}")

        return jobs
    finally:
        GREENHOUSE_COMPANIES = original_companies

if __name__ == "__main__":
    test_greenhouse_collector()
    # Optionally scan top companies
    # top_companies = get_top_greenhouse_companies()
