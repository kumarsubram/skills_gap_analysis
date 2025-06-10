"""
Jobs Bronze to Silver Pipeline

Orchestrates parsing raw API data from Bronze into structured Silver data.
Combines all sources and creates final Silver files.

Location: include/jobs/jobs_bronze_to_silver.py
"""

import os
import tempfile
import pandas as pd
from collections import Counter
from typing import Dict, List

from include.jobs.job_common_utils import load_location_data, read_bronze_raw_api, save_to_silver
from include.jobs.hackernews_parser import parse_hackernews_raw_to_jobs
from include.jobs.greenhouse_parser import parse_greenhouse_raw_to_jobs
from include.jobs.remoteok_parser import parse_remoteok_raw_to_jobs
from include.jobs.skills_extractor import load_keywords, create_keyword_patterns, extract_skills_from_jobs

def parse_all_sources_to_jobs(date_str: str) -> List[Dict]:
    """Parse all Bronze sources into structured job data"""
    print(f"\n📊 PARSING ALL SOURCES FOR {date_str}")
    
    location_data = load_location_data()
    all_jobs = []
    
    # Parse HackerNews
    hn_raw_data = read_bronze_raw_api('hackernews', date_str)
    if hn_raw_data:
        print("📡 Processing HackerNews...")
        hn_jobs = parse_hackernews_raw_to_jobs(hn_raw_data, location_data)
        all_jobs.extend(hn_jobs)
    
    # Parse Greenhouse
    gh_raw_data = read_bronze_raw_api('greenhouse', date_str)
    if gh_raw_data:
        print("📡 Processing Greenhouse...")
        gh_jobs = parse_greenhouse_raw_to_jobs(gh_raw_data, location_data)
        all_jobs.extend(gh_jobs)
    
    # Parse RemoteOK
    ro_raw_data = read_bronze_raw_api('remoteok', date_str)
    if ro_raw_data:
        print("📡 Processing RemoteOK...")
        ro_jobs = parse_remoteok_raw_to_jobs(ro_raw_data, location_data)
        all_jobs.extend(ro_jobs)
    
    print(f"✅ Total parsed jobs: {len(all_jobs)}")
    return all_jobs

def create_silver_files(jobs_with_skills: List[Dict], skill_counts: Counter, date_str: str) -> List[str]:
    """Create Silver Parquet files and return temp file paths"""
    temp_files = []
    
    # 1. Job details file
    job_details_data = []
    source_stats = Counter()
    
    for job in jobs_with_skills:
        job_details_data.append({
            'date': date_str,
            'job_id': job.get('job_id', ''),
            'source': job.get('source', ''),
            'title': job.get('title', ''),
            'company': job.get('company', ''),
            'location': job.get('location', ''),
            'location_type': job.get('location_type', 'Unknown'),
            'url': job.get('url', ''),
            'description': job.get('description', '')[:1000],
            'skills_found': job.get('skills_found', ''),
            'skill_count': job.get('skill_count', 0),
            'collected_date': job.get('collected_date', date_str)
        })
        source_stats[job.get('source', 'unknown')] += 1
    
    if job_details_data:
        job_details_df = pd.DataFrame(job_details_data)
        job_details_file = os.path.join(tempfile.gettempdir(), f"job_details_{date_str}.parquet")
        job_details_df.to_parquet(job_details_file, index=False)
        temp_files.append(job_details_file)
        print(f"   ✅ Created job_details: {len(job_details_data)} jobs")
    
    # 2. Skills demand file
    keywords = load_keywords()
    skills_demand_data = []
    
    for skill, count in skill_counts.most_common():
        meta = keywords.get(skill, {})
        skills_demand_data.append({
            'date': date_str,
            'skill': skill,
            'job_count': count,
            'type': meta.get('type', ''),
            'language': meta.get('language', ''),
            'framework': meta.get('framework', '')
        })
    
    if skills_demand_data:
        skills_demand_df = pd.DataFrame(skills_demand_data)
        skills_demand_file = os.path.join(tempfile.gettempdir(), f"skills_demand_{date_str}.parquet")
        skills_demand_df.to_parquet(skills_demand_file, index=False)
        temp_files.append(skills_demand_file)
        print(f"   ✅ Created skills_demand: {len(skills_demand_data)} skills")
    
    # 3. Source summary file
    source_summary_data = []
    for source, job_count in source_stats.items():
        jobs_with_skills_count = sum(1 for job in jobs_with_skills 
                                   if job.get('source') == source and job.get('skill_count', 0) > 0)
        usa_count = sum(1 for job in jobs_with_skills 
                      if job.get('source') == source and job.get('location_type') == 'USA')
        
        source_summary_data.append({
            'date': date_str,
            'source': source,
            'job_count': job_count,
            'jobs_with_skills': jobs_with_skills_count,
            'usa_jobs': usa_count,
            'non_usa_jobs': job_count - usa_count
        })
    
    if source_summary_data:
        source_summary_df = pd.DataFrame(source_summary_data)
        source_summary_file = os.path.join(tempfile.gettempdir(), f"source_summary_{date_str}.parquet")
        source_summary_df.to_parquet(source_summary_file, index=False)
        temp_files.append(source_summary_file)
        print(f"   ✅ Created source_summary: {len(source_summary_data)} sources")
    
    return temp_files

def run_bronze_to_silver_pipeline(date_str: str) -> Dict:
    """Main function: Parse Bronze raw API data into Silver structured data"""
    print(f"🔄 BRONZE → SILVER PIPELINE FOR {date_str}")
    print("=" * 50)
    
    # Parse all sources
    all_jobs = parse_all_sources_to_jobs(date_str)
    if not all_jobs:
        return {'success': False, 'reason': 'no_jobs_parsed'}
    
    # Extract skills
    try:
        keywords = load_keywords()
        keyword_patterns = create_keyword_patterns(keywords)
        jobs_with_skills, skill_counts = extract_skills_from_jobs(all_jobs, keyword_patterns)
        print(f"📈 Skills extraction: {sum(skill_counts.values())} mentions, {len(skill_counts)} unique skills")
    except Exception as e:
        return {'success': False, 'reason': f'skills_extraction_failed: {e}'}
    
    # Create and save Silver files
    temp_files = []  # Initialize to avoid NameError
    try:
        temp_files = create_silver_files(jobs_with_skills, skill_counts, date_str)
        
        # Validate expected file count
        if len(temp_files) != 3:  # Should have job_details, skills_demand, source_summary
            raise Exception(f"Expected 3 Silver files, got {len(temp_files)}")
        
        # Save to Silver
        save_result = save_to_silver(temp_files)
        
        # Verify ALL saves succeeded (atomic check)
        if save_result['successful'] != len(temp_files):
            raise Exception(f"Partial save failure: {save_result['successful']}/{len(temp_files)} files saved")
        
        print(f"✅ Saved {save_result['successful']}/{len(temp_files)} Silver files")
        print("=" * 50)
        
        return {
            'success': True,  # Only True if everything succeeded
            'files_saved': save_result['successful'],
            'total_jobs': len(all_jobs),
            'total_skills': sum(skill_counts.values()),
            'unique_skills': len(skill_counts)
        }
        
    except Exception as e:
        print(f"❌ Silver processing failed: {e}")
        return {'success': False, 'reason': str(e)}
        
    finally:
        # Always clean up temp files, regardless of success/failure
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as cleanup_error:
                    print(f"⚠️  Failed to cleanup {temp_file}: {cleanup_error}")
                    # Don't fail the whole pipeline for cleanup issues