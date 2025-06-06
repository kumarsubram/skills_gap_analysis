from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import os
import json
import csv
import re
import time
from collections import Counter

# 🔧 PER-SOURCE SCHEDULING CONFIGURATION
SOURCE_SCHEDULES = {
    'hackernews': 'weekly',    # Mondays only (monthly posts, no need for daily)
    'greenhouse': 'daily',     # Every day (fresh job listings from 75+ companies)
    # 'remoteok': 'daily',     # Every day (fresh job listings)
}

# Valid schedules: 'daily', 'weekly', 'disabled'

def load_keywords():
    """Load same keywords used in GitHub processing"""
    base_dir = os.path.dirname(os.path.dirname(__file__))
    keyword_path = os.path.join(base_dir, "include", "config", "keywords.json")
    print(f"🔍 Loading keywords from: {keyword_path}")
    
    with open(keyword_path, "r", encoding="utf-8") as f:
        keywords = json.load(f)
    
    print(f"📋 Loaded {len(keywords)} keywords")
    return keywords

def create_keyword_patterns(keywords):
    """Pre-compile regex patterns for better performance"""
    patterns = {}
    for keyword in keywords:
        patterns[keyword] = re.compile(rf"\b{re.escape(keyword.lower())}\b", re.IGNORECASE)
    return patterns

def should_source_run_today(source_name, schedule):
    """Check if a specific source should run today"""
    today = datetime.now(timezone.utc)
    
    if schedule == 'daily':
        return True
    elif schedule == 'weekly':
        return today.weekday() == 0  # Monday = 0
    elif schedule == 'disabled':
        return False
    else:
        return True  # Default to daily

def get_sources_for_today():
    """Determine which sources should run today"""
    today_name = datetime.now(timezone.utc).strftime("%A")
    sources_to_run = []
    
    print(f"📅 Today is {today_name} - checking source schedules:")
    
    for source_name, schedule in SOURCE_SCHEDULES.items():
        should_run = should_source_run_today(source_name, schedule)
        status = "✅ RUN" if should_run else "⏭️  SKIP"
        print(f"   📡 {source_name}: {schedule} schedule → {status}")
        
        if should_run:
            sources_to_run.append(source_name)
    
    return sources_to_run

def is_already_processed_today(date_str):
    """Check if we already collected jobs today"""
    output_file = f"/usr/local/airflow/data/job_output/{date_str}/job_details_{date_str}.csv"
    
    if os.path.exists(output_file):
        file_size = os.path.getsize(output_file)
        if file_size > 100:  # More than just headers
            print(f"✅ {date_str} already processed ({file_size} bytes)")
            return True
    
    return False

def collect_from_source(source_name, date_str, keyword_patterns):
    """Dynamically import and run specific job collector"""
    try:
        # Try new location first, then fall back to old location
        try:
            module_path = f"include.job_collectors.{source_name}_collector"
            collector_module = __import__(module_path, fromlist=[f"{source_name}_collector"])
        except ImportError:
            # Fall back to old location (in dags folder)
            module_path = f"{source_name}_collector"
            collector_module = __import__(module_path, fromlist=[f"{source_name}_collector"])
        
        # Get the collect function
        collect_function = getattr(collector_module, f"collect_{source_name}_jobs")
        
        # Run collection (date_str is for file naming, all sources get current jobs)
        print(f"📡 Collecting current jobs from {source_name}...")
        jobs = collect_function(date_str)
        
        # Extract skills from each job
        processed_jobs = []
        skill_counts = Counter()
        
        for job in jobs:
            # Search title + description for skills
            search_text = f"{job.get('title', '')} {job.get('description', '')}".lower()
            
            found_skills = []
            for keyword, pattern in keyword_patterns.items():
                if pattern.search(search_text):
                    found_skills.append(keyword)
                    skill_counts[keyword] += 1
            
            # Add skills data to job record
            enhanced_job = job.copy()
            enhanced_job['skills_found'] = ','.join(found_skills)
            enhanced_job['skill_count'] = len(found_skills)
            enhanced_job['collected_date'] = date_str
            processed_jobs.append(enhanced_job)
        
        skill_matches = sum(skill_counts.values())
        print(f"   ✅ {source_name}: {len(processed_jobs)} jobs, {skill_matches} skill matches")
        return processed_jobs, skill_counts
        
    except ImportError as e:
        print(f"   ❌ {source_name}: Collector module not found in either location - {e}")
        print(f"   💡 Expected: include/job_collectors/{source_name}_collector.py OR dags/{source_name}_collector.py")
        return [], Counter()
    except Exception as e:
        print(f"   ❌ {source_name}: Collection failed - {e}")
        return [], Counter()

def save_job_results(all_jobs, all_skill_counts, date_str):
    """Save job data to CSV files"""
    output_dir = f"/usr/local/airflow/data/job_output/{date_str}"
    os.makedirs(output_dir, exist_ok=True)
    
    # 1. Job details with skills
    job_file = os.path.join(output_dir, f"job_details_{date_str}.csv")
    with open(job_file, "w", newline="", encoding="utf-8") as f:
        if all_jobs:
            fieldnames = all_jobs[0].keys()
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_jobs)
        else:
            # Empty file with headers
            headers = ["job_id", "source", "title", "company", "location", "url", 
                      "description", "skills_found", "skill_count", "collected_date"]
            writer = csv.writer(f)
            writer.writerow(headers)
    
    # 2. Skills demand aggregation (matches GitHub format)
    skills_file = os.path.join(output_dir, f"skills_demand_{date_str}.csv")
    with open(skills_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["date", "skill", "job_count", "type", "language", "framework"])
        
        # Get keyword metadata
        keywords_config = load_keywords()
        for skill, count in all_skill_counts.most_common():
            meta = keywords_config.get(skill, {})
            writer.writerow([
                date_str,
                skill,
                count,
                meta.get("type", ""),
                meta.get("language", ""),
                meta.get("framework", "")
            ])
    
    # Summary
    total_jobs = len(all_jobs)
    total_skills = sum(all_skill_counts.values())
    unique_skills = len(all_skill_counts)
    
    print("💾 Saved results:")
    print(f"   📊 {total_jobs} jobs with {total_skills} skill mentions ({unique_skills} unique)")
    print(f"   📄 {job_file} ({os.path.getsize(job_file)} bytes)")
    print(f"   📄 {skills_file} ({os.path.getsize(skills_file)} bytes)")
    
    if total_skills > 0:
        print(f"🔥 Top skills: {dict(all_skill_counts.most_common(10))}")

def run_daily_job_collection(**context):
    """Main collection function - runs on schedule OR manual trigger"""
    
    print("🚀 DAILY JOB DEMAND COLLECTION")
    print("=" * 50)
    
    # Determine trigger type
    dag_run = context.get("dag_run", {})
    conf = dag_run.conf or {}
    is_manual = dag_run.run_type == "manual"
    is_scheduled = dag_run.run_type == "scheduled"
    
    print(f"🎯 Trigger: {'Manual' if is_manual else 'Scheduled'}")
    
    # Load keywords
    try:
        keywords = load_keywords()
        keyword_patterns = create_keyword_patterns(keywords)
    except Exception as e:
        print(f"❌ Failed to load keywords: {e}")
        raise
    
    # Determine collection date
    if "date" in conf:
        collection_date = conf["date"]
        print(f"📅 Manual date override: {collection_date}")
    else:
        collection_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        print(f"📅 Using today's date: {collection_date}")
    
    # Check if already processed (skip check for manual triggers)
    if is_scheduled and is_already_processed_today(collection_date):
        print(f"ℹ️  Already processed {collection_date} on schedule.")
        print("💡 Use manual trigger to force re-collection.")
        return
    elif is_manual and is_already_processed_today(collection_date):
        print(f"🔄 Manual trigger: Re-processing {collection_date}")
    
    # Get sources for today (or all sources if manual with force_all_sources)
    if is_manual and conf.get("force_all_sources", False):
        # Manual trigger can force all sources regardless of schedule
        sources_to_run = list(SOURCE_SCHEDULES.keys())
        print("🔧 Manual override: Running ALL sources (ignoring schedules)")
    else:
        # Normal logic: respect per-source schedules
        sources_to_run = get_sources_for_today()
    
    if not sources_to_run:
        print("⏭️  No sources to run")
        if is_scheduled:
            # Create empty files to mark date as processed
            save_job_results([], Counter(), collection_date)
        return
    
    print(f"🎯 Running {len(sources_to_run)} sources: {', '.join(sources_to_run)}")
    
    # Collect from sources
    start_time = time.time()
    all_jobs = []
    all_skill_counts = Counter()
    
    for source_name in sources_to_run:
        jobs, skill_counts = collect_from_source(source_name, collection_date, keyword_patterns)
        all_jobs.extend(jobs)
        all_skill_counts.update(skill_counts)
    
    # Save results
    save_job_results(all_jobs, all_skill_counts, collection_date)
    
    # Final summary
    elapsed = time.time() - start_time
    print("\n🏁 COLLECTION COMPLETE")
    print(f"🎯 Trigger type: {'Manual' if is_manual else 'Scheduled'}")
    print(f"⏱️  Time: {elapsed:.1f} seconds")
    print(f"📊 Sources run: {len(sources_to_run)}")
    print(f"🎯 Jobs found: {len(all_jobs)}")
    print(f"📅 Collection date: {collection_date}")

# DAG Definition
default_args = {
    "owner": "data-engineering",
    "start_date": datetime(2025, 6, 4, tzinfo=timezone.utc),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="job_demand_collector_daily",
    default_args=default_args,
    description="Daily job collection with per-source scheduling",
    schedule="0 9 * * *",  # Runs daily at 9 AM UTC
    catchup=False,
    tags=["jobs", "skills", "demand", "per-source-scheduling"],
    doc_md="""
    # Daily Job Demand Collector (Scheduled + Manual)
    
    **Automatic**: Runs daily at 9 AM UTC with per-source scheduling
    **Manual**: Available for immediate execution anytime
    
    ## Automatic Execution (Daily 9 AM UTC)
    
    - Respects per-source schedules
    - Won't re-process if already done today
    - Creates empty files on no-source days to mark completion
    
    ## Manual Execution Options
    
    ### 1. Standard Manual Trigger (respects schedules)
    ```
    Click "Trigger DAG" with no config
    ```
    - Runs sources scheduled for today
    - Will re-process even if already done
    
    ### 2. Specific Date Collection
    ```json
    {{"date": "2025-06-03"}}
    ```
    - Collects current jobs but files under specified date
    - Respects source schedules for that day-of-week
    
    ### 3. Force All Sources (ignore schedules)
    ```json
    {{"force_all_sources": true}}
    ```
    - Runs ALL enabled sources regardless of their schedule
    - Useful for testing or catching up
    
    ### 4. Specific Date + Force All
    ```json
    {{"date": "2025-06-03", "force_all_sources": true}}
    ```
    - Ultimate override: specific date + all sources
    
    ## Source Schedules
    ```python
    SOURCE_SCHEDULES = {{
        'hackernews': 'weekly',    # Mondays only
        # 'remoteok': 'daily',     # Every day (when enabled)
        # 'greenhouse': 'daily',   # Every day (when enabled)
    }}
    ```
    
    ## Trigger Examples
    
    **Monday Automatic**: Runs hackernews (weekly schedule)
    **Tuesday Automatic**: Skips hackernews, runs daily sources
    **Manual Monday**: Runs hackernews + any daily sources
    **Manual Tuesday**: Runs only daily sources (respects hackernews weekly)
    **Manual + force_all_sources**: Runs hackernews even on Tuesday
    
    ## Files Output
    - **job_details_YYYY-MM-DD.csv**: Jobs with skills + collected_date
    - **skills_demand_YYYY-MM-DD.csv**: Aggregated skills demand
    
    Located: `/data/job_output/YYYY-MM-DD/`
    """,
) as dag:

    collect_jobs = PythonOperator(
        task_id="collect_job_demand",
        python_callable=run_daily_job_collection,
        execution_timeout=timedelta(hours=1),
    )