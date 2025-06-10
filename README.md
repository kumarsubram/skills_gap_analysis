# Jobs Collection Pipeline

A modular, idempotent data pipeline for collecting job postings from multiple sources using Bronze→Silver architecture.

## 🏗️ Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   RAW API       │    │   STRUCTURED    │    │   ANALYTICS     │
│   RESPONSES     │    │   JOB DATA      │    │   READY DATA    │
│                 │    │                 │    │                 │
│   🥉 BRONZE     │───▶│   🥈 SILVER     │───▶│   🥇 GOLD       │
│                 │    │                 │    │                 │
│ • Never deleted │    │ • Regenerated   │    │ • Aggregated    │
│ • Preserves     │    │ • Standardized  │    │ • Enriched      │
│   original data │    │ • Skills tagged │    │ • Report ready  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## 📁 Component Structure

```
include/jobs/
├── daily_jobs_collector.py          # Main Airflow DAG
├── jobs_bronze_to_silver.py         # Bronze→Silver orchestrator
├── job_common_utils.py               # Shared utilities
├── skills_extractor.py               # Skills detection engine
│
├── hackernews_collector.py           # HN API collector
├── hackernews_parser.py              # HN data parser
│
├── greenhouse_collector.py           # Greenhouse API collector
├── greenhouse_parser.py              # Greenhouse data parser
│
└── [future_source]_collector.py     # New source collector
└── [future_source]_parser.py        # New source parser
```

## 🔧 Core Components

### 1. Main DAG (`daily_jobs_collector.py`)
- **Responsibility**: Workflow orchestration and idempotency control
- **Key Features**:
  - True idempotency (skips entire DAG if Silver exists)
  - Source scheduling (daily/weekly per source)
  - Manual trigger support with force flags
  - Bronze preservation (never deletes raw data)

### 2. Bronze→Silver Pipeline (`jobs_bronze_to_silver.py`)
- **Responsibility**: Parse raw API data into structured format
- **Process**:
  1. Load raw API responses from Bronze
  2. Parse each source using dedicated parsers
  3. Extract skills from job descriptions
  4. Create standardized Silver files

### 3. Shared Utilities (`job_common_utils.py`)
- **Functions**:
  - `load_location_data()` - Load location classification rules
  - `classify_location()` - Classify jobs as USA/Non-USA
  - `save_raw_api_to_bronze()` - Store raw API responses
  - `read_bronze_raw_api()` - Read raw data for parsing
  - `save_to_silver()` - Save structured data with folder organization

### 4. Skills Engine (`skills_extractor.py`)
- **Responsibility**: Extract technology skills from job descriptions
- **Features**:
  - Regex-based keyword matching
  - Skill categorization (language, framework, tool)
  - Frequency counting and aggregation

## 📊 Data Flow

### Bronze Layer (Raw API Responses)
```
bronze/jobs/
├── raw_api_hackernews_2025-06-09.parquet
├── raw_api_greenhouse_2025-06-09.parquet
└── raw_api_[source]_[date].parquet
```

**Schema**: Preserves original API response structure
- `api_type` - Type of API call (user_data, job_comment, company_jobs)
- `url` - Original API endpoint
- `status_code` - HTTP response code
- `response_data` - Raw JSON response
- `collected_at` - Collection timestamp
- `collection_date` - Processing date

### Silver Layer (Structured Job Data)
```
silver/jobs/
├── job_details/
│   └── job_details_2025-06-09.parquet
├── skills_demand/
│   └── skills_demand_2025-06-09.parquet
└── source_summary/
    └── source_summary_2025-06-09.parquet
```

**job_details** schema:
- `job_id` - Unique identifier
- `source` - Data source (hackernews, greenhouse)
- `title` - Job title
- `company` - Company name
- `location` - Job location
- `location_type` - USA/Non-USA classification
- `url` - Job posting URL
- `description` - Job description (truncated)
- `skills_found` - Comma-separated skills
- `skill_count` - Number of skills detected

## 🚀 Adding New Sources

### Step 1: Create Collector (`[source]_collector.py`)

```python
"""
[Source Name] Raw API Collector

Collects RAW API responses from [Source]. 
NO PARSING - just stores raw API data.
"""

import requests
import time
from typing import List, Dict, Any
from datetime import datetime, timezone

def collect_[source]_jobs(date_str: str) -> List[Dict[str, Any]]:
    """Collect RAW [Source] API responses"""
    print(f"   🎯 Starting {source} RAW API collection...")
    
    raw_responses = []
    
    # Your API collection logic here
    try:
        # Example API call
        response = requests.get("https://api.example.com/jobs", timeout=10)
        
        raw_responses.append({
            'api_type': 'job_listings',  # Describe what this call returns
            'url': "https://api.example.com/jobs",
            'status_code': response.status_code,
            'response_data': response.json() if response.status_code == 200 else None,
            'error_text': response.text if response.status_code != 200 else None,
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
        
    except Exception as e:
        raw_responses.append({
            'api_type': 'job_listings',
            'url': "https://api.example.com/jobs",
            'status_code': None,
            'response_data': None,
            'error_text': str(e),
            'collected_at': datetime.now(timezone.utc).isoformat(),
            'collection_date': date_str
        })
    
    print(f"   ✅ Collected {len(raw_responses)} raw API responses")
    return raw_responses
```

### Step 2: Create Parser (`[source]_parser.py`)

```python
"""
[Source Name] Parser

Parses raw [Source] API data from Bronze into structured job data.
Handles only [Source]-specific parsing logic.
"""

from typing import Dict, List
from include.jobs.job_common_utils import classify_location

def parse_[source]_raw_to_jobs(raw_responses: List[Dict], location_data: Dict) -> List[Dict]:
    """Parse raw [Source] API responses into structured job data"""
    jobs = []
    
    # Filter for successful responses
    valid_responses = [
        r for r in raw_responses 
        if r.get('api_type') == 'job_listings'  # Match your collector's api_type
        and r.get('status_code') == 200 
        and r.get('response_data')
    ]
    
    print(f"   📋 Parsing {len(valid_responses)} {source} responses...")
    
    for response in valid_responses:
        try:
            data = response['response_data']
            
            # Parse your API response structure
            for job_data in data.get('jobs', []):  # Adjust to your API structure
                job = {
                    'job_id': f"{source}_{job_data.get('id', '')}",
                    'source': '[source]',
                    'title': job_data.get('title', ''),
                    'company': job_data.get('company', ''),
                    'location': job_data.get('location', ''),
                    'location_type': classify_location(job_data.get('location', ''), location_data),
                    'url': job_data.get('url', ''),
                    'description': job_data.get('description', '')[:1000],
                    'collected_date': response.get('collection_date', '')
                }
                jobs.append(job)
                
        except Exception as e:
            print(f"   ⚠️  Error parsing response: {e}")
            continue
    
    print(f"   ✅ Parsed {len(jobs)} {source} jobs")
    return jobs
```

### Step 3: Register Source in DAG

Update `daily_jobs_collector.py`:

```python
# Add to SOURCE_SCHEDULES
SOURCE_SCHEDULES = {
    'hackernews': 'weekly',    # Mondays only
    'greenhouse': 'daily',     # Every day
    '[source]': 'daily',       # Add your source with schedule
}
```

### Step 4: Register Parser

Update `jobs_bronze_to_silver.py`:

```python
# Add import
from include.jobs.[source]_parser import parse_[source]_raw_to_jobs

# Add to parse_all_sources_to_jobs() function
def parse_all_sources_to_jobs(date_str: str) -> List[Dict]:
    """Parse all Bronze sources into structured job data"""
    location_data = load_location_data()
    all_jobs = []
    
    # Existing parsers...
    
    # Add your new parser
    source_raw_data = read_bronze_raw_api('[source]', date_str)
    if source_raw_data:
        print("📡 Processing [Source]...")
        source_jobs = parse_[source]_raw_to_jobs(source_raw_data, location_data)
        all_jobs.extend(source_jobs)
    
    return all_jobs
```

## ⚡ Pipeline Operations

### Daily Scheduled Run
```bash
# Runs automatically at 9 AM UTC
# Collects only sources scheduled for today
# Skips if Silver already exists (idempotent)
```

### Manual Triggers

**Specific date:**
```json
{"date": "2025-06-09"}
```

**Force recollection (overwrites Bronze):**
```json
{"force_recollection": true, "date": "2025-06-09"}
```

**Regenerate Silver only:**
```json
{"force_reprocessing": true, "date": "2025-06-09"}
```

**Parse only (no API calls):**
```json
{"parse_only": true, "date": "2025-06-09"}
```

**All sources (ignore schedule):**
```json
{"force_all_sources": true}
```

## 🔄 Idempotency Features

### Bronze Layer Protection
- **Never Deleted**: Raw API responses preserved forever
- **Selective Collection**: Only collects missing sources
- **Overwrite Protection**: Requires `force_recollection` flag

### Silver Layer Regeneration
- **Smart Skipping**: Skips entire DAG if Silver exists
- **Force Flags**: Manual override capabilities
- **Atomic Processing**: All-or-nothing Silver generation

## 📋 Configuration Files

### Required JSON Files

**`/usr/local/airflow/include/jsons/tech_keywords.json`**
```json
{
  "python": {"type": "language", "language": "python"},
  "react": {"type": "framework", "language": "javascript"},
  "docker": {"type": "tool", "category": "devops"}
}
```

**`/usr/local/airflow/include/jsons/location_keywords.json`**
```json
{
  "usa_keywords": ["remote", "usa", "united states", "america"],
  "us_states": ["ca", "ny", "tx", "florida", "california"],
  "us_cities": ["san francisco", "new york", "austin", "seattle"]
}
```

**`/usr/local/airflow/include/jsons/greenhouse_companies.json`**
```json
[
  "company-slug-1",
  "company-slug-2",
  "company-slug-3"
]
```

## 🌍 Environment Support

### Mac Development
- **Storage**: Local filesystem
- **Paths**: `/usr/local/airflow/data/{bronze,silver,gold}/jobs/`
- **Auto-detected**: When `AIRFLOW_ENV != 'vps'`

### VPS Production  
- **Storage**: MinIO (S3-compatible)
- **Paths**: `s3://delta-lake/{bronze,silver,gold}/jobs/`
- **Auto-detected**: When `AIRFLOW_ENV = 'vps'`

## 📊 Monitoring & Debugging

### Success Metrics
```
📊 Bronze Results:
   📦 Skipped (already exist): 1 sources
   🆕 Newly collected: 1247 jobs from 1 sources
   ✅ Total successful: 2/2 sources

📊 Silver Results:
   ✅ Files saved: 3
   📈 Total jobs: 1650
   🔍 Skills found: 4521 mentions, 167 unique
```

### Common Issues

**No Bronze data found:**
- Check source schedules in `SOURCE_SCHEDULES`
- Verify API endpoints are accessible
- Check date format in manual triggers

**Skills extraction failed:**
- Verify `tech_keywords.json` exists and is valid JSON
- Check file permissions

**Silver save failed:**
- Mac: Check directory permissions
- VPS: Verify MinIO credentials and connectivity

## 🔮 Future Enhancements

### Planned Features
- [ ] Gold layer aggregations (weekly/monthly trends)
- [ ] Salary extraction and normalization
- [ ] Company size/industry enrichment
- [ ] ML-based job categorization
- [ ] Real-time streaming ingestion

### Extension Points
- **Custom Skills**: Add domain-specific keywords
- **Location Intelligence**: Enhanced geo-classification
- **Data Quality**: Duplicate detection and cleansing
- **Rate Limiting**: Adaptive API throttling
- **Alerting**: Data quality and freshness monitoring

---

## 🚀 Quick Start

1. **Add your source**: Create collector and parser files
2. **Register in DAG**: Update `SOURCE_SCHEDULES` and parsing logic  
3. **Test manually**: Use `{"force_all_sources": true}` trigger
4. **Schedule**: Set daily/weekly schedule for your source
5. **Monitor**: Check Bronze→Silver success rates

The pipeline is designed to be **extensible**, **reliable**, and **maintainable**. Each new source follows the same pattern, making it easy to scale to dozens of job boards.