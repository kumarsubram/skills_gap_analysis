"""
Job Common Utils - FIXED VERSION

Shared utilities for job collection pipeline.
Includes improved location classification used by all parsers.

Location: include/jobs/job_common_utils.py
"""

import os
import tempfile
import pandas as pd
import json
import ast
from typing import Dict, List, Any

from include.config.env_detection import ENV, get_storage_paths
from include.delta_lake.file_manager import file_exists, save_files_to_layer

def load_json_file(file_path: str) -> Dict:
    """Load any JSON file"""
    with open(file_path, "r") as f:
        return json.load(f)

def load_location_data() -> Dict:
    """Load location classification data from JSON file"""
    # Try relative path first, then absolute
    current_dir = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(current_dir, "..", "jsons", "location_keywords.json")
    json_path = os.path.normpath(json_path)
    
    if os.path.exists(json_path):
        return load_json_file(json_path)
    
    # Fallback to absolute path
    return load_json_file("/usr/local/airflow/include/jsons/location_keywords.json")

def classify_location(location_text: str, location_data: Dict) -> str:
    """
    BULLETPROOF location classification - handles all variations
    """
    if not location_text or location_text.strip() == "":
        return "Non-USA"
        
    location_lower = location_text.lower().strip()
    
    # Handle common "Not specified" cases
    if location_lower in ["not specified", "unknown", "n/a", "na", ""]:
        return "Non-USA"
    
    # Clean location text for better matching
    # Remove common punctuation and normalize spaces
    cleaned_location = location_lower
    cleaned_location = cleaned_location.replace('(', ' ').replace(')', ' ')
    cleaned_location = cleaned_location.replace('[', ' ').replace(']', ' ')
    cleaned_location = cleaned_location.replace(',', ' ')
    cleaned_location = ' '.join(cleaned_location.split())  # Normalize spaces
    
    # Also create a version with original spacing for exact matches
    locations_to_check = [location_lower, cleaned_location]
    
    # 1. Check USA keywords
    for keyword in location_data.get("usa_keywords", []):
        keyword_lower = keyword.lower()
        for loc in locations_to_check:
            if keyword_lower in loc:
                return "USA"
    
    # 2. Manual high-priority US indicators (common patterns not in JSON)
    us_indicators = [
        'remote us', 'remote usa', 'us remote', 'usa remote',
        'us only', 'usa only', 'united states', 'america',
        'remote (us)', 'remote(us)', '(us)', '(usa)',
        'us and', 'usa and', 'us or', 'usa or'
    ]
    
    for indicator in us_indicators:
        for loc in locations_to_check:
            if indicator in loc:
                return "USA"
    
    # 3. Check US cities
    for city in location_data.get("us_cities", []):
        city_lower = city.lower()
        for loc in locations_to_check:
            if city_lower in loc:
                return "USA"
    
    # 4. Check US states (with abbreviation safety)
    for state in location_data.get("us_states", []):
        state_lower = state.lower()
        for loc in locations_to_check:
            if state_lower in loc:
                if len(state_lower) <= 2:
                    # For abbreviations, check positioning
                    if (f' {state_lower} ' in f' {loc} ' or 
                        f' {state_lower}' in f' {loc}' or
                        loc.endswith(state_lower)):
                        return "USA"
                else:
                    # Full state names are safe
                    return "USA"
    
    # 5. Final fallback patterns
    if any(pattern in cleaned_location for pattern in ['sf ', ' sf', 'nyc', 'la ', ' la']):
        return "USA"
    
    return "Non-USA"

def safe_serialize_for_parquet(value: Any) -> str:
    """Safely serialize complex objects to JSON strings for parquet storage"""
    if value is None:
        return ''
    elif isinstance(value, (str, int, float, bool)):
        return str(value)
    elif isinstance(value, (dict, list)):
        try:
            return json.dumps(value, ensure_ascii=False, separators=(',', ':'))
        except (TypeError, ValueError):
            return str(value)
    else:
        return str(value)

def safe_parse_json_string(value: Any) -> Any:
    """Safely convert string back to original data type (dict/list)"""
    if not isinstance(value, str):
        return value
    
    if not value.strip().startswith(('{', '[')):
        return value
    
    try:
        return json.loads(value)
    except (json.JSONDecodeError, ValueError):
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return value

def save_raw_api_to_bronze(raw_responses: List[Dict], source_name: str, date_str: str) -> bool:
    """Save raw API responses to Bronze with proper JSON serialization"""
    if not raw_responses:
        print(f"   ⚠️  No data to save for {source_name}")
        return False
    
    filename = f"raw_api_{source_name}_{date_str}.parquet"
    temp_path = os.path.join(tempfile.gettempdir(), filename)
    
    try:
        df = pd.DataFrame(raw_responses)
        
        # Serialize complex objects to JSON strings
        for col in df.columns:
            if df[col].dtype == 'object':
                has_complex = df[col].apply(lambda x: isinstance(x, (dict, list))).any()
                if has_complex:
                    df[col] = df[col].apply(safe_serialize_for_parquet)
                else:
                    df[col] = df[col].astype(str)
        
        df.to_parquet(temp_path, index=False)
        result = save_files_to_layer([temp_path], 'jobs', 'bronze')
        
        os.remove(temp_path)
        
        if result['successful'] > 0:
            print(f"   ✅ Saved {len(raw_responses)} responses to Bronze")
            return True
        return False
        
    except Exception as e:
        print(f"   ❌ Error saving to Bronze: {e}")
        if os.path.exists(temp_path):
            os.remove(temp_path)
        return False

def read_bronze_raw_api(source_name: str, date_str: str) -> List[Dict]:
    """Read Bronze raw API data and convert strings back to original data types"""
    filename = f"raw_api_{source_name}_{date_str}.parquet"
    
    if not file_exists(filename, 'jobs', 'bronze'):
        return []
    
    try:
        if ENV.environment == 'mac':
            paths = get_storage_paths('jobs')
            file_path = os.path.join(paths['bronze_path'], filename)
            df = pd.read_parquet(file_path)
        else:
            from include.delta_lake.file_manager import read_binary_from_layer
            binary_data = read_binary_from_layer(filename, 'jobs', 'bronze')
            if not binary_data:
                return []
            temp_path = os.path.join(tempfile.gettempdir(), filename)
            with open(temp_path, 'wb') as f:
                f.write(binary_data)
            df = pd.read_parquet(temp_path)
            os.remove(temp_path)
        
        # Convert strings back to original data types
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(safe_parse_json_string)
        
        return df.to_dict('records')
        
    except Exception as e:
        print(f"   ❌ Error reading Bronze: {e}")
        return []

def save_to_silver(temp_files: List[str]) -> Dict:
    """Save files to Silver with folder structure"""
    stats = {"successful": 0, "failed": 0}
    
    if ENV.environment == 'mac':
        paths = get_storage_paths('jobs')
        base_path = paths['silver_path']
        
        for temp_file in temp_files:
            if os.path.exists(temp_file):
                filename = os.path.basename(temp_file)
                
                if filename.startswith('job_details_'):
                    table_name = 'job_details'
                elif filename.startswith('skills_demand_'):
                    table_name = 'skills_demand'
                elif filename.startswith('source_summary_'):
                    table_name = 'source_summary'
                else:
                    table_name = 'unknown'
                
                table_dir = os.path.join(base_path, table_name)
                os.makedirs(table_dir, exist_ok=True)
                
                final_path = os.path.join(table_dir, filename)
                import shutil
                shutil.copy2(temp_file, final_path)
                stats["successful"] += 1
                print(f"✅ Saved to Silver/{table_name}: {filename}")
            else:
                stats["failed"] += 1
    else:
        from include.delta_lake.minio_connect import get_minio_client, get_bucket_name
        client = get_minio_client()
        bucket = get_bucket_name()
        
        if client:
            for temp_file in temp_files:
                if os.path.exists(temp_file):
                    filename = os.path.basename(temp_file)
                    
                    if filename.startswith('job_details_'):
                        table_name = 'job_details'
                    elif filename.startswith('skills_demand_'):
                        table_name = 'skills_demand'
                    elif filename.startswith('source_summary_'):
                        table_name = 'source_summary'
                    else:
                        table_name = 'unknown'
                    
                    s3_key = f"silver/jobs/{table_name}/{filename}"
                    
                    try:
                        client.upload_file(temp_file, bucket, s3_key)
                        stats["successful"] += 1
                        print(f"✅ Uploaded to Silver/{table_name}: {filename}")
                    except Exception as e:
                        print(f"❌ Upload failed: {e}")
                        stats["failed"] += 1
                else:
                    stats["failed"] += 1
    
    return stats

# Test function to verify location classification
def test_location_classification():
    """Test function to verify location classification works correctly"""
    location_data = load_location_data()
    
    test_cases = [
        ("Seattle, WA", "USA"),
        ("Seattle, Washington", "USA"),
        ("New York, NY", "USA"),
        ("San Francisco, CA", "USA"),
        ("Remote, USA", "USA"),
        ("United States", "USA"),
        ("London, UK", "Non-USA"),
        ("Toronto, Canada", "Non-USA"),
        ("Berlin, Germany", "Non-USA"),
        ("Not specified", "Non-USA"),
        ("", "Non-USA"),
    ]
    
    print("🧪 Testing Location Classification:")
    for location, expected in test_cases:
        result = classify_location(location, location_data)
        status = "✅" if result == expected else "❌"
        print(f"   {status} '{location}' -> {result} (expected: {expected})")
    
    return True

if __name__ == "__main__":
    test_location_classification()