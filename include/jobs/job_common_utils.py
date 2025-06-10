"""
Job Common Utils

Shared utilities for job collection pipeline.
Includes location classification used by all parsers.

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
    """Classify location as USA or Non-USA using location_keywords.json"""
    if not location_text:
        return "Non-USA"
        
    location_lower = location_text.lower()
    
    # Check USA keywords first
    for keyword in location_data.get("usa_keywords", []):
        if keyword in location_lower:
            return "USA"
    
    # Check states with better word boundary matching
    location_spaced = f" {location_lower} "
    for state in location_data.get("us_states", []):
        if len(state) <= 2:  # Abbreviations
            if f" {state} " in location_spaced:
                return "USA"
        else:  # Full state names
            if state in location_lower:
                return "USA"
    
    # Check cities
    for city in location_data.get("us_cities", []):
        if city in location_lower:
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