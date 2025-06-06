"""
GitHub Archive Data Collection Module

Handles downloading and file management for GitHub Archive data.
Includes idempotency checks and error handling.
"""

import os
import requests
import gzip


def is_date_already_processed(date_str):
    """Check if a date has already been processed (idempotent check)"""
    daily_summary_file = f"/usr/local/airflow/data/output/{date_str}/tech_trends_summary_{date_str}.csv"
    
    if os.path.exists(daily_summary_file):
        file_size = os.path.getsize(daily_summary_file)
        if file_size > 100:  # More than just headers
            print(f"✅ {date_str} already processed ({file_size} bytes)")
            return True
        else:
            print(f"⚠️  {date_str} summary too small, reprocessing...")
    
    return False


def download_github_hour(date_str, hour):
    """Download a single hour of GitHub Archive data"""
    # 🐛 FIX: Don't zero-pad the hour in the URL
    url = f"https://data.gharchive.org/{date_str}-{hour}.json.gz"
    # But keep zero-padding for local file names for consistency
    hour_str = f"{hour:02d}"
    local_path = f"/usr/local/airflow/data/gharchive/{date_str}/{date_str}-{hour_str}.json.gz"
    
    # Create directory if needed
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    
    # Skip if already downloaded (idempotent)
    if os.path.exists(local_path):
        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        print(f"   ♻️  Hour {hour_str}: Using cached file ({size_mb:.1f}MB)")
        return local_path, True
    
    # Download the file
    print(f"   📥 Hour {hour_str}: Downloading {url}")
    try:
        response = requests.get(url, timeout=30)
        if response.status_code == 200:
            with open(local_path, "wb") as f:
                f.write(response.content)
            size_mb = len(response.content) / (1024 * 1024)
            print(f"   ✅ Hour {hour_str}: Downloaded {size_mb:.1f}MB")
            return local_path, False
        else:
            print(f"   ❌ Hour {hour_str}: Failed with status {response.status_code}")
            return None, False
    except Exception as e:
        print(f"   ❌ Hour {hour_str}: Error - {e}")
        return None, False


def ensure_output_directory(date_str):
    """Ensure output directory exists for a given date"""
    output_dir = f"/usr/local/airflow/data/output/{date_str}"
    os.makedirs(output_dir, exist_ok=True)
    return output_dir


def get_file_size_mb(file_path):
    """Get file size in MB, return 0 if file doesn't exist"""
    if os.path.exists(file_path):
        return os.path.getsize(file_path) / (1024 * 1024)
    return 0


def cleanup_temp_files(date_str, keep_csv=True):
    """Clean up temporary files for a date, optionally keeping CSV files"""
    gharchive_dir = f"/usr/local/airflow/data/gharchive/{date_str}"
    
    if not keep_csv:
        output_dir = f"/usr/local/airflow/data/output/{date_str}"
        if os.path.exists(output_dir):
            import shutil
            shutil.rmtree(output_dir)
            print(f"🗑️  Cleaned up output directory: {output_dir}")
    
    # Note: We keep gharchive files for caching purposes
    # They can be manually cleaned up if disk space is needed