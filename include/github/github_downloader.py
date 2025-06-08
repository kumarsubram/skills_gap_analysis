"""
GitHub Archive Data Downloader

Unified downloader for both Mac and VPS environments.
Downloads directly to Bronze layer without temp files.
"""

import requests
from typing import Dict
from include.config.env_detection import ENV
from include.delta_lake.file_manager import file_exists, save_binary_to_layer


def download_github_hour(date_str: str, hour: int) -> bool:
    """
    Download a single hour of GitHub Archive data directly to Bronze layer
    Unified behavior for both Mac and VPS environments
    
    Args:
        date_str: Date in YYYY-MM-DD format
        hour: Hour (0-23)
    
    Returns:
        bool: True if successful, False if failed
    """
    # GitHub Archive URL (no zero-padding for hour in URL)
    url = f"https://data.gharchive.org/{date_str}-{hour}.json.gz"
    
    # Filename with zero-padding for consistency
    filename = f"{date_str}-{hour:02d}.json.gz"
    
    # Check if file already exists in Bronze layer
    if file_exists(filename, 'github', 'bronze'):
        print(f"   ♻️  Hour {hour:02d}: Already exists in Bronze")
        return True
    
    # Download the file
    print(f"   📥 Hour {hour:02d}: Downloading from {url}")
    try:
        response = requests.get(url, timeout=60, stream=True)
        if response.status_code == 200:
            
            # Get binary data
            binary_data = response.content
            
            # Save directly to Bronze layer (works for both Mac and VPS)
            success = save_binary_to_layer(binary_data, filename, 'github', 'bronze')
            
            if success:
                size_mb = len(binary_data) / (1024 * 1024)
                print(f"   ✅ Hour {hour:02d}: Saved {size_mb:.1f}MB to Bronze")
                return True
            else:
                print(f"   ❌ Hour {hour:02d}: Failed to save to Bronze")
                return False
                
        else:
            print(f"   ❌ Hour {hour:02d}: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"   ❌ Hour {hour:02d}: {e}")
        return False


def is_hour_downloaded(date_str: str, hour: int) -> bool:
    """
    Check if a specific hour is already downloaded to Bronze layer
    Unified check for both Mac and VPS environments
    
    Args:
        date_str: Date in YYYY-MM-DD format
        hour: Hour (0-23)
    
    Returns:
        bool: True if file exists in Bronze, False otherwise
    """
    filename = f"{date_str}-{hour:02d}.json.gz"
    return file_exists(filename, 'github', 'bronze')


def download_github_date(date_str: str) -> Dict:
    """
    Download all 24 hours for a single date to Bronze layer
    Unified behavior for both Mac and VPS environments
    
    Args:
        date_str: Date in YYYY-MM-DD format
    
    Returns:
        Dict with download statistics
    """
    print(f"\n📅 DOWNLOADING ALL HOURS FOR {date_str}")
    print("=" * 50)
    print(f"Environment: {ENV.environment.upper()}")
    print(f"Storage: {ENV.config['storage_backend']}")
    
    results = {
        'date': date_str,
        'successful_downloads': 0,
        'cached_files': 0,
        'failed_downloads': 0,
        'failed_hours': []
    }
    
    for hour in range(24):
        if is_hour_downloaded(date_str, hour):
            results['cached_files'] += 1
            continue
        
        success = download_github_hour(date_str, hour)
        if success:
            results['successful_downloads'] += 1
        else:
            results['failed_downloads'] += 1
            results['failed_hours'].append(hour)
    
    total_available = results['successful_downloads'] + results['cached_files']
    
    print(f"\n📊 DOWNLOAD SUMMARY FOR {date_str}:")
    print(f"   ✅ New downloads: {results['successful_downloads']}")
    print(f"   ♻️  Cached in Bronze: {results['cached_files']}")
    print(f"   ❌ Failed: {results['failed_downloads']}")
    print(f"   📁 Total available: {total_available}/24 hours")
    
    if results['failed_hours']:
        print(f"   ⚠️  Failed hours: {results['failed_hours']}")
    
    print("=" * 50)
    return results


if __name__ == "__main__":
    # Test unified downloader
    test_date = "2024-12-31"
    test_hour = 0
    
    print(f"🧪 Testing unified GitHub downloader for {test_date}")
    print("=" * 50)
    
    # Test environment detection
    from include.config.env_detection import print_environment_info
    print_environment_info()
    
    # Test single hour
    print("\n1. Testing single hour download:")
    print(f"Checking if exists: {test_date} hour {test_hour}")
    exists = is_hour_downloaded(test_date, test_hour)
    print(f"Exists in Bronze: {exists}")
    
    if not exists:
        print(f"Downloading: {test_date} hour {test_hour}")
        success = download_github_hour(test_date, test_hour)
        print(f"Download success: {success}")
    
    # Test full date download
    print("\n2. Testing full date download:")
    date_result = download_github_date(test_date)
    print(f"\nFinal result: {date_result}")