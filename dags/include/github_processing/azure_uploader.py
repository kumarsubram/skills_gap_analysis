"""
Azure Storage Upload Module

Handles uploading processed CSV files to Azure Data Lake Storage Gen2
for the Silver layer in the Delta Lake architecture.
"""

import os
import subprocess
from typing import List, Dict, Optional


def get_azure_credentials():
    """Get Azure storage credentials from environment variables"""
    storage_account = os.getenv("AZURE_STORAGE_ACCOUNT")
    storage_key = os.getenv("AZURE_STORAGE_KEY")
    
    if not storage_account or not storage_key:
        raise ValueError(
            "Azure credentials not found. Please set AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY "
            "environment variables in your Airflow configuration."
        )
    
    return storage_account, storage_key


def is_date_uploaded_to_azure(date_str: str) -> bool:
    """Check if all CSV files for a date are already uploaded to Azure Silver layer"""
    try:
        storage_account, _ = get_azure_credentials()
        
        # Check for all 4 required files in Azure
        required_files = [
            f"silver/tech_trends_summary/tech_trends_summary_{date_str}.csv",
            f"silver/hourly_trends/hourly_trends_{date_str}.csv", 
            f"silver/event_summary/event_summary_{date_str}.csv",
            f"silver/top_repos/top_repos_{date_str}.csv"
        ]
        
        for azure_path in required_files:
            # Check if blob exists using Azure CLI
            check_cmd = [
                "az", "storage", "blob", "exists",
                "--account-name", storage_account,
                "--container-name", "workspace",
                "--name", azure_path,
                "--auth-mode", "key",
                "--output", "tsv",
                "--query", "exists"
            ]
            
            result = subprocess.run(check_cmd, capture_output=True, text=True)
            if result.returncode != 0 or result.stdout.strip().lower() != "true":
                return False
        
        print(f"✅ {date_str} already uploaded to Azure Silver")
        return True
        
    except Exception as e:
        print(f"⚠️  Error checking Azure upload status for {date_str}: {e}")
        return False


def upload_file_to_azure(local_file_path: str, azure_blob_path: str) -> bool:
    """Upload a single file to Azure Blob Storage"""
    try:
        storage_account, storage_key = get_azure_credentials()
        
        if not os.path.exists(local_file_path):
            print(f"❌ Local file not found: {local_file_path}")
            return False
        
        # Upload using Azure CLI
        upload_cmd = [
            "az", "storage", "blob", "upload",
            "--account-name", storage_account,
            "--account-key", storage_key,
            "--container-name", "workspace",
            "--name", azure_blob_path,
            "--file", local_file_path,
            "--overwrite"
        ]
        
        result = subprocess.run(upload_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            file_size_mb = os.path.getsize(local_file_path) / (1024 * 1024)
            print(f"   ✅ Uploaded {os.path.basename(local_file_path)} ({file_size_mb:.1f}MB)")
            return True
        else:
            print(f"   ❌ Upload failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"   ❌ Error uploading {local_file_path}: {e}")
        return False


def upload_date_to_azure_silver(date_str: str) -> Dict[str, bool]:
    """Upload all CSV files for a single date to Azure Silver layer"""
    print(f"🔄 Uploading {date_str} to Azure Silver layer...")
    
    # Check if already uploaded (idempotent)
    if is_date_uploaded_to_azure(date_str):
        return {"already_uploaded": True}
    
    # Define file mappings: (local_file_type, azure_folder)
    file_mappings = [
        ("tech_trends_summary", "silver/tech_trends_summary/"),
        ("hourly_trends", "silver/hourly_trends/"),
        ("event_summary", "silver/event_summary/"),
        ("top_repos", "silver/top_repos/")
    ]
    
    upload_results = {}
    successful_uploads = 0
    
    for file_type, azure_folder in file_mappings:
        local_file = f"/usr/local/airflow/data/output/{date_str}/{file_type}_{date_str}.csv"
        azure_blob_path = f"{azure_folder}{file_type}_{date_str}.csv"
        
        if os.path.exists(local_file):
            success = upload_file_to_azure(local_file, azure_blob_path)
            upload_results[file_type] = success
            if success:
                successful_uploads += 1
        else:
            print(f"   ⚠️  Local file not found: {local_file}")
            upload_results[file_type] = False
    
    upload_results["total_successful"] = successful_uploads
    upload_results["total_files"] = len(file_mappings)
    
    if successful_uploads == len(file_mappings):
        print(f"🎯 {date_str}: All {successful_uploads} files uploaded successfully")
    else:
        print(f"⚠️  {date_str}: {successful_uploads}/{len(file_mappings)} files uploaded")
    
    return upload_results


def upload_date_batch_to_azure(date_list: List[str]) -> Dict[str, Dict]:
    """Upload multiple dates to Azure Silver layer"""
    print(f"🚀 Batch uploading {len(date_list)} dates to Azure...")
    
    batch_results = {}
    total_successful_dates = 0
    
    for date_str in date_list:
        try:
            result = upload_date_to_azure_silver(date_str)
            batch_results[date_str] = result
            
            if result.get("already_uploaded") or result.get("total_successful") == result.get("total_files"):
                total_successful_dates += 1
                
        except Exception as e:
            print(f"❌ Error uploading {date_str}: {e}")
            batch_results[date_str] = {"error": str(e)}
    
    print(f"📊 Batch upload complete: {total_successful_dates}/{len(date_list)} dates successful")
    return batch_results


def verify_azure_silver_structure():
    """Verify that the Azure Silver folder structure exists"""
    try:
        storage_account, _ = get_azure_credentials()
        
        required_folders = [
            "silver/tech_trends_summary/",
            "silver/hourly_trends/", 
            "silver/event_summary/",
            "silver/top_repos/"
        ]
        
        print("🔍 Verifying Azure Silver folder structure...")
        
        for folder in required_folders:
            # List blobs with the folder prefix to check if folder exists
            list_cmd = [
                "az", "storage", "blob", "list",
                "--account-name", storage_account,
                "--container-name", "workspace",
                "--prefix", folder,
                "--auth-mode", "key",
                "--output", "tsv",
                "--query", "length(@)"
            ]
            
            result = subprocess.run(list_cmd, capture_output=True, text=True)
            if result.returncode == 0:
                print(f"   ✅ {folder} structure exists")
            else:
                print(f"   ⚠️  {folder} may not exist or is empty")
        
        return True
        
    except Exception as e:
        print(f"❌ Error verifying Azure structure: {e}")
        return False


def get_azure_silver_stats(date_str: Optional[str] = None) -> Dict:
    """Get statistics about files in Azure Silver layer"""
    try:
        storage_account, _ = get_azure_credentials()
        
        if date_str:
            # Get stats for specific date
            prefix = f"silver/"
            name_filter = f"*{date_str}*"
        else:
            # Get overall stats
            prefix = "silver/"
            name_filter = "*"
        
        list_cmd = [
            "az", "storage", "blob", "list",
            "--account-name", storage_account,
            "--container-name", "workspace", 
            "--prefix", prefix,
            "--auth-mode", "key",
            "--output", "json"
        ]
        
        result = subprocess.run(list_cmd, capture_output=True, text=True)
        
        if result.returncode == 0:
            import json
            blobs = json.loads(result.stdout)
            
            if date_str:
                # Filter blobs for specific date
                date_blobs = [b for b in blobs if date_str in b["name"]]
                return {
                    "date": date_str,
                    "file_count": len(date_blobs),
                    "total_size_mb": sum(b["properties"]["contentLength"] for b in date_blobs) / (1024 * 1024),
                    "files": [os.path.basename(b["name"]) for b in date_blobs]
                }
            else:
                # Overall stats
                return {
                    "total_files": len(blobs),
                    "total_size_mb": sum(b["properties"]["contentLength"] for b in blobs) / (1024 * 1024),
                    "folders": list(set(b["name"].split("/")[1] for b in blobs if "/" in b["name"]))
                }
        else:
            return {"error": result.stderr}
            
    except Exception as e:
        return {"error": str(e)}