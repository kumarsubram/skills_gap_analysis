"""
Generic API Utilities
====================

Generic utilities for making safe API requests with rate limiting and error handling.
Works with any API type. Specific API processing should be in separate files.
"""

import requests
import time
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional
from .rate_limiting import check_rate_limit_safety
from ..config.settings import SkillsGapConfig

def safe_api_request(url: str, headers: Dict[str, str], 
                    api_type: str = "github", **kwargs) -> Optional[requests.Response]:
    """
    Make a safe API request with rate limiting and error handling.
    
    Args:
        url: API endpoint URL
        headers: Request headers
        api_type: Type of API for rate limiting ('github', 'linkedin', 'stackoverflow')
        **kwargs: Additional requests parameters (params, timeout, etc.)
        
    Returns:
        Response object if successful, None if failed or rate limited
    """
    for attempt in range(SkillsGapConfig.RETRY_ATTEMPTS):
        try:
            response = requests.get(url, headers=headers, timeout=30, **kwargs)
            
            # Generic rate limit checking - each API can have different headers
            rate_remaining = None
            rate_reset = None
            
            # GitHub headers
            if 'X-RateLimit-Remaining' in response.headers:
                rate_remaining = int(response.headers.get('X-RateLimit-Remaining', 0))
                rate_reset = int(response.headers.get('X-RateLimit-Reset', 0))
            
            # StackOverflow headers  
            elif 'X-Quota-Remaining' in response.headers:
                rate_remaining = int(response.headers.get('X-Quota-Remaining', 0))
                # Calculate reset time for daily quota
                rate_reset = int((datetime.now(timezone.utc).replace(hour=0, minute=0, second=0) + 
                                timedelta(days=1)).timestamp())
            
            # Check rate limits if headers were found
            if rate_remaining is not None and rate_reset is not None:
                if not check_rate_limit_safety(rate_remaining, rate_reset, api_type=api_type):
                    logging.warning(f"🛑 Stopping {api_type.upper()} requests due to rate limit buffer")
                    return None
            
            response.raise_for_status()
            return response
            
        except requests.exceptions.RequestException as e:
            logging.error(f"❌ {api_type.upper()} API request attempt {attempt + 1} failed: {str(e)}")
            if attempt < SkillsGapConfig.RETRY_ATTEMPTS - 1:
                sleep_time = 2 ** attempt  # Exponential backoff
                logging.info(f"😴 Retrying in {sleep_time} seconds...")
                time.sleep(sleep_time)
            else:
                logging.error(f"💥 All {api_type.upper()} API request attempts failed")
                
    return None

def paginated_api_request(base_url: str, headers: Dict[str, str], 
                         max_pages: int = 5, per_page: int = 100,
                         api_type: str = "github") -> List[Dict[str, Any]]:
    """
    Make paginated API requests to collect more data.
    
    Args:
        base_url: Base API endpoint URL
        headers: Request headers
        max_pages: Maximum number of pages to fetch
        per_page: Items per page
        api_type: Type of API for rate limiting
        
    Returns:
        List of all collected items across pages
    """
    all_items = []
    page = 1
    
    while page <= max_pages:
        params = {'page': page, 'per_page': per_page}
        response = safe_api_request(base_url, headers, api_type=api_type, params=params)
        
        if not response:
            logging.warning(f"❌ Failed to fetch page {page} from {api_type.upper()}")
            break
            
        items = response.json()
        
        # Handle different response formats
        if isinstance(items, list):
            data = items
        elif isinstance(items, dict) and 'items' in items:
            data = items['items']
        elif isinstance(items, dict) and 'data' in items:
            data = items['data']
        else:
            logging.warning(f"⚠️ Unexpected response format on page {page} from {api_type.upper()}")
            break
        
        if not data:
            logging.info(f"📭 No more data on page {page} from {api_type.upper()}, stopping pagination")
            break
            
        all_items.extend(data)
        logging.info(f"📄 {api_type.upper()} Page {page}: collected {len(data)} items, total: {len(all_items)}")
        
        page += 1
    
    logging.info(f"📊 {api_type.upper()} Pagination complete: {len(all_items)} total items from {page-1} pages")
    return all_items

def log_api_response_info(response: requests.Response, endpoint_name: str = "API", 
                         api_type: str = "unknown"):
    """
    Log useful information about API response for monitoring.
    
    Args:
        response: requests.Response object
        endpoint_name: Name of the API endpoint for logging
        api_type: Type of API for specific header handling
    """
    logging.info(f"📡 {api_type.upper()} {endpoint_name} Response Info:")
    logging.info(f"  Status: {response.status_code}")
    logging.info(f"  Content Length: {len(response.content)} bytes")
    
    # Log rate limit info if available
    if 'X-RateLimit-Remaining' in response.headers:
        logging.info(f"  Rate Limit Remaining: {response.headers['X-RateLimit-Remaining']}")
        logging.info(f"  Rate Limit Reset: {response.headers['X-RateLimit-Reset']}")
    
    elif 'X-Quota-Remaining' in response.headers:
        logging.info(f"  Quota Remaining: {response.headers['X-Quota-Remaining']}")
        logging.info(f"  Quota Max: {response.headers.get('X-Quota-Max', 'Unknown')}")
    
    # Response time if available
    if hasattr(response, 'elapsed'):
        logging.info(f"  Response Time: {response.elapsed.total_seconds():.2f}s")

def extract_text_content(data: Dict[str, Any], text_fields: List[str]) -> str:
    """
    Generic function to extract text content from API response for analysis.
    
    Args:
        data: API response data
        text_fields: List of field names to extract text from
        
    Returns:
        Combined text content
    """
    text_parts = []
    
    for field in text_fields:
        # Handle nested fields with dot notation (e.g., 'repo.name')
        value = data
        for key in field.split('.'):
            if isinstance(value, dict):
                value = value.get(key, '')
            else:
                value = ''
                break
        
        if value and isinstance(value, str):
            text_parts.append(value.lower())
    
    return ' '.join(text_parts)

def standardize_timestamp(timestamp_str: str, input_format: str = None) -> str:
    """
    Standardize timestamp to ISO format.
    
    Args:
        timestamp_str: Input timestamp string
        input_format: Expected input format (optional)
        
    Returns:
        ISO formatted timestamp string
    """
    if not timestamp_str:
        return datetime.now(timezone.utc).isoformat()
    
    try:
        # Try to parse as ISO format first
        if 'T' in timestamp_str and 'Z' in timestamp_str:
            return timestamp_str
        
        # Try common formats
        formats = [
            '%Y-%m-%d %H:%M:%S',
            '%Y-%m-%d',
            '%Y/%m/%d %H:%M:%S',
            '%m/%d/%Y',
        ]
        
        if input_format:
            formats.insert(0, input_format)
        
        for fmt in formats:
            try:
                dt = datetime.strptime(timestamp_str, fmt)
                return dt.replace(tzinfo=timezone.utc).isoformat()
            except ValueError:
                continue
        
        # If all else fails, return current time
        logging.warning(f"Could not parse timestamp: {timestamp_str}")
        return datetime.now(timezone.utc).isoformat()
        
    except Exception as e:
        logging.error(f"Error standardizing timestamp {timestamp_str}: {e}")
        return datetime.now(timezone.utc).isoformat()