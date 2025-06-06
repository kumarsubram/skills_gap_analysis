"""
Rate Limiting Utilities
=======================

Utilities for managing API rate limits for multiple APIs (GitHub, LinkedIn, etc.).
Supports configurable rate limit buffers per API type.
"""

import time
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict, Any
from ..config.settings import SkillsGapConfig

def check_rate_limit_safety(rate_remaining: int, rate_reset: int = None, 
                           rate_limit_buffer: float = None, 
                           rate_limit_per_hour: int = None,
                           api_type: str = "github") -> bool:
    """
    Check if we can safely make more API requests based on rate limit buffer.
    
    Args:
        rate_remaining: Remaining requests in current window
        rate_reset: Unix timestamp when rate limit resets (optional)
        rate_limit_buffer: Custom buffer percentage (0.0-1.0)
        rate_limit_per_hour: Custom hourly rate limit
        api_type: API type for default settings ('github', 'linkedin', 'stackoverflow')
        
    Returns:
        True if safe to continue, False if should stop/slow down
    """
    # Use provided values or defaults based on API type
    if rate_limit_buffer is None:
        if api_type == "github":
            rate_limit_buffer = SkillsGapConfig.GITHUB_RATE_LIMIT_BUFFER
        elif api_type == "linkedin":
            rate_limit_buffer = SkillsGapConfig.LINKEDIN_RATE_LIMIT_BUFFER
        elif api_type == "stackoverflow":
            rate_limit_buffer = SkillsGapConfig.STACKOVERFLOW_RATE_LIMIT_BUFFER
        else:
            rate_limit_buffer = SkillsGapConfig.GITHUB_RATE_LIMIT_BUFFER
    
    if rate_limit_per_hour is None:
        if api_type == "github":
            rate_limit_per_hour = SkillsGapConfig.GITHUB_RATE_LIMIT_PER_HOUR
        elif api_type == "linkedin":
            rate_limit_per_hour = 1000  # Adjust based on actual LinkedIn limits
        elif api_type == "stackoverflow":
            rate_limit_per_hour = 300   # Adjust based on actual StackOverflow limits
        else:
            rate_limit_per_hour = SkillsGapConfig.GITHUB_RATE_LIMIT_PER_HOUR
    
    # Calculate minimum requests we should keep in reserve
    buffer_requests = int(rate_limit_per_hour * (1 - rate_limit_buffer))
    
    if rate_remaining <= buffer_requests:
        logging.warning(f"⚠️ {api_type.upper()} rate limit buffer reached!")
        logging.warning(f"  Remaining: {rate_remaining}")
        logging.warning(f"  Buffer threshold: {buffer_requests}")
        logging.warning(f"  Rate limit buffer: {rate_limit_buffer * 100}%")
        
        if rate_reset:
            reset_time = datetime.fromtimestamp(rate_reset, tz=timezone.utc)
            logging.warning(f"  Rate limit resets at: {reset_time}")
        
        return False
    
    logging.debug(f"🔢 {api_type.upper()} rate limit OK: {rate_remaining} remaining, buffer: {buffer_requests}")
    return True

def calculate_safe_interval(rate_remaining: int, rate_reset: int, 
                           desired_requests: int,
                           rate_limit_buffer: float = None,
                           rate_limit_per_hour: int = None,
                           api_type: str = "github") -> float:
    """
    Calculate a safe interval between requests to avoid hitting rate limits.
    
    Args:
        rate_remaining: Remaining requests in current window
        rate_reset: Unix timestamp when rate limit resets
        desired_requests: Number of requests we want to make
        rate_limit_buffer: Custom buffer percentage
        rate_limit_per_hour: Custom hourly rate limit
        api_type: API type for default settings
        
    Returns:
        Recommended interval in seconds between requests
    """
    # Use defaults if not provided
    if rate_limit_buffer is None:
        rate_limit_buffer = getattr(SkillsGapConfig, f"{api_type.upper()}_RATE_LIMIT_BUFFER", 
                                   SkillsGapConfig.GITHUB_RATE_LIMIT_BUFFER)
    
    if rate_limit_per_hour is None:
        rate_limit_per_hour = getattr(SkillsGapConfig, f"{api_type.upper()}_RATE_LIMIT_PER_HOUR",
                                     SkillsGapConfig.GITHUB_RATE_LIMIT_PER_HOUR)
    
    # Time remaining until rate limit reset
    now = datetime.now(timezone.utc).timestamp()
    time_remaining = max(rate_reset - now, 0)
    
    # Requests we can safely use (respecting buffer)
    buffer_requests = int(rate_limit_per_hour * (1 - rate_limit_buffer))
    safe_requests = max(rate_remaining - buffer_requests, 1)
    
    # If we want more requests than we have available
    if desired_requests > safe_requests:
        logging.warning(f"⚠️ Desired requests ({desired_requests}) exceed safe limit ({safe_requests}) for {api_type}")
        desired_requests = safe_requests
    
    # Calculate safe interval
    if desired_requests <= 0:
        return time_remaining  # Wait until reset
    
    safe_interval = time_remaining / desired_requests
    
    logging.info(f"📊 {api_type.upper()} rate limit calculation:")
    logging.info(f"  Time until reset: {time_remaining:.0f}s")
    logging.info(f"  Safe requests available: {safe_requests}")
    logging.info(f"  Desired requests: {desired_requests}")
    logging.info(f"  Recommended interval: {safe_interval:.1f}s")
    
    return safe_interval

def adaptive_sleep(rate_remaining: int, rate_reset: int, 
                  current_interval: float, target_requests_per_hour: int = None,
                  rate_limit_buffer: float = None, 
                  rate_limit_per_hour: int = None,
                  api_type: str = "github") -> float:
    """
    Adaptively adjust sleep time based on current rate limit status.
    
    Args:
        rate_remaining: Remaining requests in current window
        rate_reset: Unix timestamp when rate limit resets
        current_interval: Current sleep interval being used
        target_requests_per_hour: Target requests per hour (optional)
        rate_limit_buffer: Custom buffer percentage
        rate_limit_per_hour: Custom hourly rate limit
        api_type: API type for default settings
        
    Returns:
        Recommended sleep time for next iteration
    """
    # Use defaults if not provided
    if rate_limit_buffer is None:
        rate_limit_buffer = getattr(SkillsGapConfig, f"{api_type.upper()}_RATE_LIMIT_BUFFER", 
                                   SkillsGapConfig.GITHUB_RATE_LIMIT_BUFFER)
    
    if rate_limit_per_hour is None:
        rate_limit_per_hour = getattr(SkillsGapConfig, f"{api_type.upper()}_RATE_LIMIT_PER_HOUR",
                                     SkillsGapConfig.GITHUB_RATE_LIMIT_PER_HOUR)
    
    if target_requests_per_hour is None:
        # Default to using 90% of available quota within buffer
        buffer_requests = int(rate_limit_per_hour * (1 - rate_limit_buffer))
        available_requests = rate_limit_per_hour - buffer_requests
        target_requests_per_hour = int(available_requests * 0.9)
    
    # Calculate optimal interval for target rate
    optimal_interval = 3600 / target_requests_per_hour  # seconds between requests
    
    # Time remaining until reset
    now = datetime.now(timezone.utc).timestamp()
    time_remaining = max(rate_reset - now, 0)
    
    # Check if we're using rate limit too aggressively
    buffer_requests = int(rate_limit_per_hour * (1 - rate_limit_buffer))
    
    if rate_remaining <= buffer_requests:
        # We're in buffer zone - slow down significantly
        recommended_sleep = time_remaining / max(rate_remaining, 1)
        logging.warning(f"🐌 {api_type.upper()} in rate limit buffer - slowing down to {recommended_sleep:.1f}s intervals")
        return recommended_sleep
    
    elif rate_remaining < (rate_limit_per_hour * 0.5):
        # Less than 50% remaining - use conservative interval
        recommended_sleep = optimal_interval * 1.5
        logging.info(f"🚶 {api_type.upper()} <50% rate limit remaining - using conservative interval: {recommended_sleep:.1f}s")
        return recommended_sleep
    
    else:
        # Plenty of quota remaining - use optimal interval
        logging.info(f"🏃 {api_type.upper()} rate limit healthy - using optimal interval: {optimal_interval:.1f}s")
        return optimal_interval

def wait_for_rate_limit_reset(rate_reset: int, max_wait_minutes: int = 60, api_type: str = "github"):
    """
    Wait for rate limit to reset if we've hit the buffer.
    
    Args:
        rate_reset: Unix timestamp when rate limit resets
        max_wait_minutes: Maximum time to wait in minutes
        api_type: API type for logging
    """
    now = datetime.now(timezone.utc).timestamp()
    wait_time = rate_reset - now
    max_wait_seconds = max_wait_minutes * 60
    
    if wait_time <= 0:
        logging.info(f"✅ {api_type.upper()} rate limit has already reset")
        return
    
    if wait_time > max_wait_seconds:
        logging.warning(f"⚠️ {api_type.upper()} rate limit reset in {wait_time/60:.1f} minutes - exceeds max wait time")
        raise Exception(f"{api_type.upper()} rate limit reset time ({wait_time/60:.1f}m) exceeds maximum ({max_wait_minutes}m)")
    
    logging.info(f"⏰ Waiting {wait_time/60:.1f} minutes for {api_type.upper()} rate limit reset...")
    
    # Sleep in smaller chunks and log progress
    chunk_size = min(300, wait_time / 4)  # 5 minutes or 1/4 of wait time, whichever is smaller
    
    while wait_time > 0:
        sleep_time = min(chunk_size, wait_time)
        time.sleep(sleep_time)
        wait_time -= sleep_time
        
        if wait_time > 0:
            logging.info(f"⏳ Still waiting for {api_type.upper()}... {wait_time/60:.1f} minutes remaining")
    
    logging.info(f"✅ {api_type.upper()} rate limit should be reset now")

def log_rate_limit_status(rate_remaining: int, rate_reset: int, 
                         rate_limit: int = None, context: str = "",
                         api_type: str = "github"):
    """
    Log detailed rate limit status for monitoring.
    
    Args:
        rate_remaining: Remaining requests
        rate_reset: Unix timestamp when resets
        rate_limit: Total rate limit (optional)
        context: Additional context for logging
        api_type: API type for settings and logging
    """
    if rate_limit is None:
        rate_limit = getattr(SkillsGapConfig, f"{api_type.upper()}_RATE_LIMIT_PER_HOUR",
                            SkillsGapConfig.GITHUB_RATE_LIMIT_PER_HOUR)
    
    rate_limit_buffer = getattr(SkillsGapConfig, f"{api_type.upper()}_RATE_LIMIT_BUFFER", 
                               SkillsGapConfig.GITHUB_RATE_LIMIT_BUFFER)
    
    # Calculate percentages and times
    percentage_used = ((rate_limit - rate_remaining) / rate_limit) * 100
    percentage_remaining = (rate_remaining / rate_limit) * 100
    
    reset_time = datetime.fromtimestamp(rate_reset, tz=timezone.utc)
    time_until_reset = reset_time - datetime.now(timezone.utc)
    
    buffer_threshold = int(rate_limit * (1 - rate_limit_buffer))
    buffer_percentage = ((rate_limit - buffer_threshold) / rate_limit) * 100
    
    # Determine status
    if rate_remaining <= buffer_threshold:
        status = "🔴 BUFFER ZONE"
    elif rate_remaining < (rate_limit * 0.5):
        status = "🟡 CAUTION"
    else:
        status = "🟢 HEALTHY"
    
    prefix = f"[{context}] " if context else ""
    
    logging.info(f"📊 {prefix}{api_type.upper()} Rate Limit Status: {status}")
    logging.info(f"  Used: {rate_limit - rate_remaining}/{rate_limit} ({percentage_used:.1f}%)")
    logging.info(f"  Remaining: {rate_remaining} ({percentage_remaining:.1f}%)")
    logging.info(f"  Buffer threshold: {buffer_threshold} ({buffer_percentage:.1f}%)")
    logging.info(f"  Resets in: {time_until_reset}")
    logging.info(f"  Reset time: {reset_time}")

class RateLimitTracker:
    """
    Class to track rate limit usage over time for better planning.
    Supports multiple API types.
    """
    
    def __init__(self, api_type: str = "github"):
        self.api_type = api_type
        self.measurements = []
        self.start_time = datetime.now(timezone.utc)
        
        # Get rate limit for this API type
        self.rate_limit_per_hour = getattr(SkillsGapConfig, f"{api_type.upper()}_RATE_LIMIT_PER_HOUR",
                                          SkillsGapConfig.GITHUB_RATE_LIMIT_PER_HOUR)
    
    def record_measurement(self, rate_remaining: int, rate_reset: int):
        """Record a rate limit measurement."""
        measurement = {
            'timestamp': datetime.now(timezone.utc),
            'rate_remaining': rate_remaining,
            'rate_reset': rate_reset,
            'rate_used': self.rate_limit_per_hour - rate_remaining
        }
        self.measurements.append(measurement)
    
    def get_usage_rate(self) -> float:
        """
        Calculate current requests per hour based on measurements.
        
        Returns:
            Current usage rate in requests per hour
        """
        if len(self.measurements) < 2:
            return 0.0
        
        first = self.measurements[0]
        last = self.measurements[-1]
        
        time_diff = (last['timestamp'] - first['timestamp']).total_seconds() / 3600  # hours
        usage_diff = last['rate_used'] - first['rate_used']
        
        if time_diff <= 0:
            return 0.0
        
        return usage_diff / time_diff
    
    def predict_time_to_buffer(self) -> Optional[float]:
        """
        Predict how long until we hit the rate limit buffer.
        
        Returns:
            Hours until buffer is reached, or None if not predictable
        """
        usage_rate = self.get_usage_rate()
        
        if usage_rate <= 0 or not self.measurements:
            return None
        
        current_remaining = self.measurements[-1]['rate_remaining']
        buffer_threshold = SkillsGapConfig.get_rate_limit_threshold(self.api_type)
        
        requests_until_buffer = current_remaining - buffer_threshold
        
        if requests_until_buffer <= 0:
            return 0.0  # Already at buffer
        
        return requests_until_buffer / usage_rate
    
    def get_summary(self) -> Dict[str, Any]:
        """Get a summary of rate limit usage."""
        if not self.measurements:
            return {'status': 'no_data', 'api_type': self.api_type}
        
        latest = self.measurements[-1]
        usage_rate = self.get_usage_rate()
        time_to_buffer = self.predict_time_to_buffer()
        
        return {
            'status': 'active',
            'api_type': self.api_type,
            'measurements_count': len(self.measurements),
            'current_remaining': latest['rate_remaining'],
            'usage_rate_per_hour': usage_rate,
            'time_to_buffer_hours': time_to_buffer,
            'tracking_duration_minutes': (datetime.now(timezone.utc) - self.start_time).total_seconds() / 60
        }