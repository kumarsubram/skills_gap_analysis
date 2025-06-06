"""
Connection Management - FIXED VERSION
=====================================

Utilities for managing Airflow connections to external systems.
Uses connections configured in airflow_settings.yaml.
"""

from airflow.hooks.base import BaseHook
from ..config.settings import SkillsGapConfig
import json
import logging
from typing import Dict

def get_kafka_connection() -> Dict[str, str]:
    """
    Get Kafka connection details from Airflow connection.
    Uses the kafka_default connection configured in airflow_settings.yaml.
    
    Returns:
        Dict containing Kafka producer configuration
    """
    try:
        kafka_conn = BaseHook.get_connection('kafka_default')
        extra_config = json.loads(kafka_conn.extra) if kafka_conn.extra else {}
        
        # Debug logging to see what we're getting
        logging.info(f"🔍 Kafka connection - Host: {kafka_conn.host}, Port: {kafka_conn.port}")
        logging.info(f"🔍 Kafka connection - Extra: {extra_config}")
        
        # FIXED: Ensure bootstrap.servers is always set correctly
        # Priority: 1) From extra config, 2) From host:port, 3) Fallback
        if 'bootstrap.servers' in extra_config:
            bootstrap_servers = extra_config['bootstrap.servers']
        elif kafka_conn.host and kafka_conn.port:
            bootstrap_servers = f"{kafka_conn.host}:{kafka_conn.port}"
        else:
            bootstrap_servers = SkillsGapConfig.KAFKA_BOOTSTRAP_SERVERS
        
        # Build base configuration
        config = {
            'bootstrap.servers': bootstrap_servers,
            'client.id': extra_config.get('client.id', f'{SkillsGapConfig.PROJECT_NAME}-default'),
        }
        
        # Add any additional Kafka configs from connection extra
        # Skip bootstrap.servers and client.id as we've already handled them
        for key, value in extra_config.items():
            if key not in ['client.id', 'bootstrap.servers']:
                config[key] = value
        
        # CRITICAL: Log the exact config that will be used
        logging.info(f"✅ Kafka connection configured with bootstrap.servers: {config['bootstrap.servers']}")
        logging.info(f"🔧 Complete config: {config}")
        
        # Validate that bootstrap.servers is not empty
        if not config['bootstrap.servers'] or config['bootstrap.servers'].strip() == '':
            raise ValueError("bootstrap.servers cannot be empty")
        
        return config
        
    except Exception as e:
        logging.error(f"❌ Failed to get Kafka connection: {str(e)}")
        import traceback
        logging.error(f"📋 Full traceback: {traceback.format_exc()}")
        
        # Enhanced fallback with validation
        fallback_config = {
            'bootstrap.servers': SkillsGapConfig.KAFKA_BOOTSTRAP_SERVERS,
            'client.id': f'{SkillsGapConfig.PROJECT_NAME}-fallback'
        }
        logging.warning(f"🔄 Using fallback config: {fallback_config}")
        return fallback_config

def get_github_connection() -> Dict[str, any]:
    """
    Get GitHub API connection details from Airflow connection.
    Uses the github_api connection configured in airflow_settings.yaml.
    
    Returns:
        Dict containing base_url and headers for GitHub API requests
    """
    try:
        github_conn = BaseHook.get_connection('github_api')
        extra_config = json.loads(github_conn.extra) if github_conn.extra else {}
        
        headers = {
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': f'{SkillsGapConfig.PROJECT_NAME}/1.0'
        }
        
        # Add authorization from connection extra
        if 'Authorization' in extra_config:
            headers['Authorization'] = extra_config['Authorization']
        elif SkillsGapConfig.GITHUB_TOKEN:
            headers['Authorization'] = f'Bearer {SkillsGapConfig.GITHUB_TOKEN}'
        
        logging.info(f"✅ GitHub connection configured: {github_conn.host}")
        return {
            'base_url': github_conn.host,
            'headers': headers
        }
        
    except Exception as e:
        logging.error(f"❌ Failed to get GitHub connection: {str(e)}")
        # Fallback to variables
        return {
            'base_url': 'https://api.github.com',
            'headers': {
                'Accept': 'application/vnd.github.v3+json',
                'User-Agent': f'{SkillsGapConfig.PROJECT_NAME}/1.0',
                'Authorization': f'Bearer {SkillsGapConfig.GITHUB_TOKEN}'
            }
        }

def test_connections():
    """
    Test all configured connections for debugging.
    
    Returns:
        Dict with connection test results
    """
    results = {}
    
    # Test Kafka connection
    try:
        kafka_config = get_kafka_connection()
        results['kafka'] = {
            'status': 'success',
            'bootstrap_servers': kafka_config.get('bootstrap.servers'),
            'client_id': kafka_config.get('client.id'),
            'config_keys': list(kafka_config.keys())
        }
        
        # Additional validation
        if not kafka_config.get('bootstrap.servers'):
            results['kafka']['status'] = 'failed'
            results['kafka']['error'] = 'bootstrap.servers is empty or None'
            
    except Exception as e:
        results['kafka'] = {
            'status': 'failed', 
            'error': str(e)
        }
    
    # Test GitHub connection
    try:
        github_config = get_github_connection()
        results['github'] = {
            'status': 'success',
            'base_url': github_config.get('base_url'),
            'has_auth': 'Authorization' in github_config.get('headers', {})
        }
    except Exception as e:
        results['github'] = {
            'status': 'failed',
            'error': str(e)
        }
    
    return results