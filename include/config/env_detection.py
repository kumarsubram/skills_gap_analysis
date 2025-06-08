"""
Environment Detection Module

Auto-detects Mac vs VPS environments and provides environment-specific configurations.
Clean and simple - no hardcoded cleanup decisions.
"""

import os
import socket
from typing import Dict

class EnvironmentDetector:
    """Detect environment and provide appropriate configurations"""
    
    def __init__(self):
        self.environment = self._detect_environment()
        self.config = self._get_environment_config()
    
    def _detect_environment(self) -> str:
        """Auto-detect if running on Mac (development) or VPS (production)"""
        env_type = os.getenv('AIRFLOW_ENV', 'mac').lower()
        
        if env_type in ['vps', 'prod', 'production']:
            return 'vps'
        else:
            return 'mac'
    
    def _get_environment_config(self) -> Dict:
        """Get environment-specific configuration"""
        
        base_config = {
            'airflow_home': '/usr/local/airflow',
            'keywords_file': '/usr/local/airflow/include/jsons/tech_keywords.json',
        }
        
        if self.environment == 'mac':
            return {
                **base_config,
                'use_minio': False,
                'bronze_base_dir': '/usr/local/airflow/data/bronze',
                'silver_base_dir': '/usr/local/airflow/data/silver',
                'gold_base_dir': '/usr/local/airflow/data/gold',
                'storage_backend': 'local',
            }
        
        else:  # VPS
            vps_ip = os.getenv('VPS_IP', 'localhost')
            base_config.update({
                'use_minio': True,
                'minio_bucket': 'delta-lake',
                'minio_endpoint': f'{vps_ip}:9000',
                'storage_backend': 'minio',
                'bronze_base_prefix': 'bronze', 
                'silver_base_prefix': 'silver',
                'gold_base_prefix': 'gold'
            })
            return base_config     
    
    def get_storage_paths(self, data_source: str = 'github') -> Dict[str, str]:
        """Get environment-appropriate storage paths for a given date and data source"""
        
        if self.environment == 'mac':
            return {
                'bronze_path': f"{self.config['bronze_base_dir']}/{data_source}",
                'silver_path': f"{self.config['silver_base_dir']}/{data_source}",
                'gold_path': f"{self.config['gold_base_dir']}/{data_source}",
            }
        
        else:  # VPS
            return {
                'bronze_path': f"s3://{self.config['minio_bucket']}/{self.config['bronze_base_prefix']}/{data_source}",
                'silver_path': f"s3://{self.config['minio_bucket']}/{self.config['silver_base_prefix']}/{data_source}",
                'gold_path': f"s3://{self.config['minio_bucket']}/{self.config['gold_base_prefix']}/{data_source}",
            }
    
    def get_summary(self) -> Dict:
        """Get environment summary for logging"""
        return {
            'environment': self.environment,
            'hostname': socket.gethostname(),
            'storage_backend': self.config['storage_backend'],
            'use_minio': self.config.get('use_minio', False),
        }

# Global instance
ENV = EnvironmentDetector()

def get_environment() -> str:
    """Get current environment (mac/vps)"""
    return ENV.environment

def get_config() -> Dict:
    """Get current environment configuration"""
    return ENV.config

def get_storage_paths(data_source: str = 'github') -> Dict[str, str]:
    """Get storage paths for data source"""
    return ENV.get_storage_paths(data_source)

def print_environment_info():
    """Print environment detection results"""
    summary = ENV.get_summary()
    print("🔍 ENVIRONMENT DETECTION")
    print("=" * 40)
    print(f"Environment: {summary['environment'].upper()}")
    print(f"Hostname: {summary['hostname']}")
    print(f"Storage Backend: {summary['storage_backend']}")
    print(f"Use MinIO: {summary['use_minio']}")
    print("=" * 40)

if __name__ == "__main__":
    # Test environment detection
    print_environment_info()

    # Test path generation  
    paths = get_storage_paths()  # <- Use default 'github'
    print("\nTest paths:")
    for key, path in paths.items():
        print(f"  {key}: {path}")