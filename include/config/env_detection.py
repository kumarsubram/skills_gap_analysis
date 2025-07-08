"""
Environment Detection Module - MinIO S3 Everywhere!

SIMPLIFIED: Both Mac and VPS use MinIO S3 storage consistently.
Only difference is the MinIO endpoint (localhost vs VPS IP).
"""

import os
import socket
from typing import Dict

class EnvironmentDetector:
    """Detect environment and provide appropriate configurations"""
    
    def __init__(self):
        self.environment = self._detect_environment()
        self.base_path = self._get_base_path()
        self.config = self._get_environment_config()
    
    def _detect_environment(self) -> str:
        """Auto-detect if running on Mac (development) or VPS (production)"""
        env_type = os.getenv('AIRFLOW_ENV', 'mac').lower()
        
        if env_type in ['vps', 'prod', 'production']:
            return 'vps'
        else:
            return 'mac'
    
    def _get_base_path(self) -> str:
        """Get base path for current environment"""
        if self.environment == 'mac':
            # Check if running in container (dockerenv exists)
            if os.path.exists('/.dockerenv'):
                return '/opt/airflow'  # Container path
            else:
                # Local Mac development
                current_dir = os.getcwd()
                if 'airflow-spark' in current_dir:
                    return current_dir
                return os.path.join(os.path.expanduser('~'), 'airflow-spark')
        else:
            # VPS uses /usr/local/airflow  
            return '/opt/airflow'
    
    def _get_environment_config(self) -> Dict:
        """Get environment-specific configuration - MinIO everywhere!"""
        
        base_config = {
            'airflow_home': self.base_path,
            'keywords_file': os.path.join(self.base_path, 'include', 'jsons', 'tech_keywords.json'),
            'use_minio': True,
            'minio_bucket': 'delta-lake',
            'storage_backend': 'minio',  # Always MinIO!
            'bronze_base_prefix': 'bronze', 
            'silver_base_prefix': 'silver',
            'gold_base_prefix': 'gold'
        }
        
        if self.environment == 'mac':
            # Mac uses LOCAL MinIO (Docker container)
            base_config['minio_endpoint'] = 'localhost:9000'
            return base_config
        else:
            # VPS uses REMOTE MinIO
            vps_ip = os.getenv('VPS_IP', '147.93.44.215')
            base_config['minio_endpoint'] = f'{vps_ip}:9000'
            return base_config
    
    def get_storage_paths(self, data_source: str = 'github') -> Dict[str, str]:
        """Get storage paths - S3 paths for BOTH Mac and VPS"""
        
        # UNIFIED: Both environments use S3 paths!
        return {
            'bronze_path': f"s3a://{self.config['minio_bucket']}/{self.config['bronze_base_prefix']}/{data_source}",
            'silver_path': f"s3a://{self.config['minio_bucket']}/{self.config['silver_base_prefix']}/{data_source}",
            'gold_path': f"s3a://{self.config['minio_bucket']}/{self.config['gold_base_prefix']}/{data_source}",
        }
    
    def get_summary(self) -> Dict:
        """Get environment summary for logging"""
        return {
            'environment': self.environment,
            'hostname': socket.gethostname(),
            'base_path': self.base_path,
            'storage_backend': self.config['storage_backend'],
            'use_minio': self.config.get('use_minio', False),
            'minio_endpoint': self.config.get('minio_endpoint', 'unknown'),
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
    print(f"Base Path: {summary['base_path']}")
    print(f"Storage Backend: {summary['storage_backend']} (UNIFIED)")
    print(f"Use MinIO: {summary['use_minio']}")
    print(f"MinIO Endpoint: {summary['minio_endpoint']}")
    
    # Show actual storage paths
    paths = get_storage_paths()
    print("\nStorage Paths (S3 for both Mac + VPS):")
    for layer, path in paths.items():
        print(f"  {layer}: {path}")
    print("=" * 40)

if __name__ == "__main__":
    # Test environment detection
    print_environment_info()

    # Test path generation  
    paths = get_storage_paths()  # <- Use default 'github'
    print("\nTest paths:")
    for key, path in paths.items():
        print(f"  {key}: {path}")