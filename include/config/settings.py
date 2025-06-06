"""
Configuration Management
========================

Centralized configuration management using Airflow Variables.
All configuration values from airflow_settings.yaml are accessed here.
Updated to support API-specific rate limiting configurations.
"""

from datetime import datetime, timedelta
from airflow.models import Variable
import logging

class SkillsGapConfig:
    """Centralized configuration management using Airflow Variables."""
    
    # GitHub Configuration
    GITHUB_TOKEN = Variable.get("github_token", default_var="")
    GITHUB_RATE_LIMIT_BUFFER = float(Variable.get("github_rate_limit_buffer", default_var="0.72"))
    GITHUB_RATE_LIMIT_PER_HOUR = int(Variable.get("github_rate_limit_per_hour", default_var="5000"))
    
    # Other API Rate Limits (for future use)
    LINKEDIN_RATE_LIMIT_BUFFER = float(Variable.get("linkedin_rate_limit_buffer", default_var="0.80"))
    STACKOVERFLOW_RATE_LIMIT_BUFFER = float(Variable.get("stackoverflow_rate_limit_buffer", default_var="0.90"))
    
    # Kafka Configuration
    KAFKA_BOOTSTRAP_SERVERS = Variable.get("kafka_bootstrap_servers", default_var="31.220.22.47:9092")
    KAFKA_TEST_TOPIC = Variable.get("kafka_test_topic", default_var="test-topic")
    KAFKA_MAIN_TOPICS = Variable.get("kafka_main_topics", default_var="github-events,job-postings,supply-metrics").split(",")
    
    # Project Configuration
    PROJECT_NAME = Variable.get("project_name", default_var="skills-gap-analysis")
    BATCH_SIZE_DEFAULT = int(Variable.get("batch_size_default", default_var="100"))
    RETRY_ATTEMPTS = int(Variable.get("retry_attempts", default_var="3"))
    
    @classmethod
    def get_kafka_topics(cls):
        """Get all Kafka topics as a list."""
        return [topic.strip() for topic in cls.KAFKA_MAIN_TOPICS]
    
    @classmethod
    def get_github_rate_limit_threshold(cls):
        """Calculate the minimum GitHub requests to keep in reserve."""
        return int(cls.GITHUB_RATE_LIMIT_PER_HOUR * (1 - cls.GITHUB_RATE_LIMIT_BUFFER))
    
    @classmethod
    def get_rate_limit_threshold(cls, api_type: str = "github"):
        """
        Calculate rate limit threshold for different APIs.
        
        Args:
            api_type: Type of API ('github', 'linkedin', 'stackoverflow')
            
        Returns:
            Minimum requests to keep in reserve
        """
        if api_type == "github":
            return cls.get_github_rate_limit_threshold()
        elif api_type == "linkedin":
            # Assuming LinkedIn has 1000 requests/hour (adjust as needed)
            return int(1000 * (1 - cls.LINKEDIN_RATE_LIMIT_BUFFER))
        elif api_type == "stackoverflow":
            # Assuming StackOverflow has 300 requests/hour (adjust as needed)
            return int(300 * (1 - cls.STACKOVERFLOW_RATE_LIMIT_BUFFER))
        else:
            # Default to GitHub settings
            return cls.get_github_rate_limit_threshold()
    
    @classmethod
    def log_config(cls):
        """Log current configuration for debugging."""
        logging.info("📋 Current Configuration:")
        logging.info(f"  Project: {cls.PROJECT_NAME}")
        logging.info(f"  GitHub Rate Limit Buffer: {cls.GITHUB_RATE_LIMIT_BUFFER * 100}%")
        logging.info(f"  LinkedIn Rate Limit Buffer: {cls.LINKEDIN_RATE_LIMIT_BUFFER * 100}%")
        logging.info(f"  StackOverflow Rate Limit Buffer: {cls.STACKOVERFLOW_RATE_LIMIT_BUFFER * 100}%")
        logging.info(f"  Kafka Topics: {', '.join(cls.get_kafka_topics())}")
        logging.info(f"  Batch Size: {cls.BATCH_SIZE_DEFAULT}")
        logging.info(f"  Retry Attempts: {cls.RETRY_ATTEMPTS}")

def get_default_dag_args():
    """Get standard default arguments for DAGs."""
    return {
        'owner': SkillsGapConfig.PROJECT_NAME,
        'depends_on_past': False,
        'start_date': datetime(2024, 6, 1),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': SkillsGapConfig.RETRY_ATTEMPTS,
        'retry_delay': timedelta(minutes=2),
    }