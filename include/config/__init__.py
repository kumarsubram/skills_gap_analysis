"""
Configuration Package
====================

Centralized configuration management for the Skills Gap Analysis project.
"""

from .settings import SkillsGapConfig, get_default_dag_args

__all__ = ['SkillsGapConfig', 'get_default_dag_args']