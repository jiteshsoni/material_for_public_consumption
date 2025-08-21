#!/usr/bin/env python3
"""
Configuration management for AgentBricks package.
Centralizes all configuration settings and provides environment-based configuration.
"""

import os
from typing import Optional
from dataclasses import dataclass

@dataclass
class AgentBricksConfig:
    """Configuration class for AgentBricks settings."""
    
    # Required settings
    databricks_token: str
    
    # Optional settings with defaults
    databricks_host: str = "https://your-workspace.cloud.databricks.com"
    databricks_cluster_id: str = "your_cluster_id"
    base_url: str = "https://dbc-555fe676-d529.cloud.databricks.com/serving-endpoints"
    model: str = "ka-de16acc4-endpoint"
    
    # Advanced settings
    max_retries: int = 3
    retry_delay: int = 30  # seconds
    timeout: int = 60  # seconds
    
    @classmethod
    def from_environment(cls) -> 'AgentBricksConfig':
        """
        Create configuration from environment variables.
        
        Returns:
            AgentBricksConfig instance with values from environment variables
        """
        return cls(
            databricks_token=os.environ.get('DATABRICKS_TOKEN', ''),
            databricks_host=os.environ.get('DATABRICKS_HOST', 'https://your-workspace.cloud.databricks.com'),
            databricks_cluster_id=os.environ.get('DATABRICKS_CLUSTER_ID', 'your_cluster_id'),
            base_url=os.environ.get('DATABRICKS_BASE_URL', 'https://dbc-555fe676-d529.cloud.databricks.com/serving-endpoints'),
            model=os.environ.get('DATABRICKS_MODEL', 'ka-de16acc4-endpoint'),
            max_retries=int(os.environ.get('AGENTBRICKS_MAX_RETRIES', '3')),
            retry_delay=int(os.environ.get('AGENTBRICKS_RETRY_DELAY', '30')),
            timeout=int(os.environ.get('AGENTBRICKS_TIMEOUT', '60'))
        )
    
    def validate(self) -> tuple[bool, str]:
        """
        Validate the configuration.
        
        Returns:
            Tuple of (is_valid, error_message)
        """
        if not self.databricks_token:
            return False, "DATABRICKS_TOKEN is required"
        
        if len(self.databricks_token) < 10:
            return False, "DATABRICKS_TOKEN must be at least 10 characters long"
        
        if not self.base_url:
            return False, "DATABRICKS_BASE_URL is required"
        
        if not self.model:
            return False, "DATABRICKS_MODEL is required"
        
        return True, ""

def load_config() -> AgentBricksConfig:
    """
    Load configuration from environment variables.
    
    Returns:
        AgentBricksConfig instance
    """
    return AgentBricksConfig.from_environment()

def get_required_env_vars() -> list[str]:
    """
    Get list of required environment variables.
    
    Returns:
        List of required environment variable names
    """
    return ['DATABRICKS_TOKEN', 'DATABRICKS_BASE_URL', 'DATABRICKS_MODEL']

def get_optional_env_vars() -> list[str]:
    """
    Get list of optional environment variables.
    
    Returns:
        List of optional environment variable names
    """
    return [
        'DATABRICKS_HOST',
        'DATABRICKS_CLUSTER_ID', 
        'AGENTBRICKS_MAX_RETRIES',
        'AGENTBRICKS_RETRY_DELAY',
        'AGENTBRICKS_TIMEOUT'
    ]

def get_all_env_vars() -> list[str]:
    """
    Get list of all environment variables used by AgentBricks.
    
    Returns:
        List of all environment variable names
    """
    return get_required_env_vars() + get_optional_env_vars()

def print_config_help():
    """Print configuration help information."""
    print("🔧 AgentBricks Configuration")
    print("=" * 50)
    print()
    
    print("📋 Required Environment Variables:")
    for var in get_required_env_vars():
        print(f"   • {var}")
    print()
    
    print("📋 Optional Environment Variables:")
    for var in get_optional_env_vars():
        print(f"   • {var}")
    print()
    
    print("💡 Example .env file:")
    print("DATABRICKS_TOKEN=your_token_here")
    print("DATABRICKS_HOST=https://your-workspace.cloud.databricks.com")
    print("DATABRICKS_CLUSTER_ID=your_cluster_id")
    print("DATABRICKS_BASE_URL=https://your-workspace.cloud.databricks.com/serving-endpoints")
    print("DATABRICKS_MODEL=ka-de16acc4-endpoint")
    print("AGENTBRICKS_MAX_RETRIES=3")
    print("AGENTBRICKS_RETRY_DELAY=30")
    print("AGENTBRICKS_TIMEOUT=60")
    print()
    
    print("🔗 Get your token from: https://your-workspace.cloud.databricks.com/settings/tokens")
