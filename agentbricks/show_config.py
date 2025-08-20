#!/usr/bin/env python3
"""
Configuration helper script for AgentBricks.
Shows current configuration and provides setup guidance.
"""

import os
import sys
from config import load_config, print_config_help, get_all_env_vars

def show_current_config():
    """Show the current configuration values."""
    print("🔧 Current AgentBricks Configuration")
    print("=" * 50)
    print()
    
    config = load_config()
    
    print("📋 Current Values:")
    print(f"   • DATABRICKS_TOKEN: {'*' * min(len(config.databricks_token), 10)}..." if config.databricks_token else "   • DATABRICKS_TOKEN: Not set")
    print(f"   • DATABRICKS_HOST: {config.databricks_host}")
    print(f"   • DATABRICKS_CLUSTER_ID: {config.databricks_cluster_id}")
    print(f"   • DATABRICKS_BASE_URL: {config.base_url}")
    print(f"   • DATABRICKS_MODEL: {config.model}")
    print(f"   • AGENTBRICKS_MAX_RETRIES: {config.max_retries}")
    print(f"   • AGENTBRICKS_RETRY_DELAY: {config.retry_delay}")
    print(f"   • AGENTBRICKS_TIMEOUT: {config.timeout}")
    print()
    
    # Validate configuration
    is_valid, error_msg = config.validate()
    if is_valid:
        print("✅ Configuration is valid!")
    else:
        print(f"❌ Configuration error: {error_msg}")
    print()

def show_missing_vars():
    """Show which environment variables are missing."""
    print("🔍 Missing Environment Variables")
    print("=" * 50)
    print()
    
    all_vars = get_all_env_vars()
    missing_vars = []
    
    for var in all_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if not missing_vars:
        print("✅ All environment variables are set!")
    else:
        print("❌ Missing environment variables:")
        for var in missing_vars:
            print(f"   • {var}")
    print()

def main():
    """Main function."""
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "help":
            print_config_help()
        elif command == "missing":
            show_missing_vars()
        elif command == "current":
            show_current_config()
        else:
            print(f"Unknown command: {command}")
            print("Available commands: help, missing, current")
    else:
        # Default: show current configuration
        show_current_config()
        print("💡 Use 'python show_config.py help' for setup instructions")
        print("💡 Use 'python show_config.py missing' to see missing variables")

if __name__ == "__main__":
    main()
