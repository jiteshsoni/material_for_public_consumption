#!/usr/bin/env python3
"""
Secure Configuration File Finder
Automatically finds the appropriate configuration file while maintaining security.
"""

import os
import logging
from typing import Optional

logger = logging.getLogger(__name__)

def find_config_file(custom_path: Optional[str] = None) -> str:
    """
    Find the appropriate configuration file using secure precedence.
    
    Returns:
        str: Path to configuration file to use
    """
    
    if custom_path:
        if os.path.exists(custom_path):
            logger.info(f"Using custom config file: {custom_path}")
            return custom_path
        else:
            logger.warning(f"Custom config file not found: {custom_path}")
    
    # Security-first precedence order
    config_candidates = [
        "lakebase_credentials.conf",         # User's actual config (gitignored)
        "lakebase_credentials.conf.local",   # Alternative local config  
        "lakebase_credentials.conf.template", # Template with placeholders
        "lakebase_credentials.conf.example",  # Example template
        ".env"  # Will trigger environment-only mode
    ]
    
    for candidate in config_candidates:
        if os.path.exists(candidate):
            # Check if it's a template file
            if candidate.endswith(('.template', '.example')):
                if not os.path.exists("lakebase_credentials.conf"):
                    logger.warning(f"Found template {candidate} but no actual config file.")
                    logger.info("Run: python config_update_helper.py to create your config file")
                    return candidate  # Will be handled by config loading logic
            
            logger.info(f"Using config file: {candidate}")
            return candidate
    
    # No config file found - use environment variables only
    logger.warning("No configuration file found. Using environment variables only.")
    logger.info("Create a config file with: python config_update_helper.py")
    return "lakebase_credentials.conf"  # Will trigger env-only fallback

def get_results_directory() -> str:
    """
    Get the appropriate results directory (renamed for security).
    
    Returns:
        str: Path to results directory
    """
    
    # Prefer the new secure name
    if os.path.exists("benchmark_results"):
        return "benchmark_results"
    
    # Fallback to old name if it exists (for migration)
    if os.path.exists("results"):
        logger.warning("Found old 'results' directory. Consider renaming to 'benchmark_results' for security.")
        return "results"
    
    # Create new secure directory
    logger.info("Creating benchmark_results directory")
    os.makedirs("benchmark_results", exist_ok=True)
    return "benchmark_results"

def check_security_status() -> dict:
    """
    Check the security status of configuration files.
    
    Returns:
        dict: Security status report
    """
    
    status = {
        "secure": True,
        "issues": [],
        "recommendations": []
    }
    
    # Check for actual credentials file
    if os.path.exists("lakebase_credentials.conf"):
        status["recommendations"].append("Actual credentials file found (good - will be gitignored)")
    
    # Check for template files
    templates = ["lakebase_credentials.conf.example", "lakebase_credentials.conf.template"]
    template_count = sum(1 for t in templates if os.path.exists(t))
    if template_count == 0:
        status["issues"].append("No template files found - users won't know how to configure")
        status["secure"] = False
    
    # Check gitignore
    if os.path.exists(".gitignore"):
        with open(".gitignore", 'r') as f:
            gitignore = f.read()
        
        required_patterns = ["*.conf", ".env", "benchmark_results/", "results/"]
        missing_patterns = [p for p in required_patterns if p not in gitignore]
        
        if missing_patterns:
            status["issues"].append(f"Missing gitignore patterns: {missing_patterns}")
            status["secure"] = False
    else:
        status["issues"].append("No .gitignore file found - sensitive files at risk!")
        status["secure"] = False
    
    # Check for old results directory
    if os.path.exists("results") and not os.path.exists("benchmark_results"):
        status["recommendations"].append("Rename 'results' to 'benchmark_results' for better security")
    
    return status

if __name__ == "__main__":
    # Test the configuration finder
    print("🔍 CONFIGURATION FILE FINDER")
    print("=" * 40)
    
    config_file = find_config_file()
    print(f"Config file: {config_file}")
    
    results_dir = get_results_directory() 
    print(f"Results directory: {results_dir}")
    
    security_status = check_security_status()
    print(f"\n🔒 Security Status: {'✅ SECURE' if security_status['secure'] else '⚠️ ISSUES FOUND'}")
    
    for issue in security_status['issues']:
        print(f"❌ {issue}")
    
    for rec in security_status['recommendations']:
        print(f"💡 {rec}")