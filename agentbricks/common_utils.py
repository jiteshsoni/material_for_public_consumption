#!/usr/bin/env python3
"""
Common utilities for the AgentBricks package.
Centralizes shared functionality to reduce code duplication.
"""

import os
import re
from typing import Tuple, List, Optional

def validate_environment_variables(required_vars: List[str]) -> Tuple[bool, str]:
    """
    Validate that required environment variables are set.
    
    Args:
        required_vars: List of required environment variable names
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    missing_vars = []
    
    for var in required_vars:
        if not os.environ.get(var):
            missing_vars.append(var)
    
    if missing_vars:
        error_msg = f"""
❌ Missing required environment variables: {', '.join(missing_vars)}

🔧 Setup Instructions:
1. Set your Databricks token:
   export DATABRICKS_TOKEN="your_databricks_token_here"

2. Set your Databricks base URL:
   export DATABRICKS_BASE_URL="https://your-workspace.cloud.databricks.com/serving-endpoints"

3. Set your Knowledge Agent model:
   export DATABRICKS_MODEL="ka-de16acc4-endpoint"

💡 You can also create a .env file in the current directory with:
   DATABRICKS_TOKEN=your_token
   DATABRICKS_BASE_URL=https://your-workspace.cloud.databricks.com/serving-endpoints
   DATABRICKS_MODEL=ka-de16acc4-endpoint

🔗 Get your token from: https://your-workspace.cloud.databricks.com/settings/tokens
"""
        return False, error_msg
    
    return True, ""

def validate_token_format(token: str) -> Tuple[bool, str]:
    """
    Validate token format and length.
    
    Args:
        token: The token to validate
        
    Returns:
        Tuple of (is_valid, error_message)
    """
    if len(token) < 10:
        return False, """❌ Invalid DATABRICKS_TOKEN format!

🔧 The token should be a long string (typically 50+ characters).
Please check your token and try again.

🔗 Get a new token from: https://your-workspace.cloud.databricks.com/settings/tokens"""
    
    return True, ""

def remove_footnotes(text: str) -> str:
    """
    Remove footnotes from response text.
    
    Args:
        text: The response text that may contain footnotes
        
    Returns:
        Text with footnotes removed
    """
    # Remove footnote references like [^UCVq-1], [^Ep6f-2], etc.
    text = re.sub(r'\[\^[A-Za-z0-9\-]+\]', '', text)
    
    # Remove footnote definitions at the end of the text
    lines = text.split('\n')
    cleaned_lines = []
    in_footnote_section = False
    
    for line in lines:
        # Check if we're entering the footnote section
        if re.match(r'^\[\^[A-Za-z0-9\-]+\]:', line.strip()):
            in_footnote_section = True
            continue
        
        # If we're in the footnote section, skip lines that are part of footnotes
        if in_footnote_section:
            # If we hit a blank line or non-footnote content, we might be out of footnotes
            if line.strip() == '' or not line.strip().startswith('['):
                in_footnote_section = False
                if line.strip() != '':  # Don't add empty lines
                    cleaned_lines.append(line)
            continue
        
        # Add non-footnote lines
        cleaned_lines.append(line)
    
    return '\n'.join(cleaned_lines).strip()

def is_rate_limiting_error(error_message: str) -> bool:
    """
    Check if an error message indicates rate limiting.
    
    Args:
        error_message: The error message to check
        
    Returns:
        True if it's a rate limiting error, False otherwise
    """
    error_lower = error_message.lower()
    rate_limit_keywords = ['rate limit', 'rate limiting', '429', 'too many requests', 'quota exceeded']
    return any(keyword in error_lower for keyword in rate_limit_keywords)

def get_standard_error_response(error_type: str, details: str = "") -> str:
    """
    Get standardized error response messages.
    
    Args:
        error_type: Type of error ('token', 'network', 'auth', 'rate_limit')
        details: Additional error details
        
    Returns:
        Formatted error message
    """
    error_templates = {
        'token': """❌ Required environment variables not found!

🔧 Required variables:
1. DATABRICKS_TOKEN - Your Databricks authentication token
2. DATABRICKS_BASE_URL - Your Knowledge Agent endpoint URL
3. DATABRICKS_MODEL - Your Knowledge Agent model name

🔧 Setup Instructions:
1. Set your Databricks token:
   export DATABRICKS_TOKEN="your_token_here"
2. Set your Databricks base URL:
   export DATABRICKS_BASE_URL="https://your-workspace.cloud.databricks.com/serving-endpoints"
3. Set your Knowledge Agent model:
   export DATABRICKS_MODEL="ka-de16acc4-endpoint"

🔗 Get your token from Databricks workspace settings.""",
        
        'network': f"""❌ Network connectivity error: {details}

🔧 Please check:
1. Your internet connection
2. Firewall settings
3. Databricks workspace availability

💡 Try checking your network connectivity.""",
        
        'auth': f"""❌ Authentication error: {details}

🔧 Please check:
1. Your DATABRICKS_TOKEN is valid and not expired
2. You have proper permissions to access the Knowledge Agent
3. Your workspace configuration is correct

🔗 Verify your token at: https://your-workspace.cloud.databricks.com/settings/tokens""",
        
        'rate_limit': """❌ Rate limiting detected.

⏳ Please wait a few minutes and try again.
💡 Consider reducing the frequency of your requests."""
    }
    
    return error_templates.get(error_type, f"❌ Unexpected error: {details}")
