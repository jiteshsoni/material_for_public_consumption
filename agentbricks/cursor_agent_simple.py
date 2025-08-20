import os
import time
import sys
from agent_client import AgentBricksClient
from config import load_config, get_required_env_vars, get_optional_env_vars, print_config_help
from common_utils import validate_environment_variables, validate_token_format, is_rate_limiting_error, get_standard_error_response

def ask_agent(question: str, include_footnotes: bool = False, token: str = None, endpoint: str = None) -> str:
    """
    Ask the Databricks Knowledge Agent a question.
    
    Args:
        question: The question to ask
        include_footnotes: Whether to include footnotes in the response (default: False)
        token: Databricks token (if not provided, will try to get from DATABRICKS_TOKEN env var)
        endpoint: Custom endpoint URL (optional)
        
    Returns:
        Response from the agent (with or without footnotes)
        
    Example:
        response = ask_agent("What is Delta Lake?")  # No footnotes
        response = ask_agent("What is Delta Lake?", include_footnotes=True)  # With footnotes
        response = ask_agent("What is Delta Lake?", token="your_token", endpoint="your_endpoint")
    """
    # Check environment setup first
    is_valid, error_msg = validate_environment_variables(get_required_env_vars())
    if not is_valid:
        return error_msg
    
    # Get token from parameter or environment
    if not token:
        token = os.environ.get('DATABRICKS_TOKEN')
        if not token:
            return get_standard_error_response('token')
    
    # Validate token format
    is_valid, error_msg = validate_token_format(token)
    if not is_valid:
        return error_msg
    
    # Load configuration for retry settings
    config = load_config()
    
    # Create client with optional custom endpoint
    try:
        if endpoint:
            client = AgentBricksClient(databricks_token=token, base_url=endpoint)
        else:
            client = AgentBricksClient(databricks_token=token)
    except ValueError as e:
        return get_standard_error_response('auth', str(e))
    except Exception as e:
        return get_standard_error_response('network', str(e))
    
    # Retry logic for rate limiting using config values
    max_retries = config.max_retries
    retry_delay = config.retry_delay
    
    for attempt in range(max_retries):
        try:
            return client.call_agent(question, include_footnotes=include_footnotes)
            
        except Exception as e:
            error_message = str(e).lower()
            
            # Check if it's a rate limiting error
            if is_rate_limiting_error(error_message):
                print(f"⚠️  Rate limiting error detected (attempt {attempt + 1}/{max_retries}): {str(e)}")
                
                if attempt < max_retries - 1:  # Not the last attempt
                    print(f"⏳ Waiting {retry_delay} seconds before retrying...")
                    time.sleep(retry_delay)
                    continue
                else:
                    return get_standard_error_response('rate_limit')
            else:
                # Non-rate limiting error, don't retry
                return get_standard_error_response('auth', str(e))
    
    return "❌ Unexpected error occurred during agent communication."


# Test the function
if __name__ == "__main__":
    print("🤖 Databricks Knowledge Agent Client")
    print("=" * 50)
    
    # Check environment first
    is_valid, error_msg = validate_environment_variables(get_required_env_vars())
    if not is_valid:
        print(error_msg)
        sys.exit(1)
    
    print("✅ Environment variables are properly configured!")
    print("🔗 Testing connection to Knowledge Agent...")
    
    try:
        response = ask_agent("What is Delta Lake?")
        print("\n📝 Response:")
        print(response)
    except KeyboardInterrupt:
        print("\n⏹️  Operation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error: {str(e)}")
        sys.exit(1)
