#!/usr/bin/env python3
"""
Test script to demonstrate the enhanced error handling in cursor_agent_simple.py
"""

import os
import sys
from cursor_agent_simple import ask_agent, check_environment_setup

def test_scenario_1_no_env_vars():
    """Test scenario: No environment variables set"""
    print("🧪 Test Scenario 1: No environment variables set")
    print("=" * 60)
    
    # Clear environment variables
    for var in ['DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID']:
        if var in os.environ:
            del os.environ[var]
    
    is_valid, error_msg = check_environment_setup()
    print(f"Environment valid: {is_valid}")
    print(error_msg)
    print()

def test_scenario_2_missing_token():
    """Test scenario: Missing token only"""
    print("🧪 Test Scenario 2: Missing token only")
    print("=" * 60)
    
    # Set some environment variables but not token
    os.environ['DATABRICKS_HOST'] = "https://test.cloud.databricks.com"
    os.environ['DATABRICKS_CLUSTER_ID'] = "test-cluster"
    
    if 'DATABRICKS_TOKEN' in os.environ:
        del os.environ['DATABRICKS_TOKEN']
    
    is_valid, error_msg = check_environment_setup()
    print(f"Environment valid: {is_valid}")
    print(error_msg)
    print()

def test_scenario_3_short_token():
    """Test scenario: Token too short"""
    print("🧪 Test Scenario 3: Token too short")
    print("=" * 60)
    
    # Set all environment variables but with short token
    os.environ['DATABRICKS_TOKEN'] = "short"
    os.environ['DATABRICKS_HOST'] = "https://test.cloud.databricks.com"
    os.environ['DATABRICKS_CLUSTER_ID'] = "test-cluster"
    
    response = ask_agent("What is Delta Lake?")
    print(response)
    print()

def test_scenario_4_invalid_token():
    """Test scenario: Invalid token format"""
    print("🧪 Test Scenario 4: Invalid token format")
    print("=" * 60)
    
    # Set all environment variables but with invalid token
    os.environ['DATABRICKS_TOKEN'] = "this_is_a_longer_token_but_still_invalid_for_testing_purposes_only"
    os.environ['DATABRICKS_HOST'] = "https://test.cloud.databricks.com"
    os.environ['DATABRICKS_CLUSTER_ID'] = "test-cluster"
    
    response = ask_agent("What is Delta Lake?")
    print(response)
    print()

def test_scenario_5_direct_token_parameter():
    """Test scenario: Using token parameter directly"""
    print("🧪 Test Scenario 5: Using token parameter directly")
    print("=" * 60)
    
    # Clear environment variables
    for var in ['DATABRICKS_TOKEN', 'DATABRICKS_HOST', 'DATABRICKS_CLUSTER_ID']:
        if var in os.environ:
            del os.environ[var]
    
    # Pass token directly as parameter
    response = ask_agent("What is Delta Lake?", token="invalid_token_for_testing")
    print(response)
    print()

def main():
    """Run all test scenarios"""
    print("🚀 Testing Enhanced Error Handling in cursor_agent_simple.py")
    print("=" * 80)
    print()
    
    try:
        test_scenario_1_no_env_vars()
        test_scenario_2_missing_token()
        test_scenario_3_short_token()
        test_scenario_4_invalid_token()
        test_scenario_5_direct_token_parameter()
        
        print("✅ All test scenarios completed!")
        print("\n📋 Summary:")
        print("   • Scenario 1: Tests missing environment variables")
        print("   • Scenario 2: Tests missing token specifically")
        print("   • Scenario 3: Tests token length validation")
        print("   • Scenario 4: Tests invalid token format")
        print("   • Scenario 5: Tests direct token parameter usage")
        
    except KeyboardInterrupt:
        print("\n⏹️  Testing interrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Unexpected error during testing: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
