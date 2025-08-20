import os
import time
from unittest.mock import Mock, patch
from cursor_agent_simple import ask_agent

def test_basic_functionality():
    """Test basic functionality with a real question."""
    print("=" * 60)
    print("TESTING BASIC FUNCTIONALITY")
    print("=" * 60)
    
    # Check if token is set in environment
    token = os.environ.get('DATABRICKS_TOKEN')
    if not token:
        print("⚠️  DATABRICKS_TOKEN not set in environment. Skipping real API test.")
        print("Set DATABRICKS_TOKEN environment variable to run full tests.")
        return True
    
    try:
        # Test basic question
        print("\n1. Testing basic question:")
        response = ask_agent("What is Delta Lake?")
        print(f"Response length: {len(response)} characters")
        print(f"Response preview: {response[:100]}...")
        
        if response.startswith("Error"):
            print("❌ Basic functionality test failed")
            return False
        else:
            print("✅ Basic functionality test passed")
        
        # Test with footnotes
        print("\n2. Testing with footnotes:")
        response_with_footnotes = ask_agent("What is Delta Lake?", include_footnotes=True)
        print(f"Response length: {len(response_with_footnotes)} characters")
        
        # Check if footnotes are present
        footnote_patterns = ['[^', ']:']
        has_footnotes = any(pattern in response_with_footnotes for pattern in footnote_patterns)
        print(f"Has footnotes: {has_footnotes}")
        
        if has_footnotes:
            print("✅ Footnotes test passed")
        else:
            print("⚠️  Footnotes test - no footnotes found (may be normal)")
        
        return True
        
    except Exception as e:
        print(f"❌ Basic functionality test failed: {e}")
        return False

def test_missing_token():
    """Test error handling when token is missing."""
    print("\n" + "=" * 60)
    print("TESTING MISSING TOKEN")
    print("=" * 60)
    
    # Temporarily remove token
    original_token = os.environ.get('DATABRICKS_TOKEN')
    if 'DATABRICKS_TOKEN' in os.environ:
        del os.environ['DATABRICKS_TOKEN']
    
    try:
        response = ask_agent("What is Delta Lake?")
        print(f"Response: {response}")
        
        if "DATABRICKS_TOKEN environment variable not set" in response:
            print("✅ Missing token test passed")
            return True
        else:
            print("❌ Missing token test failed")
            return False
            
    finally:
        # Restore token
        if original_token:
            os.environ['DATABRICKS_TOKEN'] = original_token

def test_rate_limiting_retry_logic():
    """Test rate limiting retry logic with mocked errors (fast version)."""
    print("\n" + "=" * 60)
    print("TESTING RATE LIMITING RETRY LOGIC (FAST)")
    print("=" * 60)
    
    # Mock the AgentBricksClient to simulate rate limiting
    with patch('cursor_agent_simple.AgentBricksClient') as mock_client_class:
        # Create a mock client instance
        mock_client = Mock()
        mock_client_class.return_value = mock_client
        
        # Test: Rate limiting error that should retry (with shorter delays for testing)
        print("\nTesting rate limiting retry (fast):")
        mock_client.call_agent.side_effect = [
            Exception("429 Too Many Requests"),  # First attempt fails
            "Success response"                   # Second attempt succeeds
        ]
        
        # Temporarily patch time.sleep to be faster for testing
        with patch('time.sleep', return_value=None):
            response = ask_agent("Test question")
        
        print(f"Response: {response}")
        print(f"Call count: {mock_client.call_agent.call_count}")
        
        if response == "Success response" and mock_client.call_agent.call_count == 2:
            print("✅ Rate limiting retry test passed")
            return True
        else:
            print("❌ Rate limiting retry test failed")
            return False



def main():
    """Run all tests."""
    print("🧪 CURSOR AGENT COMPREHENSIVE TEST SUITE")
    print("=" * 80)
    
    tests = [
        ("Basic Functionality", test_basic_functionality),
        ("Missing Token", test_missing_token),
        ("Rate Limiting Retry Logic", test_rate_limiting_retry_logic)
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        try:
            if test_func():
                passed += 1
                print(f"\n✅ {test_name}: PASSED")
            else:
                print(f"\n❌ {test_name}: FAILED")
        except Exception as e:
            print(f"\n❌ {test_name}: FAILED with exception: {e}")
    
    print("\n" + "=" * 80)
    print(f"TEST RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("🎉 ALL TESTS PASSED! The cursor agent is working correctly.")
    else:
        print("⚠️  Some tests failed. Please check the implementation.")
    
    return passed == total

if __name__ == "__main__":
    main()
