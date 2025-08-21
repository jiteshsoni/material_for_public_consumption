from openai import OpenAI
import os
import json
from typing import List, Dict, Any, Optional
from config import AgentBricksConfig, load_config
from common_utils import remove_footnotes, get_standard_error_response

class AgentBricksClient:
    """
    A client for making synchronous calls to Databricks Knowledge Agent.
    """
    
    def __init__(self, 
                 databricks_token: Optional[str] = None,
                 base_url: Optional[str] = None,
                 model: Optional[str] = None,
                 config: Optional[AgentBricksConfig] = None):
        """
        Initialize the AgentBricks client.
        
        Args:
            databricks_token: Databricks token. If None, will try to get from config or DATABRICKS_TOKEN env var
            base_url: Base URL for the Databricks serving endpoint. If None, will try to get from config or DATABRICKS_BASE_URL env var
            model: Model name for the knowledge agent. If None, will try to get from config or DATABRICKS_MODEL env var
            config: Configuration object. If None, will load from environment variables
        """
        # Load configuration if not provided
        if config is None:
            config = load_config()
        
        # Use provided parameters or fall back to config
        self.databricks_token = databricks_token or config.databricks_token
        self.base_url = base_url or config.base_url
        self.model = model or config.model
        
        # Validate token
        if not self.databricks_token:
            raise ValueError("Databricks token is required. Set DATABRICKS_TOKEN environment variable or pass it directly.")
        
        self.client = OpenAI(
            api_key=self.databricks_token,
            base_url=self.base_url
        )
    
    def call_agent(self, 
                   message: str, 
                   system_prompt: Optional[str] = None,
                   conversation_history: Optional[List[Dict[str, str]]] = None,
                   include_footnotes: bool = False) -> str:
        """
        Make a synchronous call to the knowledge agent.
        
        Args:
            message: The user message to send to the agent
            system_prompt: Optional system prompt to set context
            conversation_history: Optional list of previous messages in the conversation
            include_footnotes: Whether to include footnotes in the response (default: False)
            
        Returns:
            The agent's response as a string
        """
        # Build the input messages
        messages = []
        
        # Add system prompt if provided
        if system_prompt:
            messages.append({
                "role": "system",
                "content": system_prompt
            })
        
        # Add conversation history if provided
        if conversation_history:
            messages.extend(conversation_history)
        
        # Add the current user message
        messages.append({
            "role": "user",
            "content": message
        })
        
        try:
            response = self.client.responses.create(
                model=self.model,
                input=messages
            )
            
            # Extract the response text
            if response.output and len(response.output) > 0:
                if hasattr(response.output[0], 'content') and len(response.output[0].content) > 0:
                    response_text = response.output[0].content[0].text
                    
                    # Remove footnotes if requested
                    if not include_footnotes:
                        response_text = remove_footnotes(response_text)
                    
                    return response_text
                else:
                    return str(response.output[0])
            else:
                return "No response received from agent"
                
        except Exception as e:
            raise Exception(f"Error calling agent: {str(e)}")
    
    def call_agent_with_metadata(self, 
                                message: str, 
                                system_prompt: Optional[str] = None,
                                conversation_history: Optional[List[Dict[str, str]]] = None,
                                include_footnotes: bool = False) -> Dict[str, Any]:
        """
        Make a synchronous call to the knowledge agent and return full response metadata.
        
        Args:
            message: The user message to send to the agent
            system_prompt: Optional system prompt to set context
            conversation_history: Optional list of previous messages in the conversation
            include_footnotes: Whether to include footnotes in the response (default: False)
            
        Returns:
            Dictionary containing the response text and metadata
        """
        # Build the input messages
        messages = []
        
        # Add system prompt if provided
        if system_prompt:
            messages.append({
                "role": "system",
                "content": system_prompt
            })
        
        # Add conversation history if provided
        if conversation_history:
            messages.extend(conversation_history)
        
        # Add the current user message
        messages.append({
            "role": "user",
            "content": message
        })
        
        try:
            response = self.client.responses.create(
                model=self.model,
                input=messages
            )
            
            response_text = response.output[0].content[0].text if response.output and len(response.output) > 0 and hasattr(response.output[0], 'content') and len(response.output[0].content) > 0 else str(response.output[0]) if response.output else "No response received"
            
            # Remove footnotes if requested
            if not include_footnotes:
                response_text = remove_footnotes(response_text)
            
            return {
                "response_text": response_text,
                "full_response": response,
                "model": self.model,
                "messages_sent": messages
            }
                
        except Exception as e:
            raise Exception(f"Error calling agent: {str(e)}")


def main():
    """
    Example usage of the AgentBricks client.
    """
    try:
        # Initialize the client
        client = AgentBricksClient()
        
        # Example 1: Simple call
        print("=== Simple Call ===")
        response = client.call_agent("What is an LLM agent?")
        print(f"Response: {response}\n")
        
        # Example 2: Call with system prompt
        print("=== Call with System Prompt ===")
        system_prompt = "You are a helpful AI assistant that provides concise answers."
        response = client.call_agent(
            "Explain machine learning in one sentence.",
            system_prompt=system_prompt
        )
        print(f"Response: {response}\n")
        
        # Example 3: Call with conversation history
        print("=== Call with Conversation History ===")
        conversation_history = [
            {"role": "user", "content": "What is Python?"},
            {"role": "assistant", "content": "Python is a high-level programming language known for its simplicity and readability."}
        ]
        response = client.call_agent(
            "What are its main features?",
            conversation_history=conversation_history
        )
        print(f"Response: {response}\n")
        
        # Example 4: Get full response metadata
        print("=== Full Response Metadata ===")
        result = client.call_agent_with_metadata("What is data science?")
        print(f"Response Text: {result['response_text']}")
        print(f"Model Used: {result['model']}")
        print(f"Messages Sent: {len(result['messages_sent'])}")
        
    except Exception as e:
        print(f"Error: {e}")
        print("\nMake sure you have set the DATABRICKS_TOKEN environment variable:")
        print("export DATABRICKS_TOKEN='your_token_here'")


if __name__ == "__main__":
    main()
