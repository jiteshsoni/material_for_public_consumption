#!/usr/bin/env python3
"""
Enhanced Lakebase Setup Script
Automates the setup process for the enhanced benchmark with FastAPI best practices.
"""

import os
import sys
import subprocess
import json
from pathlib import Path

def print_header(title: str):
    """Print a formatted header"""
    print(f"\n{'='*60}")
    print(f"🌊 {title}")
    print(f"{'='*60}")

def print_step(step: str):
    """Print a formatted step"""
    print(f"\n🔹 {step}")

def run_command(command: str, description: str):
    """Run a command and handle errors"""
    print(f"   Running: {description}")
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"   ✅ {description} completed successfully")
            return True
        else:
            print(f"   ❌ {description} failed:")
            print(f"   Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"   ❌ {description} failed with exception: {e}")
        return False

def check_python_version():
    """Check if Python version meets requirements"""
    print_step("Checking Python version...")
    
    version = sys.version_info
    if version.major == 3 and version.minor >= 8:
        print(f"   ✅ Python {version.major}.{version.minor}.{version.micro} is supported")
        return True
    else:
        print(f"   ❌ Python {version.major}.{version.minor}.{version.micro} is not supported")
        print(f"   Required: Python 3.8 or higher")
        return False

def install_dependencies():
    """Install required dependencies"""
    print_step("Installing dependencies...")
    
    # Check if requirements.txt exists
    if not os.path.exists("requirements.txt"):
        print("   ❌ requirements.txt not found")
        return False
    
    # Install requirements
    return run_command(
        f"{sys.executable} -m pip install -r requirements.txt",
        "Installing Python packages"
    )

def create_env_file():
    """Create .env file from template"""
    print_step("Setting up environment configuration...")
    
    env_example = ".env.example"
    env_file = ".env"
    
    if os.path.exists(env_file):
        print(f"   ℹ️  {env_file} already exists, skipping creation")
        return True
    
    if not os.path.exists(env_example):
        print(f"   ❌ {env_example} template not found")
        return False
    
    try:
        # Copy template to .env
        with open(env_example, 'r') as src, open(env_file, 'w') as dst:
            dst.write(src.read())
        
        print(f"   ✅ Created {env_file} from template")
        print(f"   ⚠️  Please edit {env_file} with your actual configuration values")
        return True
        
    except Exception as e:
        print(f"   ❌ Failed to create {env_file}: {e}")
        return False

def validate_configuration():
    """Validate the configuration"""
    print_step("Validating configuration...")
    
    try:
        # Try to import and load configuration
        sys.path.insert(0, os.getcwd())
        from config import load_settings
        
        settings = load_settings()
        print("   ✅ Configuration loaded successfully")
        
        # Run validation
        validation = settings.validate_configuration()
        if validation['valid']:
            print("   ✅ Configuration validation passed")
        else:
            print("   ⚠️  Configuration validation found issues:")
            for error in validation['errors']:
                print(f"      ❌ {error}")
        
        if validation['warnings']:
            print("   ⚠️  Configuration warnings:")
            for warning in validation['warnings']:
                print(f"      ⚠️  {warning}")
        
        if validation['recommendations']:
            print("   💡 Configuration recommendations:")
            for rec in validation['recommendations']:
                print(f"      💡 {rec}")
        
        return validation['valid']
        
    except Exception as e:
        print(f"   ❌ Configuration validation failed: {e}")
        print(f"   💡 Please check your .env file or lakebase_credentials.conf")
        return False

def test_database_connection():
    """Test database connection"""
    print_step("Testing database connection...")
    
    try:
        from enhanced_production_pool import create_enhanced_pool
        
        with create_enhanced_pool() as pool:
            conn = pool.get_connection(timeout=10)
            
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1 as test, NOW() as timestamp")
                result = cursor.fetchone()
            
            pool.return_connection(conn)
            
            print(f"   ✅ Database connection successful")
            print(f"   Result: test={result[0]}, timestamp={result[1]}")
            return True
            
    except Exception as e:
        print(f"   ❌ Database connection failed: {e}")
        print(f"   💡 Please check your database credentials and instance status")
        return False

def run_demo():
    """Run the enhanced demo"""
    print_step("Running enhanced benchmark demo...")
    
    return run_command(
        f"{sys.executable} enhanced_benchmark_example.py",
        "Enhanced benchmark demonstration"
    )

def show_next_steps():
    """Show next steps to the user"""
    print_step("Setup Complete! Next Steps:")
    
    print("""
   🚀 Getting Started:
   
   1. Edit Configuration:
      nano .env  # Add your actual Databricks credentials
   
   2. Run Basic Benchmark:
      python lakebase_benchmark.py
   
   3. Try Enhanced Features:
      python enhanced_benchmark_example.py
   
   4. Start Monitoring Dashboard:
      python fastapi_monitoring.py
      # Then visit: http://localhost:8000/dashboard
   
   5. Read Documentation:
      cat README_ENHANCED.md
   
   📚 Available Scripts:
   - lakebase_benchmark.py          # Original benchmark
   - enhanced_benchmark_example.py  # All enhanced features demo
   - fastapi_monitoring.py          # Real-time monitoring API
   - enhanced_production_pool.py    # Test enhanced connection pool
   
   📊 Key Features Available:
   ✅ Environment-based configuration
   ✅ Structured error handling
   ✅ Real-time performance monitoring
   ✅ Comprehensive health checks
   ✅ Production-grade connection pooling
   ✅ FastAPI monitoring dashboard
   ✅ WebSocket live updates
   
   🔧 Configuration Files:
   - .env                          # Environment variables (recommended)
   - lakebase_credentials.conf     # JSON config (fallback)
   - production_config_example.json # Production settings example
   """)

def main():
    """Main setup function"""
    print_header("ENHANCED LAKEBASE SETUP")
    
    print("This script will set up the enhanced Lakebase benchmark with FastAPI best practices.")
    print("Please ensure you have your Databricks workspace credentials ready.")
    
    # Step 1: Check Python version
    if not check_python_version():
        sys.exit(1)
    
    # Step 2: Install dependencies
    if not install_dependencies():
        print("\n❌ Dependency installation failed. Please resolve the issues and try again.")
        sys.exit(1)
    
    # Step 3: Create environment file
    if not create_env_file():
        print("\n❌ Environment file creation failed.")
        sys.exit(1)
    
    # Step 4: Validate configuration (may fail if not configured yet)
    print("\n" + "="*60)
    print("⚠️  CONFIGURATION REQUIRED")
    print("="*60)
    print("Before proceeding, please edit your .env file with actual values:")
    print("   nano .env")
    print("\nRequired values:")
    print("   DATABRICKS_WORKSPACE_URL=https://your-workspace.cloud.databricks.com")
    print("   DATABRICKS_PERSONAL_ACCESS_TOKEN=dapiXXXXXXXX")
    print("   DATABRICKS_USERNAME=your.email@company.com")
    print("   LAKEBASE_INSTANCE_NAME=your-database-instance")
    print("   LAKEBASE_HOST=your-instance.database.cloud.databricks.com")
    
    response = input("\nHave you configured your .env file? (y/n): ").strip().lower()
    
    if response == 'y':
        # Step 5: Validate configuration
        if not validate_configuration():
            print("\n⚠️  Configuration validation failed, but setup can continue.")
            print("Please fix configuration issues before running benchmarks.")
        
        # Step 6: Test database connection (optional)
        test_connection = input("\nTest database connection now? (y/n): ").strip().lower()
        if test_connection == 'y':
            if test_database_connection():
                print("\n✅ Database connection test passed!")
                
                # Step 7: Run demo (optional)
                run_demo_now = input("\nRun enhanced demo now? (y/n): ").strip().lower()
                if run_demo_now == 'y':
                    run_demo()
            else:
                print("\n⚠️  Database connection test failed.")
                print("Please check your configuration and try again later.")
    
    # Show next steps
    show_next_steps()
    
    print("\n🎉 Setup completed! You're ready to use the enhanced Lakebase benchmark.")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n👋 Setup interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\n\n❌ Setup failed with error: {e}")
        sys.exit(1)