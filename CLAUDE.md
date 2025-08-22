# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository Overview

This repository contains Databricks and Spark streaming code for DLT (Delta Live Tables) auto-scaling demos and examples, focusing on production-ready streaming patterns, benchmarking tools, and agent-based integrations.

## Project Structure

### Core Components

**agentbricks/** - Databricks Knowledge Agent client library
- Production-ready Python client for Databricks Knowledge Agent
- Environment-based configuration with comprehensive error handling
- Zero hardcoded values, fully configurable via environment variables

**lakebase/** - Enhanced Lakebase benchmarking suite  
- Production-grade database connection pooling and benchmarking
- FastAPI monitoring layer with real-time performance metrics
- Pydantic-based configuration with environment variable support

**notebooks/** - Spark streaming demos and examples
- 40+ Python notebooks demonstrating Spark streaming patterns
- DLT pipeline examples with merge operations
- Real-time data processing and analytics patterns

**utils.py** - Shared utilities for Spark operations

## Development Commands

### AgentBricks Module
```bash
# Install dependencies
cd agentbricks && pip install -r requirements_agent_bricks.txt

# Test functionality
python test_cursor_agent.py

# Check configuration
python show_config.py
python show_config.py help
python show_config.py missing
python show_config.py current
```

### Lakebase Module  
```bash
# Install dependencies
cd lakebase && pip install -r requirements.txt

# Run basic benchmark
python lakebase_benchmark.py

# Run enhanced benchmark with monitoring
python enhanced_production_pool.py

# Start FastAPI monitoring dashboard
python fastapi_monitoring.py
# Access dashboard at http://localhost:8000/dashboard

# Development testing (from lakebase README)
python -m pytest tests/
python -m flake8 *.py
python -m mypy *.py
```

### Notebooks
Notebooks are primarily .py files designed to run in Databricks environments. Most require:
- Spark session initialization
- Delta Lake libraries
- Databricks runtime environment

## Configuration Management

### AgentBricks Configuration
Uses environment variables for all configuration:
```bash
# Required
export DATABRICKS_TOKEN="your_token_here"
export DATABRICKS_BASE_URL="https://your-workspace.cloud.databricks.com/serving-endpoints"
export DATABRICKS_MODEL="ka-de16acc4-endpoint"

# Optional
export DATABRICKS_HOST="https://your-workspace.cloud.databricks.com"
export AGENTBRICKS_MAX_RETRIES="3"
export AGENTBRICKS_RETRY_DELAY="30"
export AGENTBRICKS_TIMEOUT="60"
```

### Lakebase Configuration  
Supports both environment variables (.env files) and JSON configuration:
```bash
# Environment approach (recommended)
export DATABRICKS_WORKSPACE_URL="https://your-workspace.cloud.databricks.com"
export DATABRICKS_PERSONAL_ACCESS_TOKEN="dapiXXXXXXXX"
export LAKEBASE_INSTANCE_NAME="your-database-instance"
export DB_POOL_SIZE="20"
export DB_MAX_OVERFLOW="30"
export ENABLE_PERFORMANCE_MONITORING="true"
```

## Architecture Patterns

### Connection Management
- **lakebase/enhanced_production_pool.py**: Production-grade connection pooling with auto-refresh, health monitoring, and performance metrics
- **lakebase/error_handling.py**: Structured error categorization and recovery strategies
- **lakebase/performance_monitoring.py**: Real-time performance tracking and metrics collection

### Streaming Patterns
The notebooks demonstrate key Spark streaming patterns:
- **foreachBatch processing**: Batch-level operations with merge capabilities
- **Stream-to-stream joins**: Real-time data joining patterns  
- **Delta Live Tables**: Automated pipeline creation and management
- **Liquid clustering**: Performance optimization for Delta tables
- **Dynamic file pruning**: Query optimization techniques

### Configuration Architecture
Both modules use layered configuration:
1. Environment variables (highest priority)
2. .env files (development)
3. JSON configuration files (backward compatibility)
4. Default values (fallback)

## Security Considerations

- All sensitive data (tokens, credentials) should be in environment variables
- SSL/TLS enforcement for all database connections
- Credential isolation - no credentials logged in production mode
- Application naming for connection monitoring and security auditing

## Testing Strategy

### AgentBricks
- Functional tests with real API calls (when token available)
- Configuration validation tests
- Error handling and retry logic tests
- Network connectivity tests

### Lakebase  
- Performance benchmarking with multiple thread counts
- Connection pool stress testing
- Health monitoring validation
- FastAPI endpoint testing

## Performance Tuning

### Connection Pool Sizing
```bash
# Light workload (development)
DB_POOL_SIZE=5
DB_MAX_OVERFLOW=10

# Medium workload (testing)  
DB_POOL_SIZE=20
DB_MAX_OVERFLOW=30

# Heavy workload (production)
DB_POOL_SIZE=50
DB_MAX_OVERFLOW=100
```

### Monitoring Configuration
```bash
ENABLE_PERFORMANCE_MONITORING=true
ENABLE_HEALTH_CHECKS=true
HEALTH_CHECK_INTERVAL=300
METRICS_COLLECTION_INTERVAL=60
```

## Common File Patterns

- **Configuration files**: Use Pydantic models with environment variable support
- **Error handling**: Structured exceptions with categorization and recovery strategies  
- **Logging**: Structured logging with performance metrics
- **Connection management**: Context managers for automatic resource cleanup
- **Monitoring**: Real-time metrics collection with FastAPI dashboards

## Dependencies

### Core Dependencies
- **databricks-sdk**: Databricks workspace integration
- **pydantic**: Configuration validation and type safety
- **fastapi**: Monitoring API layer
- **sqlalchemy**: Database connection management
- **delta-spark**: Delta Lake operations (for Databricks notebooks)

### Development Dependencies
- **pytest**: Testing framework (when available)
- **flake8**: Code linting (when available)  
- **mypy**: Type checking (when available)