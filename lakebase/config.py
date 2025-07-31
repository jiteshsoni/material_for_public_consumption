#!/usr/bin/env python3
"""
Enhanced Configuration Management with Environment Variables
Based on FastAPI best practices for production-ready configuration.

Features:
- Environment variable support with .env file loading
- Pydantic validation for type safety and data validation
- Fallback to JSON config files for backward compatibility
- Comprehensive logging configuration
- Security-focused credential management
"""

import os
import json
from typing import List, Optional, Dict, Any
from pydantic import Field, validator
from pydantic_settings import BaseSettings
from dotenv import load_dotenv
import logging

# Load environment variables from .env file if it exists
load_dotenv()

logger = logging.getLogger(__name__)


class DatabricksConfig(BaseSettings):
    """Databricks workspace configuration"""
    workspace_url: str = Field(..., description="Databricks workspace URL")
    workspace_id: Optional[str] = Field(None, description="Databricks workspace ID")
    personal_access_token: str = Field(..., description="Personal access token")
    username: str = Field(..., description="Databricks username")
    oauth_token: Optional[str] = Field(None, description="OAuth token (optional)")
    
    class Config:
        env_prefix = "DATABRICKS_"
        case_sensitive = False


class LakebaseConfig(BaseSettings):
    """Lakebase database instance configuration"""
    instance_name: str = Field(..., alias="name", description="Database instance name")
    instance_id: Optional[str] = Field(None, description="Database instance ID")
    host: str = Field(..., description="Database host")
    port: int = Field(5432, description="Database port")
    database: str = Field("databricks_postgres", description="Database name")
    
    class Config:
        env_prefix = "LAKEBASE_"
        case_sensitive = False


class BenchmarkConfig(BaseSettings):
    """Benchmark execution configuration"""
    thread_counts: List[int] = Field([20, 40, 80, 100], description="Thread counts to test")
    test_duration_seconds: int = Field(30, description="Test duration in seconds")
    table_rows: int = Field(1000000, description="Number of rows in test table")
    batch_size: int = Field(10000, description="Batch size for operations")
    table_name: str = Field("benchmark_test", description="Test table name")
    connection_stagger_delay: float = Field(0.05, description="Delay between connections")
    max_pool_init_time: int = Field(90, description="Max pool initialization time")
    
    @validator('thread_counts', pre=True)
    def parse_thread_counts(cls, v):
        if isinstance(v, str):
            return [int(x.strip()) for x in v.split(',')]
        return v
    
    class Config:
        env_prefix = "BENCHMARK_"
        case_sensitive = False


class ProductionPoolConfig(BaseSettings):
    """Production connection pool configuration based on FastAPI app best practices"""
    pool_size: int = Field(20, description="Base connection pool size")
    max_overflow: int = Field(30, description="Maximum overflow connections")
    pool_timeout: int = Field(30, description="Pool checkout timeout seconds")
    command_timeout: int = Field(10, description="Database command timeout seconds")
    pool_recycle_interval: int = Field(3000, description="Pool recycle interval seconds")
    enable_background_refresh: bool = Field(True, description="Enable background token refresh")
    health_check_interval: int = Field(300, description="Health check interval seconds")
    use_production_pool: bool = Field(True, description="Use production pool implementation")
    
    class Config:
        env_prefix = "DB_"
        case_sensitive = False


class SecurityConfig(BaseSettings):
    """Security configuration following FastAPI best practices"""
    ssl_require: bool = Field(True, description="Require SSL connections")
    application_name: str = Field("lakebase_benchmark", description="Application name for monitoring")
    log_credentials: bool = Field(False, description="Enable credential logging (NOT for production)")
    
    class Config:
        env_prefix = ""
        case_sensitive = False


class MonitoringConfig(BaseSettings):
    """Performance monitoring and logging configuration"""
    log_level: str = Field("INFO", description="Logging level")
    enable_performance_monitoring: bool = Field(True, description="Enable performance monitoring")
    enable_health_checks: bool = Field(True, description="Enable health checks")
    metrics_collection_interval: int = Field(60, description="Metrics collection interval")
    
    class Config:
        env_prefix = ""
        case_sensitive = False


class LakebaseSettings(BaseSettings):
    """Main configuration class combining all settings"""
    databricks: DatabricksConfig
    lakebase: LakebaseConfig
    benchmark: BenchmarkConfig
    production_pool: ProductionPoolConfig
    security: SecurityConfig = SecurityConfig()
    monitoring: MonitoringConfig = MonitoringConfig()
    
    @classmethod
    def from_env_or_json(cls, json_config_path: str = "lakebase_credentials.conf") -> "LakebaseSettings":
        """
        Create settings from environment variables or JSON config file
        Environment variables take precedence over JSON config
        """
        try:
            # First try to load from environment variables
            databricks = DatabricksConfig()
            lakebase = LakebaseConfig()
            benchmark = BenchmarkConfig()
            production_pool = ProductionPoolConfig()
            security = SecurityConfig()
            monitoring = MonitoringConfig()
            
            return cls(
                databricks=databricks,
                lakebase=lakebase,
                benchmark=benchmark,
                production_pool=production_pool,
                security=security,
                monitoring=monitoring
            )
            
        except Exception as env_error:
            logger.warning(f"Failed to load from environment variables: {env_error}")
            logger.info(f"Falling back to JSON config: {json_config_path}")
            
            # Fallback to JSON config
            return cls.from_json_config(json_config_path)
    
    @classmethod
    def from_json_config(cls, config_path: str) -> "LakebaseSettings":
        """Load configuration from JSON file (backward compatibility)"""
        try:
            with open(config_path, 'r') as f:
                config_data = json.load(f)
            
            # Map JSON structure to pydantic models
            databricks = DatabricksConfig(**config_data.get("databricks", {}))
            lakebase = LakebaseConfig(**config_data.get("lakebase", {}))
            benchmark = BenchmarkConfig(**config_data.get("benchmark", {}))
            production_pool = ProductionPoolConfig(**config_data.get("production_pool", {}))
            security = SecurityConfig()
            monitoring = MonitoringConfig()
            
            return cls(
                databricks=databricks,
                lakebase=lakebase,
                benchmark=benchmark,
                production_pool=production_pool,
                security=security,
                monitoring=monitoring
            )
            
        except Exception as e:
            logger.error(f"Failed to load JSON config from {config_path}: {e}")
            raise ValueError(f"Configuration loading failed: {e}")
    
    def setup_logging(self):
        """Setup logging configuration based on monitoring settings"""
        logging.basicConfig(
            level=getattr(logging, self.monitoring.log_level.upper()),
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        
        # Ensure database credentials are not logged
        if not self.security.log_credentials:
            logging.getLogger("databricks.sdk").setLevel(logging.WARNING)
            logging.getLogger("psycopg2").setLevel(logging.WARNING)
    
    def validate_configuration(self) -> Dict[str, Any]:
        """Validate configuration and return validation report"""
        validation_report = {
            "valid": True,
            "warnings": [],
            "errors": [],
            "recommendations": []
        }
        
        # Security validations
        if not self.security.ssl_require:
            validation_report["warnings"].append("SSL is not required - consider enabling for production")
        
        if self.security.log_credentials:
            validation_report["errors"].append("Credential logging is enabled - DISABLE for production")
            validation_report["valid"] = False
        
        # Performance recommendations
        if self.production_pool.pool_size < 10:
            validation_report["recommendations"].append("Consider increasing pool_size for better performance")
        
        if self.production_pool.pool_recycle_interval > 3600:
            validation_report["warnings"].append("Pool recycle interval > 1 hour may cause token expiry issues")
        
        # Monitoring recommendations
        if not self.monitoring.enable_performance_monitoring:
            validation_report["recommendations"].append("Enable performance monitoring for production insights")
        
        return validation_report


def load_settings(config_path: str = None) -> LakebaseSettings:
    """
    Convenience function to load settings with proper error handling.
    Automatically finds the appropriate configuration file.
    """
    if config_path is None:
        # Try to find configuration file in order of preference
        config_candidates = [
            "lakebase_credentials.conf",  # User's actual config (gitignored)
            "lakebase_credentials.conf.template", 
            "lakebase_credentials.conf.example",
            ".env"  # Environment variables only
        ]
        
        config_path = None
        for candidate in config_candidates:
            if os.path.exists(candidate):
                config_path = candidate
                break
        
        if config_path is None:
            logger.warning("No configuration file found. Using environment variables only.")
            config_path = "lakebase_credentials.conf"  # Will trigger env-only mode
    try:
        settings = LakebaseSettings.from_env_or_json(config_path)
        settings.setup_logging()
        
        # Validate configuration
        validation = settings.validate_configuration()
        if not validation["valid"]:
            for error in validation["errors"]:
                logger.error(f"Configuration Error: {error}")
            raise ValueError("Configuration validation failed")
        
        for warning in validation["warnings"]:
            logger.warning(f"Configuration Warning: {warning}")
        
        for rec in validation["recommendations"]:
            logger.info(f"Configuration Recommendation: {rec}")
        
        logger.info("Configuration loaded and validated successfully")
        return settings
        
    except Exception as e:
        logger.error(f"Failed to load configuration: {e}")
        raise


# Example usage and testing
if __name__ == "__main__":
    # Test configuration loading
    try:
        settings = load_settings()
        print("✅ Configuration loaded successfully")
        print(f"Database: {settings.lakebase.host}:{settings.lakebase.port}")
        print(f"Pool size: {settings.production_pool.pool_size}")
        print(f"Thread counts: {settings.benchmark.thread_counts}")
        
    except Exception as e:
        print(f"❌ Configuration loading failed: {e}")