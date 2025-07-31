#!/usr/bin/env python3
"""
Enhanced Production-Grade Lakebase Connection Pool
Integrating all FastAPI best practices and monitoring capabilities.

This module combines:
- Environment-based configuration with pydantic validation
- Comprehensive error handling and categorization
- Performance monitoring and metrics collection
- Health monitoring with detailed diagnostics
- Security best practices
- Structured logging
"""

import asyncio
import logging
import queue
import random
import threading
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, Optional, Callable, Any, List

import psycopg2
from psycopg2.extras import RealDictCursor

# Import our new best practice modules
from config import load_settings, LakebaseSettings
from error_handling import (
    ErrorHandler, 
    DatabaseConnectionError, 
    DatabaseTimeoutError,
    handle_error,
    with_error_handling
)
from performance_monitoring import (
    PerformanceMonitor,
    PerformanceTimer,
    ConnectionPoolMetrics,
    timed_operation
)
from health_monitoring import HealthMonitor, HealthChecker
from lakebase_sdk_auth import LakebaseSDKAuthManager

logger = logging.getLogger(__name__)


class EnhancedProductionConnectionPool:
    """
    Production-grade connection pool integrating all FastAPI best practices
    """
    
    def __init__(
        self,
        auth_manager: Optional[LakebaseSDKAuthManager] = None,
        settings: Optional[LakebaseSettings] = None,
        config_file: str = None
    ):
        """
        Initialize enhanced production connection pool
        
        Args:
            auth_manager: Optional pre-configured auth manager
            settings: Optional pre-loaded settings
            config_file: Configuration file path for fallback
        """
        # Load configuration using new pydantic-based system
        self.settings = settings or load_settings(config_file)
        
        # Initialize monitoring and error handling
        self.error_handler = ErrorHandler(enable_metrics=True)
        self.performance_monitor = PerformanceMonitor(
            enable_detailed_metrics=self.settings.monitoring.enable_performance_monitoring
        )
        self.health_monitor = HealthMonitor(
            check_interval_seconds=self.settings.production_pool.health_check_interval
        )
        
        # Pool configuration from settings
        self.pool_size = self.settings.production_pool.pool_size
        self.max_overflow = self.settings.production_pool.max_overflow
        self.pool_timeout = self.settings.production_pool.pool_timeout
        self.command_timeout = self.settings.production_pool.command_timeout
        self.pool_recycle_interval = self.settings.production_pool.pool_recycle_interval
        
        # Connection management
        self.auth_manager = auth_manager or LakebaseSDKAuthManager(config_file)
        self.pool = queue.Queue(maxsize=self.pool_size + self.max_overflow)
        self.active_connections = set()
        self.connection_stats = {
            'total_created': 0,
            'total_closed': 0,
            'current_active': 0,
            'total_checkouts': 0,
            'total_checkins': 0,
            'checkout_timeouts': 0,
            'connection_errors': 0,
            'last_cleanup': time.time()
        }
        
        # Thread safety
        self.lock = threading.RLock()
        self.pool_initialized = False
        
        # Connection creation throttling (max 5 concurrent connection creations)
        self.connection_creation_semaphore = threading.Semaphore(5)
        self.last_connection_batch_time = 0
        self.connection_batch_delay = 0.5  # 500ms delay between batches
        
        # Background tasks
        self.background_tasks = []
        self.shutdown_event = threading.Event()
        
        # Initialize components
        self._setup_logging()
        self._setup_health_monitoring()
        
        logger.info(f"Enhanced production pool initialized with {self.pool_size} base connections")
    
    def _setup_logging(self):
        """Setup structured logging based on configuration"""
        log_level = getattr(logging, self.settings.monitoring.log_level.upper())
        logging.getLogger().setLevel(log_level)
        
        # Ensure sensitive information is not logged
        if not self.settings.security.log_credentials:
            logging.getLogger("psycopg2").setLevel(logging.WARNING)
            logging.getLogger("databricks.sdk").setLevel(logging.WARNING)
    
    def _setup_health_monitoring(self):
        """Setup comprehensive health monitoring"""
        if not self.settings.monitoring.enable_health_checks:
            return
        
        # Add connection pool health checker
        self.health_monitor.add_connection_pool_health_checker(self)
        
        # Add database health checkers
        try:
            conn_params = self.auth_manager.get_connection_params()
            self.health_monitor.add_database_health_checker(conn_params)
        except Exception as e:
            logger.warning(f"Failed to setup database health checker: {e}")
        
        # Add Databricks health checkers
        try:
            workspace_client = self.auth_manager.workspace_client
            instance_name = self.settings.lakebase.instance_name
            self.health_monitor.add_databricks_health_checker(workspace_client, instance_name)
        except Exception as e:
            logger.warning(f"Failed to setup Databricks health checker: {e}")
    
    @with_error_handling("pool_initialization")
    def initialize_pool(self):
        """Initialize the connection pool with error handling and monitoring"""
        if self.pool_initialized:
            logger.warning("Pool already initialized")
            return
        
        logger.info(f"Initializing connection pool with {self.pool_size} connections")
        
        with PerformanceTimer("pool_initialization", self.performance_monitor):
            # Create initial connections with stagger
            successful_connections = 0
            
            for i in range(self.pool_size):
                try:
                    with PerformanceTimer("connection_creation", self.performance_monitor):
                        conn = self._create_connection()
                        self.pool.put(conn, block=False)
                        successful_connections += 1
                        
                    # Stagger connection creation to avoid rate limits
                    if i < self.pool_size - 1:
                        time.sleep(self.settings.benchmark.connection_stagger_delay)
                        
                except Exception as e:
                    error_context = self.error_handler.handle_exception(
                        e, f"connection_creation_{i}", retry_count=0
                    )
                    logger.error(f"Failed to create connection {i}: {error_context.message}")
                    self.connection_stats['connection_errors'] += 1
            
            if successful_connections == 0:
                raise DatabaseConnectionError(
                    "Failed to create any connections during pool initialization"
                )
            
            logger.info(f"Pool initialized with {successful_connections}/{self.pool_size} connections")
            self.pool_initialized = True
            
            # Start background tasks
            self._start_background_tasks()
    
    def _create_connection(self) -> psycopg2.extensions.connection:
        """Create a new database connection with throttling and full error handling"""
        # Acquire semaphore to limit concurrent connection creation to 5
        with self.connection_creation_semaphore:
            # Add delay between connection creation batches
            current_time = time.time()
            if current_time - self.last_connection_batch_time < self.connection_batch_delay:
                sleep_time = self.connection_batch_delay - (current_time - self.last_connection_batch_time)
                logger.debug(f"Throttling connection creation: sleeping {sleep_time:.3f}s")
                time.sleep(sleep_time)
            
            try:
                conn_params = self.auth_manager.get_connection_params()
                
                # Add security configurations
                if self.settings.security.ssl_require:
                    conn_params['sslmode'] = 'require'
                
                conn_params['application_name'] = self.settings.security.application_name
                conn_params['connect_timeout'] = self.command_timeout
                
                conn = psycopg2.connect(**conn_params)
                conn.set_session(autocommit=True)
                
                # Track connection creation and update batch time
                with self.lock:
                    self.connection_stats['total_created'] += 1
                    self.active_connections.add(id(conn))
                    self.last_connection_batch_time = time.time()
                
                logger.debug(f"Created new connection: {id(conn)}")
                return conn
                
            except Exception as e:
                raise DatabaseConnectionError(
                    f"Failed to create database connection: {str(e)}",
                    details={"connection_params_keys": list(conn_params.keys()) if 'conn_params' in locals() else []}
                )
    
    @timed_operation("connection_checkout")
    def get_connection(self, timeout: Optional[float] = None) -> psycopg2.extensions.connection:
        """Get a connection from the pool with comprehensive monitoring"""
        timeout = timeout or self.pool_timeout
        start_time = time.time()
        
        try:
            with self.lock:
                self.connection_stats['total_checkouts'] += 1
            
            # Try to get connection from pool
            try:
                conn = self.pool.get(timeout=timeout)
                
                # Test connection health
                if self._is_connection_healthy(conn):
                    with self.lock:
                        self.connection_stats['current_active'] += 1
                    return conn
                else:
                    # Connection is stale, close it and create new one
                    self._close_connection(conn)
                    
            except queue.Empty:
                # Pool is empty, try to create overflow connection
                if len(self.active_connections) < (self.pool_size + self.max_overflow):
                    with PerformanceTimer("overflow_connection_creation", self.performance_monitor):
                        conn = self._create_connection()
                        with self.lock:
                            self.connection_stats['current_active'] += 1
                        return conn
                else:
                    # No connections available and at max capacity
                    with self.lock:
                        self.connection_stats['checkout_timeouts'] += 1
                    
                    elapsed = time.time() - start_time
                    raise DatabaseTimeoutError(
                        f"Connection checkout timeout after {elapsed:.2f}s",
                        details={
                            "timeout_seconds": timeout,
                            "pool_size": self.pool_size,
                            "max_overflow": self.max_overflow,
                            "active_connections": len(self.active_connections)
                        }
                    )
            
            # If we get here, we need to create a new connection (healthy check failed)
            with PerformanceTimer("connection_replacement", self.performance_monitor):
                conn = self._create_connection()
                with self.lock:
                    self.connection_stats['current_active'] += 1
                return conn
                
        except Exception as e:
            elapsed = time.time() - start_time
            error_context = self.error_handler.handle_exception(
                e, "connection_checkout", duration_ms=elapsed * 1000
            )
            
            if isinstance(e, (DatabaseConnectionError, DatabaseTimeoutError)):
                raise
            else:
                raise DatabaseConnectionError(
                    f"Connection checkout failed: {error_context.message}",
                    details=error_context.details
                )
    
    def _is_connection_healthy(self, conn: psycopg2.extensions.connection) -> bool:
        """Check if connection is healthy and not expired"""
        try:
            # Check if connection is closed
            if conn.closed:
                return False
            
            # Quick health check query
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None
                
        except Exception as e:
            logger.debug(f"Connection health check failed: {e}")
            return False
    
    @timed_operation("connection_checkin")
    def return_connection(self, conn: psycopg2.extensions.connection):
        """Return a connection to the pool with monitoring"""
        if not conn or conn.closed:
            logger.warning("Attempted to return closed/None connection")
            return
        
        try:
            # Reset connection state
            conn.rollback()
            
            # Check if connection should be recycled
            if self._should_recycle_connection(conn):
                self._close_connection(conn)
            else:
                # Return to pool if space available
                try:
                    self.pool.put(conn, block=False)
                except queue.Full:
                    # Pool is full, close overflow connection
                    self._close_connection(conn)
            
            with self.lock:
                self.connection_stats['total_checkins'] += 1
                self.connection_stats['current_active'] = max(0, 
                    self.connection_stats['current_active'] - 1)
                
        except Exception as e:
            error_context = self.error_handler.handle_exception(e, "connection_checkin")
            logger.error(f"Error returning connection: {error_context.message}")
            self._close_connection(conn)
    
    def _should_recycle_connection(self, conn: psycopg2.extensions.connection) -> bool:
        """Determine if connection should be recycled based on age and health"""
        try:
            # Check connection age (recycle before token expiry)
            conn_id = id(conn)
            if hasattr(conn, '_created_at'):
                age_seconds = time.time() - conn._created_at
                if age_seconds > self.pool_recycle_interval:
                    logger.debug(f"Recycling connection {conn_id} due to age: {age_seconds}s")
                    return True
            
            # Additional health checks could go here
            return False
            
        except Exception:
            return True  # Recycle on any error
    
    def _close_connection(self, conn: psycopg2.extensions.connection):
        """Close a connection with proper cleanup and tracking"""
        try:
            conn_id = id(conn)
            conn.close()
            
            with self.lock:
                self.connection_stats['total_closed'] += 1
                self.active_connections.discard(conn_id)
            
            logger.debug(f"Closed connection: {conn_id}")
            
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")
    
    def _start_background_tasks(self):
        """Start background monitoring and maintenance tasks"""
        if not self.settings.production_pool.enable_background_refresh:
            return
        
        # Token refresh task (every 50 minutes as per FastAPI app)
        refresh_task = threading.Thread(
            target=self._background_token_refresh,
            daemon=True,
            name="token_refresh"
        )
        refresh_task.start()
        self.background_tasks.append(refresh_task)
        
        # Pool maintenance task
        maintenance_task = threading.Thread(
            target=self._background_pool_maintenance,
            daemon=True,
            name="pool_maintenance"
        )
        maintenance_task.start()
        self.background_tasks.append(maintenance_task)
        
        # Health monitoring task
        if self.settings.monitoring.enable_health_checks:
            health_task = threading.Thread(
                target=self._background_health_monitoring,
                daemon=True,
                name="health_monitoring"
            )
            health_task.start()
            self.background_tasks.append(health_task)
        
        logger.info(f"Started {len(self.background_tasks)} background tasks")
    
    def _background_token_refresh(self):
        """Background task for token refresh (50-minute interval)"""
        while not self.shutdown_event.is_set():
            try:
                # Wait 50 minutes (3000 seconds)
                if self.shutdown_event.wait(timeout=3000):
                    break
                
                logger.info("Starting background token refresh")
                with PerformanceTimer("token_refresh", self.performance_monitor):
                    # Refresh auth manager token
                    if hasattr(self.auth_manager, 'refresh_token'):
                        self.auth_manager.refresh_token()
                    
                    # Optionally refresh some connections
                    self._refresh_pool_connections()
                
                logger.info("Background token refresh completed")
                
            except Exception as e:
                error_context = self.error_handler.handle_exception(e, "token_refresh")
                logger.error(f"Background token refresh failed: {error_context.message}")
    
    def _background_pool_maintenance(self):
        """Background task for pool maintenance"""
        while not self.shutdown_event.is_set():
            try:
                # Run maintenance every 5 minutes
                if self.shutdown_event.wait(timeout=300):
                    break
                
                with PerformanceTimer("pool_maintenance", self.performance_monitor):
                    self._perform_pool_maintenance()
                
            except Exception as e:
                error_context = self.error_handler.handle_exception(e, "pool_maintenance")
                logger.error(f"Pool maintenance failed: {error_context.message}")
    
    def _background_health_monitoring(self):
        """Background task for health monitoring"""
        while not self.shutdown_event.is_set():
            try:
                # Health check interval from configuration
                interval = self.settings.production_pool.health_check_interval
                if self.shutdown_event.wait(timeout=interval):
                    break
                
                # Run health checks (this is async, so we need to handle it)
                # For now, just update connection pool metrics
                self._update_pool_metrics()
                
            except Exception as e:
                error_context = self.error_handler.handle_exception(e, "health_monitoring")
                logger.error(f"Health monitoring failed: {error_context.message}")
    
    def _refresh_pool_connections(self):
        """Refresh a portion of pool connections"""
        refreshed = 0
        max_refresh = min(5, self.pool_size // 4)  # Refresh up to 25% or 5 connections
        
        for _ in range(max_refresh):
            try:
                conn = self.pool.get(block=False)
                self._close_connection(conn)
                
                # Create replacement
                new_conn = self._create_connection()
                self.pool.put(new_conn, block=False)
                refreshed += 1
                
            except queue.Empty:
                break
            except Exception as e:
                logger.warning(f"Error refreshing connection: {e}")
        
        if refreshed > 0:
            logger.info(f"Refreshed {refreshed} pool connections")
    
    def _perform_pool_maintenance(self):
        """Perform routine pool maintenance"""
        with self.lock:
            stats = self.connection_stats.copy()
        
        logger.debug(f"Pool maintenance - Active: {stats['current_active']}, "
                    f"Created: {stats['total_created']}, Closed: {stats['total_closed']}")
        
        # Update performance metrics
        self._update_pool_metrics()
        
        # Log performance summary periodically
        if time.time() - stats['last_cleanup'] > 1800:  # Every 30 minutes
            self.performance_monitor.log_performance_summary()
            with self.lock:
                self.connection_stats['last_cleanup'] = time.time()
    
    def _update_pool_metrics(self):
        """Update connection pool metrics for monitoring"""
        with self.lock:
            stats = self.connection_stats.copy()
            pool_size = self.pool.qsize()
            
        metrics = ConnectionPoolMetrics(
            pool_size=self.pool_size,
            active_connections=stats['current_active'],
            idle_connections=pool_size,
            overflow_connections=max(0, stats['current_active'] - self.pool_size),
            total_checkouts=stats['total_checkouts'],
            total_checkins=stats['total_checkins'],
            checkout_timeouts=stats['checkout_timeouts'],
            connection_errors=stats['connection_errors'],
            average_checkout_time_ms=0.0  # Could be calculated from performance data
        )
        
        self.performance_monitor.update_connection_pool_metrics(metrics)
    
    def get_pool_status(self) -> Dict[str, Any]:
        """Get comprehensive pool status for monitoring"""
        with self.lock:
            stats = self.connection_stats.copy()
            
        pool_size = self.pool.qsize()
        utilization_rate = (stats['current_active'] / max(self.pool_size, 1)) * 100
        
        return {
            "pool_size": self.pool_size,
            "max_overflow": self.max_overflow,
            "available_connections": pool_size,
            "active_connections": stats['current_active'],
            "overflow_connections": max(0, stats['current_active'] - self.pool_size),
            "utilization_rate": round(utilization_rate, 2),
            "total_created": stats['total_created'],
            "total_closed": stats['total_closed'],
            "total_checkouts": stats['total_checkouts'],
            "total_checkins": stats['total_checkins'],
            "checkout_timeouts": stats['checkout_timeouts'],
            "connection_errors": stats['connection_errors'],
            "failed_connections": stats['connection_errors'],
            "pool_initialized": self.pool_initialized
        }
    
    def get_performance_metrics(self) -> Dict[str, Any]:
        """Get performance metrics"""
        return self.performance_monitor.get_performance_summary()
    
    def get_health_status(self) -> Optional[Dict[str, Any]]:
        """Get current health status"""
        return self.health_monitor.get_current_health()
    
    def close_all(self):
        """Close all connections and shutdown background tasks"""
        logger.info("Shutting down enhanced production connection pool")
        
        # Signal background tasks to stop
        self.shutdown_event.set()
        
        # Wait for background tasks to complete
        for task in self.background_tasks:
            if task.is_alive():
                task.join(timeout=5)
        
        # Close all connections
        closed_count = 0
        while True:
            try:
                conn = self.pool.get(block=False)
                self._close_connection(conn)
                closed_count += 1
            except queue.Empty:
                break
        
        logger.info(f"Closed {closed_count} pool connections")
        
        # Stop health monitoring synchronously
        try:
            # Try to stop monitoring gracefully
            if hasattr(self.health_monitor, '_stop_monitoring_sync'):
                self.health_monitor._stop_monitoring_sync()
            else:
                # Fallback: just mark it as stopped
                self.health_monitor._monitoring_active = False
                logger.info("Health monitor stopped (sync fallback)")
        except Exception as e:
            logger.warning(f"Could not stop health monitor gracefully: {e}")
        
        logger.info("Enhanced production pool shutdown complete")
    
    def __enter__(self):
        """Context manager entry"""
        if not self.pool_initialized:
            self.initialize_pool()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        self.close_all()


# Convenience function to create configured pool
def create_enhanced_pool(config_file: str = None) -> EnhancedProductionConnectionPool:
    """Create a fully configured enhanced production pool"""
    settings = load_settings(config_file)
    auth_manager = LakebaseSDKAuthManager(config_file)
    
    pool = EnhancedProductionConnectionPool(
        auth_manager=auth_manager,
        settings=settings,
        config_file=config_file
    )
    
    return pool


# Example usage
if __name__ == "__main__":
    # Test the enhanced pool
    try:
        with create_enhanced_pool() as pool:
            print("✅ Enhanced pool created successfully")
            
            # Test connection checkout/checkin
            conn = pool.get_connection()
            print("✅ Connection checked out")
            
            # Test query
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("SELECT 1 as test")
                result = cursor.fetchone()
                print(f"✅ Query result: {result}")
            
            pool.return_connection(conn)
            print("✅ Connection returned")
            
            # Show status
            status = pool.get_pool_status()
            print(f"📊 Pool Status: {status}")
            
            # Show performance metrics
            metrics = pool.get_performance_metrics()
            print(f"📈 Performance Metrics: {metrics['recent_5min']}")
            
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()