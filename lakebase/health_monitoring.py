#!/usr/bin/env python3
"""
Enhanced Health Monitoring System
Based on FastAPI best practices for comprehensive health checks and monitoring.

Features:
- Database health checks with detailed diagnostics
- Connection pool health monitoring
- System resource monitoring
- Health status aggregation
- Alerting and recovery recommendations
"""

import asyncio
import logging
import time
import threading
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
import psycopg2
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError

logger = logging.getLogger(__name__)


class HealthStatus(Enum):
    """Health status levels"""
    HEALTHY = "healthy"
    WARNING = "warning"
    UNHEALTHY = "unhealthy"
    CRITICAL = "critical"


@dataclass
class HealthCheckResult:
    """Individual health check result"""
    component: str
    status: HealthStatus
    message: str
    timestamp: datetime
    response_time_ms: float
    details: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "component": self.component,
            "status": self.status.value,
            "message": self.message,
            "timestamp": self.timestamp.isoformat(),
            "response_time_ms": round(self.response_time_ms, 2),
            "details": self.details
        }


@dataclass
class SystemHealth:
    """Overall system health status"""
    overall_status: HealthStatus
    timestamp: datetime
    checks: List[HealthCheckResult]
    uptime_seconds: int
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "overall_status": self.overall_status.value,
            "timestamp": self.timestamp.isoformat(),
            "uptime_seconds": self.uptime_seconds,
            "checks": [check.to_dict() for check in self.checks],
            "summary": {
                "total_checks": len(self.checks),
                "healthy": len([c for c in self.checks if c.status == HealthStatus.HEALTHY]),
                "warning": len([c for c in self.checks if c.status == HealthStatus.WARNING]),
                "unhealthy": len([c for c in self.checks if c.status == HealthStatus.UNHEALTHY]),
                "critical": len([c for c in self.checks if c.status == HealthStatus.CRITICAL])
            }
        }


class HealthChecker:
    """Individual health check implementation"""
    
    def __init__(self, name: str, check_function: Callable, timeout_seconds: int = 10):
        self.name = name
        self.check_function = check_function
        self.timeout_seconds = timeout_seconds
    
    async def run_check(self) -> HealthCheckResult:
        """Run the health check with timeout"""
        start_time = time.time()
        
        try:
            # Run check with timeout
            result = await asyncio.wait_for(
                self.check_function(),
                timeout=self.timeout_seconds
            )
            
            response_time_ms = (time.time() - start_time) * 1000
            
            if isinstance(result, HealthCheckResult):
                result.response_time_ms = response_time_ms
                return result
            else:
                return HealthCheckResult(
                    component=self.name,
                    status=HealthStatus.HEALTHY,
                    message="Check passed",
                    timestamp=datetime.now(),
                    response_time_ms=response_time_ms,
                    details=result if isinstance(result, dict) else {}
                )
                
        except asyncio.TimeoutError:
            response_time_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.CRITICAL,
                message=f"Health check timed out after {self.timeout_seconds}s",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
                details={"timeout_seconds": self.timeout_seconds}
            )
            
        except Exception as e:
            response_time_ms = (time.time() - start_time) * 1000
            return HealthCheckResult(
                component=self.name,
                status=HealthStatus.UNHEALTHY,
                message=f"Health check failed: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=response_time_ms,
                details={"error": str(e), "error_type": type(e).__name__}
            )


class DatabaseHealthChecker:
    """Specialized health checker for database connections"""
    
    def __init__(self, connection_params: Dict[str, Any]):
        self.connection_params = connection_params
    
    async def check_database_connection(self) -> HealthCheckResult:
        """Check basic database connectivity"""
        try:
            conn = psycopg2.connect(**self.connection_params)
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                
            conn.close()
            
            return HealthCheckResult(
                component="database_connection",
                status=HealthStatus.HEALTHY,
                message="Database connection successful",
                timestamp=datetime.now(),
                response_time_ms=0,  # Will be set by HealthChecker
                details={"query_result": result[0] if result else None}
            )
            
        except Exception as e:
            return HealthCheckResult(
                component="database_connection",
                status=HealthStatus.CRITICAL,
                message=f"Database connection failed: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=0,
                details={"error": str(e), "error_type": type(e).__name__}
            )
    
    async def check_database_performance(self) -> HealthCheckResult:
        """Check database performance with a simple query"""
        query_start = time.time()
        
        try:
            conn = psycopg2.connect(**self.connection_params)
            with conn.cursor() as cursor:
                cursor.execute("SELECT pg_database_size(current_database())")
                db_size = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM pg_stat_activity WHERE state = 'active'")
                active_connections = cursor.fetchone()[0]
                
            conn.close()
            
            query_time_ms = (time.time() - query_start) * 1000
            
            # Determine status based on performance
            if query_time_ms > 5000:
                status = HealthStatus.CRITICAL
                message = f"Database queries are very slow ({query_time_ms:.1f}ms)"
            elif query_time_ms > 1000:
                status = HealthStatus.WARNING
                message = f"Database queries are slow ({query_time_ms:.1f}ms)"
            else:
                status = HealthStatus.HEALTHY
                message = f"Database performance is good ({query_time_ms:.1f}ms)"
            
            return HealthCheckResult(
                component="database_performance",
                status=status,
                message=message,
                timestamp=datetime.now(),
                response_time_ms=0,
                details={
                    "query_time_ms": round(query_time_ms, 2),
                    "database_size_bytes": db_size,
                    "active_connections": active_connections
                }
            )
            
        except Exception as e:
            return HealthCheckResult(
                component="database_performance",
                status=HealthStatus.UNHEALTHY,
                message=f"Database performance check failed: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=0,
                details={"error": str(e), "error_type": type(e).__name__}
            )


class DatabricksHealthChecker:
    """Health checker for Databricks SDK connectivity"""
    
    def __init__(self, workspace_client: WorkspaceClient, instance_name: str):
        self.workspace_client = workspace_client
        self.instance_name = instance_name
    
    async def check_workspace_connectivity(self) -> HealthCheckResult:
        """Check Databricks workspace connectivity"""
        try:
            # Test workspace connectivity
            current_user = self.workspace_client.current_user.me()
            
            return HealthCheckResult(
                component="databricks_workspace",
                status=HealthStatus.HEALTHY,
                message="Databricks workspace connection successful",
                timestamp=datetime.now(),
                response_time_ms=0,
                details={"user": current_user.user_name if current_user else "unknown"}
            )
            
        except DatabricksError as e:
            status = HealthStatus.CRITICAL if "401" in str(e) or "403" in str(e) else HealthStatus.UNHEALTHY
            return HealthCheckResult(
                component="databricks_workspace",
                status=status,
                message=f"Databricks workspace connection failed: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=0,
                details={"error": str(e), "error_type": type(e).__name__}
            )
        except Exception as e:
            return HealthCheckResult(
                component="databricks_workspace",
                status=HealthStatus.UNHEALTHY,
                message=f"Databricks workspace check failed: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=0,
                details={"error": str(e), "error_type": type(e).__name__}
            )
    
    async def check_database_instance(self) -> HealthCheckResult:
        """Check database instance status"""
        try:
            instance = self.workspace_client.database.get_database_instance(name=self.instance_name)
            
            # Check instance state
            if hasattr(instance, 'state') and instance.state:
                if instance.state.value.upper() == 'RUNNING':
                    status = HealthStatus.HEALTHY
                    message = f"Database instance {self.instance_name} is running"
                elif instance.state.value.upper() in ['STARTING', 'STOPPING']:
                    status = HealthStatus.WARNING
                    message = f"Database instance {self.instance_name} is {instance.state.value.lower()}"
                else:
                    status = HealthStatus.UNHEALTHY
                    message = f"Database instance {self.instance_name} is in state: {instance.state.value}"
            else:
                status = HealthStatus.HEALTHY
                message = f"Database instance {self.instance_name} is accessible"
            
            details = {
                "instance_name": self.instance_name,
                "instance_id": getattr(instance, 'id', 'unknown'),
                "state": getattr(instance.state, 'value', 'unknown') if hasattr(instance, 'state') else 'unknown'
            }
            
            return HealthCheckResult(
                component="database_instance",
                status=status,
                message=message,
                timestamp=datetime.now(),
                response_time_ms=0,
                details=details
            )
            
        except Exception as e:
            return HealthCheckResult(
                component="database_instance",
                status=HealthStatus.CRITICAL,
                message=f"Database instance check failed: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=0,
                details={"error": str(e), "error_type": type(e).__name__}
            )


class ConnectionPoolHealthChecker:
    """Health checker for connection pool status"""
    
    def __init__(self, pool):
        self.pool = pool
    
    async def check_pool_health(self) -> HealthCheckResult:
        """Check connection pool health"""
        try:
            if not hasattr(self.pool, 'get_pool_status'):
                return HealthCheckResult(
                    component="connection_pool",
                    status=HealthStatus.WARNING,
                    message="Pool status not available",
                    timestamp=datetime.now(),
                    response_time_ms=0,
                    details={"reason": "Pool does not support status checking"}
                )
            
            pool_status = self.pool.get_pool_status()
            
            # Analyze pool health
            if pool_status.get('available_connections', 0) == 0:
                status = HealthStatus.CRITICAL
                message = "No available connections in pool"
            elif pool_status.get('utilization_rate', 0) > 90:
                status = HealthStatus.WARNING
                message = f"High pool utilization: {pool_status.get('utilization_rate', 0):.1f}%"
            elif pool_status.get('failed_connections', 0) > 0:
                status = HealthStatus.WARNING
                message = f"Some failed connections: {pool_status.get('failed_connections', 0)}"
            else:
                status = HealthStatus.HEALTHY
                message = "Connection pool is healthy"
            
            return HealthCheckResult(
                component="connection_pool",
                status=status,
                message=message,
                timestamp=datetime.now(),
                response_time_ms=0,
                details=pool_status
            )
            
        except Exception as e:
            return HealthCheckResult(
                component="connection_pool",
                status=HealthStatus.UNHEALTHY,
                message=f"Pool health check failed: {str(e)}",
                timestamp=datetime.now(),
                response_time_ms=0,
                details={"error": str(e), "error_type": type(e).__name__}
            )


class HealthMonitor:
    """Comprehensive health monitoring system"""
    
    def __init__(self, check_interval_seconds: int = 60):
        self.check_interval_seconds = check_interval_seconds
        self.health_checkers: List[HealthChecker] = []
        self.last_health_check: Optional[SystemHealth] = None
        self.start_time = datetime.now()
        self.monitoring_task: Optional[asyncio.Task] = None
        self.is_monitoring = False
        
        # Health history for trend analysis
        self.health_history: List[SystemHealth] = []
        self.max_history_size = 100
    
    def add_health_checker(self, checker: HealthChecker):
        """Add a health checker to the monitoring system"""
        self.health_checkers.append(checker)
        logger.info(f"Added health checker: {checker.name}")
    
    def add_database_health_checker(self, connection_params: Dict[str, Any]):
        """Add database health checkers"""
        db_checker = DatabaseHealthChecker(connection_params)
        
        self.add_health_checker(HealthChecker(
            "database_connection",
            db_checker.check_database_connection,
            timeout_seconds=10
        ))
        
        self.add_health_checker(HealthChecker(
            "database_performance", 
            db_checker.check_database_performance,
            timeout_seconds=15
        ))
    
    def add_databricks_health_checker(self, workspace_client: WorkspaceClient, instance_name: str):
        """Add Databricks health checkers"""
        databricks_checker = DatabricksHealthChecker(workspace_client, instance_name)
        
        self.add_health_checker(HealthChecker(
            "databricks_workspace",
            databricks_checker.check_workspace_connectivity,
            timeout_seconds=10
        ))
        
        self.add_health_checker(HealthChecker(
            "database_instance",
            databricks_checker.check_database_instance,
            timeout_seconds=15
        ))
    
    def add_connection_pool_health_checker(self, pool):
        """Add connection pool health checker"""
        pool_checker = ConnectionPoolHealthChecker(pool)
        
        self.add_health_checker(HealthChecker(
            "connection_pool",
            pool_checker.check_pool_health,
            timeout_seconds=5
        ))
    
    async def run_health_checks(self) -> SystemHealth:
        """Run all health checks and return system health status"""
        check_results = []
        
        # Run all health checks concurrently
        check_tasks = [checker.run_check() for checker in self.health_checkers]
        
        if check_tasks:
            check_results = await asyncio.gather(*check_tasks, return_exceptions=True)
            
            # Handle any exceptions from gather
            for i, result in enumerate(check_results):
                if isinstance(result, Exception):
                    check_results[i] = HealthCheckResult(
                        component=self.health_checkers[i].name,
                        status=HealthStatus.CRITICAL,
                        message=f"Health check crashed: {str(result)}",
                        timestamp=datetime.now(),
                        response_time_ms=0,
                        details={"error": str(result), "error_type": type(result).__name__}
                    )
        
        # Determine overall system health
        overall_status = self._determine_overall_status(check_results)
        
        # Calculate uptime
        uptime_seconds = int((datetime.now() - self.start_time).total_seconds())
        
        system_health = SystemHealth(
            overall_status=overall_status,
            timestamp=datetime.now(),
            checks=check_results,
            uptime_seconds=uptime_seconds
        )
        
        # Store in history
        self.health_history.append(system_health)
        if len(self.health_history) > self.max_history_size:
            self.health_history.pop(0)
        
        self.last_health_check = system_health
        
        # Log health status
        self._log_health_status(system_health)
        
        return system_health
    
    def _determine_overall_status(self, check_results: List[HealthCheckResult]) -> HealthStatus:
        """Determine overall system health from individual checks"""
        if not check_results:
            return HealthStatus.HEALTHY
        
        # Count status types
        status_counts = {status: 0 for status in HealthStatus}
        for result in check_results:
            status_counts[result.status] += 1
        
        # Determine overall status based on worst case
        if status_counts[HealthStatus.CRITICAL] > 0:
            return HealthStatus.CRITICAL
        elif status_counts[HealthStatus.UNHEALTHY] > 0:
            return HealthStatus.UNHEALTHY
        elif status_counts[HealthStatus.WARNING] > 0:
            return HealthStatus.WARNING
        else:
            return HealthStatus.HEALTHY
    
    def _log_health_status(self, system_health: SystemHealth):
        """Log health status with appropriate level"""
        status_emoji = {
            HealthStatus.HEALTHY: "✅",
            HealthStatus.WARNING: "⚠️",
            HealthStatus.UNHEALTHY: "❌",
            HealthStatus.CRITICAL: "🚨"
        }
        
        emoji = status_emoji.get(system_health.overall_status, "❓")
        
        if system_health.overall_status == HealthStatus.CRITICAL:
            logger.critical(f"{emoji} System Health: {system_health.overall_status.value.upper()}")
        elif system_health.overall_status == HealthStatus.UNHEALTHY:
            logger.error(f"{emoji} System Health: {system_health.overall_status.value.upper()}")
        elif system_health.overall_status == HealthStatus.WARNING:
            logger.warning(f"{emoji} System Health: {system_health.overall_status.value.upper()}")
        else:
            logger.info(f"{emoji} System Health: {system_health.overall_status.value.upper()}")
        
        # Log individual check issues
        for check in system_health.checks:
            if check.status != HealthStatus.HEALTHY:
                logger.warning(f"  - {check.component}: {check.status.value} - {check.message}")
    
    async def start_monitoring(self):
        """Start continuous health monitoring"""
        if self.is_monitoring:
            logger.warning("Health monitoring is already running")
            return
        
        self.is_monitoring = True
        self.monitoring_task = asyncio.create_task(self._monitoring_loop())
        logger.info(f"Started health monitoring with {self.check_interval_seconds}s interval")
    
    async def stop_monitoring(self):
        """Stop continuous health monitoring"""
        if not self.is_monitoring:
            return
        
        self.is_monitoring = False
        if self.monitoring_task:
            self.monitoring_task.cancel()
            try:
                await self.monitoring_task
            except asyncio.CancelledError:
                pass
        
        logger.info("Stopped health monitoring")
    
    async def _monitoring_loop(self):
        """Continuous monitoring loop"""
        while self.is_monitoring:
            try:
                await self.run_health_checks()
                await asyncio.sleep(self.check_interval_seconds)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health monitoring loop: {e}")
                await asyncio.sleep(self.check_interval_seconds)
    
    def get_current_health(self) -> Optional[Dict[str, Any]]:
        """Get current health status"""
        if self.last_health_check:
            return self.last_health_check.to_dict()
        return None
    
    def get_health_trends(self, hours: int = 24) -> Dict[str, Any]:
        """Get health trends over specified time period"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_checks = [
            health for health in self.health_history
            if health.timestamp >= cutoff_time
        ]
        
        if not recent_checks:
            return {"message": "No health data available for the specified period"}
        
        # Calculate trends
        status_counts = {status.value: 0 for status in HealthStatus}
        for health in recent_checks:
            status_counts[health.overall_status.value] += 1
        
        # Calculate availability percentage
        healthy_count = status_counts[HealthStatus.HEALTHY.value]
        total_checks = len(recent_checks)
        availability_percent = (healthy_count / total_checks * 100) if total_checks > 0 else 0
        
        return {
            "period_hours": hours,
            "total_checks": total_checks,
            "availability_percent": round(availability_percent, 2),
            "status_distribution": status_counts,
            "latest_status": recent_checks[-1].overall_status.value if recent_checks else None,
            "trend": "improving" if len(recent_checks) >= 2 and 
                    recent_checks[-1].overall_status.value < recent_checks[-2].overall_status.value 
                    else "stable"
        }


# Global health monitor instance
global_health_monitor = HealthMonitor()


# Example usage and testing
if __name__ == "__main__":
    import asyncio
    
    async def test_health_monitoring():
        monitor = HealthMonitor(check_interval_seconds=10)
        
        # Add a simple test health checker
        async def test_check():
            return {"status": "ok", "timestamp": datetime.now().isoformat()}
        
        monitor.add_health_checker(HealthChecker("test_service", test_check))
        
        # Run single health check
        health = await monitor.run_health_checks()
        print("Health Check Result:")
        print(health.to_dict())
        
        # Start monitoring for a short period
        await monitor.start_monitoring()
        await asyncio.sleep(30)  # Monitor for 30 seconds
        await monitor.stop_monitoring()
    
    # Run test
    asyncio.run(test_health_monitoring())