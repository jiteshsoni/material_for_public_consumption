#!/usr/bin/env python3
"""
Performance Monitoring and Metrics Collection
Based on FastAPI best practices for comprehensive performance tracking.

Features:
- Request timing and latency tracking
- Database performance metrics
- Connection pool monitoring
- Health check integration
- Real-time performance reporting
"""

import time
import logging
import threading
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from collections import defaultdict, deque
import statistics
import json

logger = logging.getLogger(__name__)


@dataclass
class PerformanceMetric:
    """Individual performance metric data point"""
    timestamp: datetime
    operation: str
    duration_ms: float
    success: bool
    error_category: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class OperationStats:
    """Aggregated statistics for an operation"""
    operation: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    total_duration_ms: float = 0.0
    min_duration_ms: float = float('inf')
    max_duration_ms: float = 0.0
    durations: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    @property
    def success_rate(self) -> float:
        """Calculate success rate percentage"""
        if self.total_requests == 0:
            return 0.0
        return (self.successful_requests / self.total_requests) * 100
    
    @property
    def average_duration_ms(self) -> float:
        """Calculate average duration"""
        if self.total_requests == 0:
            return 0.0
        return self.total_duration_ms / self.total_requests
    
    @property
    def percentiles(self) -> Dict[str, float]:
        """Calculate performance percentiles"""
        if not self.durations:
            return {"p50": 0.0, "p95": 0.0, "p99": 0.0}
        
        sorted_durations = sorted(self.durations)
        n = len(sorted_durations)
        
        return {
            "p50": sorted_durations[int(n * 0.5)] if n > 0 else 0.0,
            "p95": sorted_durations[int(n * 0.95)] if n > 0 else 0.0,
            "p99": sorted_durations[int(n * 0.99)] if n > 0 else 0.0
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        percentiles = self.percentiles
        return {
            "operation": self.operation,
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": round(self.success_rate, 2),
            "average_duration_ms": round(self.average_duration_ms, 2),
            "min_duration_ms": round(self.min_duration_ms, 2),
            "max_duration_ms": round(self.max_duration_ms, 2),
            "p50_duration_ms": round(percentiles["p50"], 2),
            "p95_duration_ms": round(percentiles["p95"], 2),
            "p99_duration_ms": round(percentiles["p99"], 2)
        }


@dataclass
class ConnectionPoolMetrics:
    """Connection pool performance metrics"""
    pool_size: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    overflow_connections: int = 0
    total_checkouts: int = 0
    total_checkins: int = 0
    checkout_timeouts: int = 0
    connection_errors: int = 0
    average_checkout_time_ms: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "pool_size": self.pool_size,
            "active_connections": self.active_connections,
            "idle_connections": self.idle_connections,
            "overflow_connections": self.overflow_connections,
            "total_checkouts": self.total_checkouts,
            "total_checkins": self.total_checkins,
            "checkout_timeouts": self.checkout_timeouts,
            "connection_errors": self.connection_errors,
            "average_checkout_time_ms": round(self.average_checkout_time_ms, 2),
            "utilization_rate": round((self.active_connections / max(self.pool_size, 1)) * 100, 2)
        }


class PerformanceMonitor:
    """Comprehensive performance monitoring system"""
    
    def __init__(self, enable_detailed_metrics: bool = True, max_history_size: int = 10000):
        self.enable_detailed_metrics = enable_detailed_metrics
        self.max_history_size = max_history_size
        self.metrics_history: deque = deque(maxlen=max_history_size)
        self.operation_stats: Dict[str, OperationStats] = {}
        self.connection_pool_metrics = ConnectionPoolMetrics()
        self.start_time = datetime.now()
        self.lock = threading.RLock()
        
        # Performance thresholds for alerting
        self.thresholds = {
            "slow_query_ms": 5000,
            "error_rate_percent": 5.0,
            "checkout_timeout_ms": 30000
        }
    
    def record_operation(
        self, 
        operation: str, 
        duration_ms: float, 
        success: bool = True,
        error_category: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """Record a performance metric for an operation"""
        
        metric = PerformanceMetric(
            timestamp=datetime.now(),
            operation=operation,
            duration_ms=duration_ms,
            success=success,
            error_category=error_category,
            metadata=metadata or {}
        )
        
        with self.lock:
            # Store detailed metric if enabled
            if self.enable_detailed_metrics:
                self.metrics_history.append(metric)
            
            # Update operation statistics
            if operation not in self.operation_stats:
                self.operation_stats[operation] = OperationStats(operation=operation)
            stats = self.operation_stats[operation]
            stats.total_requests += 1
            stats.total_duration_ms += duration_ms
            stats.durations.append(duration_ms)
            
            if success:
                stats.successful_requests += 1
            else:
                stats.failed_requests += 1
            
            # Update min/max
            stats.min_duration_ms = min(stats.min_duration_ms, duration_ms)
            stats.max_duration_ms = max(stats.max_duration_ms, duration_ms)
            
            # Check for performance alerts
            self._check_performance_alerts(metric, stats)
    
    def _check_performance_alerts(self, metric: PerformanceMetric, stats: OperationStats):
        """Check for performance issues and log alerts"""
        
        # Slow query alert
        if metric.duration_ms > self.thresholds["slow_query_ms"]:
            logger.warning(
                f"Slow operation detected: {metric.operation} took {metric.duration_ms:.1f}ms "
                f"(threshold: {self.thresholds['slow_query_ms']}ms)"
            )
        
        # High error rate alert
        if stats.total_requests >= 10 and stats.success_rate < (100 - self.thresholds["error_rate_percent"]):
            logger.warning(
                f"High error rate for {metric.operation}: {stats.success_rate:.1f}% success rate "
                f"({stats.failed_requests}/{stats.total_requests} failed)"
            )
    
    def update_connection_pool_metrics(self, metrics: ConnectionPoolMetrics):
        """Update connection pool metrics"""
        with self.lock:
            self.connection_pool_metrics = metrics
    
    def get_operation_stats(self, operation: Optional[str] = None) -> Dict[str, Any]:
        """Get statistics for specific operation or all operations"""
        with self.lock:
            if operation:
                stats = self.operation_stats.get(operation)
                return stats.to_dict() if stats else {}
            
            return {
                op: stats.to_dict() 
                for op, stats in self.operation_stats.items()
            }
    
    def get_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive performance summary"""
        with self.lock:
            now = datetime.now()
            uptime = now - self.start_time
            
            # Overall statistics
            total_requests = sum(stats.total_requests for stats in self.operation_stats.values())
            total_errors = sum(stats.failed_requests for stats in self.operation_stats.values())
            
            # Recent performance (last 5 minutes)
            recent_cutoff = now - timedelta(minutes=5)
            recent_metrics = [
                m for m in self.metrics_history 
                if m.timestamp >= recent_cutoff
            ]
            
            recent_success_rate = 0.0
            recent_avg_duration = 0.0
            if recent_metrics:
                recent_successes = sum(1 for m in recent_metrics if m.success)
                recent_success_rate = (recent_successes / len(recent_metrics)) * 100
                recent_avg_duration = statistics.mean(m.duration_ms for m in recent_metrics)
            
            return {
                "uptime_seconds": int(uptime.total_seconds()),
                "total_requests": total_requests,
                "total_errors": total_errors,
                "overall_success_rate": round(
                    ((total_requests - total_errors) / max(total_requests, 1)) * 100, 2
                ),
                "recent_5min": {
                    "requests": len(recent_metrics),
                    "success_rate": round(recent_success_rate, 2),
                    "average_duration_ms": round(recent_avg_duration, 2)
                },
                "operation_stats": self.get_operation_stats(),
                "connection_pool": self.connection_pool_metrics.to_dict(),
                "performance_alerts": self._get_active_alerts()
            }
    
    def _get_active_alerts(self) -> List[Dict[str, Any]]:
        """Get list of active performance alerts"""
        alerts = []
        
        for operation, stats in self.operation_stats.items():
            # High error rate alerts
            if stats.total_requests >= 10 and stats.success_rate < 95.0:
                alerts.append({
                    "type": "high_error_rate",
                    "operation": operation,
                    "success_rate": stats.success_rate,
                    "severity": "high" if stats.success_rate < 90.0 else "medium"
                })
            
            # Slow operation alerts
            avg_duration = stats.average_duration_ms
            if avg_duration > self.thresholds["slow_query_ms"]:
                alerts.append({
                    "type": "slow_operation",
                    "operation": operation,
                    "average_duration_ms": avg_duration,
                    "severity": "high" if avg_duration > 10000 else "medium"
                })
        
        # Connection pool alerts
        pool_metrics = self.connection_pool_metrics
        if pool_metrics.pool_size > 0:
            utilization = (pool_metrics.active_connections / pool_metrics.pool_size) * 100
            if utilization > 90:
                alerts.append({
                    "type": "high_pool_utilization",
                    "utilization_rate": utilization,
                    "severity": "high" if utilization > 95 else "medium"
                })
            
            if pool_metrics.checkout_timeouts > 0:
                alerts.append({
                    "type": "connection_checkout_timeouts",
                    "timeout_count": pool_metrics.checkout_timeouts,
                    "severity": "high"
                })
        
        return alerts
    
    def export_metrics(self, format: str = "json") -> str:
        """Export metrics in specified format"""
        data = self.get_performance_summary()
        
        if format.lower() == "json":
            return json.dumps(data, indent=2, default=str)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def reset_metrics(self):
        """Reset all metrics (useful for testing)"""
        with self.lock:
            self.metrics_history.clear()
            self.operation_stats.clear()
            self.connection_pool_metrics = ConnectionPoolMetrics()
            self.start_time = datetime.now()
            logger.info("Performance metrics reset")


# Global performance monitor instance
global_performance_monitor = PerformanceMonitor()


class PerformanceTimer:
    """Context manager for timing operations"""
    
    def __init__(
        self, 
        operation: str, 
        monitor: Optional[PerformanceMonitor] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        self.operation = operation
        self.monitor = monitor or global_performance_monitor
        self.metadata = metadata or {}
        self.start_time = None
        self.end_time = None
        self.success = True
        self.error_category = None
    
    def __enter__(self):
        self.start_time = time.time()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        duration_ms = (self.end_time - self.start_time) * 1000
        
        # Determine success based on exception
        if exc_type is not None:
            self.success = False
            self.error_category = exc_type.__name__
        
        # Record the metric
        self.monitor.record_operation(
            operation=self.operation,
            duration_ms=duration_ms,
            success=self.success,
            error_category=self.error_category,
            metadata=self.metadata
        )
        
        # Log performance info
        status = "SUCCESS" if self.success else f"ERROR({self.error_category})"
        logger.info(f"Operation: {self.operation} - {duration_ms:.1f}ms - {status}")
        
        return False  # Don't suppress exceptions


def timed_operation(operation_name: str, metadata: Optional[Dict[str, Any]] = None):
    """Decorator for automatic operation timing"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            with PerformanceTimer(operation_name, metadata=metadata):
                return func(*args, **kwargs)
        return wrapper
    return decorator


def log_performance_summary(monitor: Optional[PerformanceMonitor] = None):
    """Log current performance summary"""
    monitor = monitor or global_performance_monitor
    summary = monitor.get_performance_summary()
    
    logger.info("=== PERFORMANCE SUMMARY ===")
    logger.info(f"Uptime: {summary['uptime_seconds']}s")
    logger.info(f"Total Requests: {summary['total_requests']}")
    logger.info(f"Overall Success Rate: {summary['overall_success_rate']}%")
    logger.info(f"Recent 5min: {summary['recent_5min']['requests']} requests, "
               f"{summary['recent_5min']['success_rate']}% success, "
               f"{summary['recent_5min']['average_duration_ms']}ms avg")
    
    # Log top operations
    operations = summary['operation_stats']
    if operations:
        logger.info("--- Top Operations ---")
        for op_name, stats in sorted(operations.items(), 
                                   key=lambda x: x[1]['total_requests'], 
                                   reverse=True)[:5]:
            logger.info(f"{op_name}: {stats['total_requests']} requests, "
                       f"{stats['success_rate']}% success, "
                       f"{stats['average_duration_ms']}ms avg")
    
    # Log alerts
    alerts = summary['performance_alerts']
    if alerts:
        logger.warning(f"Active Performance Alerts: {len(alerts)}")
        for alert in alerts:
            logger.warning(f"  - {alert['type']}: {alert}")


# Example usage
if __name__ == "__main__":
    # Test performance monitoring
    monitor = PerformanceMonitor()
    
    # Simulate some operations
    with PerformanceTimer("database_query", monitor):
        time.sleep(0.1)  # Simulate 100ms query
    
    with PerformanceTimer("database_connection", monitor):
        time.sleep(0.05)  # Simulate 50ms connection
    
    # Print summary
    print(json.dumps(monitor.get_performance_summary(), indent=2, default=str))