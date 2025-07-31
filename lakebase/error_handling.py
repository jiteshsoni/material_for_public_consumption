#!/usr/bin/env python3
"""
Enhanced Error Handling and Categorization
Based on FastAPI best practices for comprehensive error management.

Features:
- Structured error categorization
- Proper exception hierarchy
- Database-specific error handling
- Performance monitoring integration
- Detailed logging with context
"""

import logging
import time
import traceback
from enum import Enum
from typing import Dict, Optional, Any, List
from dataclasses import dataclass
from datetime import datetime
import psycopg2
from databricks.sdk.errors import DatabricksError

logger = logging.getLogger(__name__)


class ErrorCategory(Enum):
    """Error categories for structured error handling"""
    DATABASE_CONNECTION = "database_connection"
    DATABASE_QUERY = "database_query" 
    DATABASE_TIMEOUT = "database_timeout"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    CONFIGURATION = "configuration"
    NETWORK = "network"
    RATE_LIMITING = "rate_limiting"
    SYSTEM = "system"
    VALIDATION = "validation"
    UNKNOWN = "unknown"


class ErrorSeverity(Enum):
    """Error severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class ErrorContext:
    """Structured error context information"""
    timestamp: datetime
    category: ErrorCategory
    severity: ErrorSeverity
    error_code: Optional[str] = None
    message: str = ""
    details: Dict[str, Any] = None
    operation: Optional[str] = None
    duration_ms: Optional[float] = None
    retry_count: int = 0
    recoverable: bool = True
    
    def __post_init__(self):
        if self.details is None:
            self.details = {}


class LakebaseException(Exception):
    """Base exception class for Lakebase operations"""
    
    def __init__(
        self, 
        message: str, 
        category: ErrorCategory = ErrorCategory.UNKNOWN,
        severity: ErrorSeverity = ErrorSeverity.MEDIUM,
        error_code: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        recoverable: bool = True
    ):
        super().__init__(message)
        self.context = ErrorContext(
            timestamp=datetime.now(),
            category=category,
            severity=severity,
            error_code=error_code,
            message=message,
            details=details or {},
            recoverable=recoverable
        )


class DatabaseConnectionError(LakebaseException):
    """Database connection related errors"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.DATABASE_CONNECTION,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )


class DatabaseQueryError(LakebaseException):
    """Database query execution errors"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.DATABASE_QUERY,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )


class DatabaseTimeoutError(LakebaseException):
    """Database timeout errors"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.DATABASE_TIMEOUT,
            severity=ErrorSeverity.HIGH,
            **kwargs
        )


class AuthenticationError(LakebaseException):
    """Authentication related errors"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.AUTHENTICATION,
            severity=ErrorSeverity.CRITICAL,
            recoverable=False,
            **kwargs
        )


class RateLimitError(LakebaseException):
    """Rate limiting errors"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.RATE_LIMITING,
            severity=ErrorSeverity.MEDIUM,
            **kwargs
        )


class ConfigurationError(LakebaseException):
    """Configuration related errors"""
    
    def __init__(self, message: str, **kwargs):
        super().__init__(
            message,
            category=ErrorCategory.CONFIGURATION,
            severity=ErrorSeverity.CRITICAL,
            recoverable=False,
            **kwargs
        )


class ErrorHandler:
    """Centralized error handling with monitoring and recovery"""
    
    def __init__(self, enable_metrics: bool = True):
        self.enable_metrics = enable_metrics
        self.error_counts: Dict[ErrorCategory, int] = {}
        self.error_history: List[ErrorContext] = []
        self.max_history_size = 1000
    
    def handle_exception(
        self, 
        exception: Exception, 
        operation: Optional[str] = None,
        duration_ms: Optional[float] = None,
        retry_count: int = 0
    ) -> ErrorContext:
        """
        Handle and categorize exceptions with structured logging
        """
        error_context = self._categorize_exception(
            exception, operation, duration_ms, retry_count
        )
        
        # Log error with appropriate level
        self._log_error(error_context)
        
        # Update metrics
        if self.enable_metrics:
            self._update_metrics(error_context)
        
        # Store in history
        self._store_error_history(error_context)
        
        return error_context
    
    def _categorize_exception(
        self, 
        exception: Exception, 
        operation: Optional[str] = None,
        duration_ms: Optional[float] = None,
        retry_count: int = 0
    ) -> ErrorContext:
        """Categorize exception and create error context"""
        
        # Handle our custom exceptions first
        if isinstance(exception, LakebaseException):
            exception.context.operation = operation
            exception.context.duration_ms = duration_ms
            exception.context.retry_count = retry_count
            return exception.context
        
        # Handle psycopg2 database errors
        if isinstance(exception, psycopg2.Error):
            return self._handle_psycopg2_error(exception, operation, duration_ms, retry_count)
        
        # Handle Databricks SDK errors
        if isinstance(exception, DatabricksError):
            return self._handle_databricks_error(exception, operation, duration_ms, retry_count)
        
        # Handle standard Python exceptions
        return self._handle_standard_error(exception, operation, duration_ms, retry_count)
    
    def _handle_psycopg2_error(
        self, 
        error: psycopg2.Error, 
        operation: Optional[str] = None,
        duration_ms: Optional[float] = None,
        retry_count: int = 0
    ) -> ErrorContext:
        """Handle PostgreSQL specific errors"""
        
        error_code = getattr(error, 'pgcode', None)
        message = str(error).strip()
        
        # Connection errors
        if isinstance(error, (psycopg2.OperationalError, psycopg2.InterfaceError)):
            if "timeout" in message.lower():
                category = ErrorCategory.DATABASE_TIMEOUT
                severity = ErrorSeverity.HIGH
            else:
                category = ErrorCategory.DATABASE_CONNECTION
                severity = ErrorSeverity.HIGH
        
        # Authentication/Authorization errors
        elif isinstance(error, psycopg2.errors.InvalidPassword):
            category = ErrorCategory.AUTHENTICATION
            severity = ErrorSeverity.CRITICAL
        
        elif isinstance(error, psycopg2.errors.InsufficientPrivilege):
            category = ErrorCategory.AUTHORIZATION
            severity = ErrorSeverity.HIGH
        
        # Query errors
        elif isinstance(error, (psycopg2.ProgrammingError, psycopg2.DataError)):
            category = ErrorCategory.DATABASE_QUERY
            severity = ErrorSeverity.MEDIUM
        
        else:
            category = ErrorCategory.DATABASE_CONNECTION
            severity = ErrorSeverity.MEDIUM
        
        return ErrorContext(
            timestamp=datetime.now(),
            category=category,
            severity=severity,
            error_code=error_code,
            message=message,
            details={"original_error": type(error).__name__},
            operation=operation,
            duration_ms=duration_ms,
            retry_count=retry_count,
            recoverable=category != ErrorCategory.AUTHENTICATION
        )
    
    def _handle_databricks_error(
        self, 
        error: DatabricksError, 
        operation: Optional[str] = None,
        duration_ms: Optional[float] = None,
        retry_count: int = 0
    ) -> ErrorContext:
        """Handle Databricks SDK specific errors"""
        
        message = str(error)
        error_code = getattr(error, 'error_code', None)
        
        # Rate limiting
        if "rate limit" in message.lower() or "429" in message:
            category = ErrorCategory.RATE_LIMITING
            severity = ErrorSeverity.MEDIUM
        
        # Authentication
        elif "401" in message or "unauthorized" in message.lower():
            category = ErrorCategory.AUTHENTICATION
            severity = ErrorSeverity.CRITICAL
        
        # Authorization
        elif "403" in message or "forbidden" in message.lower():
            category = ErrorCategory.AUTHORIZATION
            severity = ErrorSeverity.HIGH
        
        # Network issues
        elif "network" in message.lower() or "timeout" in message.lower():
            category = ErrorCategory.NETWORK
            severity = ErrorSeverity.HIGH
        
        else:
            category = ErrorCategory.SYSTEM
            severity = ErrorSeverity.MEDIUM
        
        return ErrorContext(
            timestamp=datetime.now(),
            category=category,
            severity=severity,
            error_code=error_code,
            message=message,
            details={"original_error": type(error).__name__},
            operation=operation,
            duration_ms=duration_ms,
            retry_count=retry_count,
            recoverable=category not in [ErrorCategory.AUTHENTICATION, ErrorCategory.AUTHORIZATION]
        )
    
    def _handle_standard_error(
        self, 
        error: Exception, 
        operation: Optional[str] = None,
        duration_ms: Optional[float] = None,
        retry_count: int = 0
    ) -> ErrorContext:
        """Handle standard Python exceptions"""
        
        message = str(error)
        
        # Timeout errors
        if isinstance(error, TimeoutError) or "timeout" in message.lower():
            category = ErrorCategory.DATABASE_TIMEOUT
            severity = ErrorSeverity.HIGH
        
        # Connection errors
        elif isinstance(error, ConnectionError):
            category = ErrorCategory.NETWORK
            severity = ErrorSeverity.HIGH
        
        # Value/Type errors usually indicate configuration issues
        elif isinstance(error, (ValueError, TypeError)):
            category = ErrorCategory.VALIDATION
            severity = ErrorSeverity.MEDIUM
        
        else:
            category = ErrorCategory.UNKNOWN
            severity = ErrorSeverity.MEDIUM
        
        return ErrorContext(
            timestamp=datetime.now(),
            category=category,
            severity=severity,
            error_code=None,
            message=message,
            details={
                "original_error": type(error).__name__,
                "traceback": traceback.format_exc()
            },
            operation=operation,
            duration_ms=duration_ms,
            retry_count=retry_count,
            recoverable=True
        )
    
    def _log_error(self, error_context: ErrorContext):
        """Log error with appropriate level and structured information"""
        
        log_data = {
            "category": error_context.category.value,
            "severity": error_context.severity.value,
            "operation": error_context.operation,
            "error_code": error_context.error_code,
            "duration_ms": error_context.duration_ms,
            "retry_count": error_context.retry_count,
            "recoverable": error_context.recoverable
        }
        
        if error_context.severity == ErrorSeverity.CRITICAL:
            logger.critical(f"{error_context.message} | {log_data}")
        elif error_context.severity == ErrorSeverity.HIGH:
            logger.error(f"{error_context.message} | {log_data}")
        elif error_context.severity == ErrorSeverity.MEDIUM:
            logger.warning(f"{error_context.message} | {log_data}")
        else:
            logger.info(f"{error_context.message} | {log_data}")
    
    def _update_metrics(self, error_context: ErrorContext):
        """Update error metrics"""
        category = error_context.category
        if category not in self.error_counts:
            self.error_counts[category] = 0
        self.error_counts[category] += 1
    
    def _store_error_history(self, error_context: ErrorContext):
        """Store error in history with size limit"""
        self.error_history.append(error_context)
        if len(self.error_history) > self.max_history_size:
            self.error_history.pop(0)
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary for monitoring"""
        return {
            "total_errors": len(self.error_history),
            "error_counts_by_category": {
                category.value: count 
                for category, count in self.error_counts.items()
            },
            "recent_errors": [
                {
                    "timestamp": ctx.timestamp.isoformat(),
                    "category": ctx.category.value,
                    "severity": ctx.severity.value,
                    "message": ctx.message,
                    "operation": ctx.operation
                }
                for ctx in self.error_history[-10:]  # Last 10 errors
            ]
        }
    
    def should_retry(self, error_context: ErrorContext, max_retries: int = 3) -> bool:
        """Determine if operation should be retried"""
        if not error_context.recoverable:
            return False
        
        if error_context.retry_count >= max_retries:
            return False
        
        # Don't retry authentication errors
        if error_context.category == ErrorCategory.AUTHENTICATION:
            return False
        
        # Retry rate limiting with backoff
        if error_context.category == ErrorCategory.RATE_LIMITING:
            return True
        
        # Retry network and timeout errors
        if error_context.category in [ErrorCategory.NETWORK, ErrorCategory.DATABASE_TIMEOUT]:
            return True
        
        # Retry connection errors
        if error_context.category == ErrorCategory.DATABASE_CONNECTION:
            return True
        
        return False
    
    def get_retry_delay(self, error_context: ErrorContext) -> float:
        """Calculate retry delay based on error type and retry count"""
        base_delay = 1.0
        
        # Exponential backoff
        delay = base_delay * (2 ** error_context.retry_count)
        
        # Rate limiting gets longer delays
        if error_context.category == ErrorCategory.RATE_LIMITING:
            delay *= 5
        
        # Add jitter
        import random
        delay += random.uniform(0, delay * 0.1)
        
        return min(delay, 60.0)  # Cap at 60 seconds


# Global error handler instance
global_error_handler = ErrorHandler()

def handle_error(
    exception: Exception, 
    operation: Optional[str] = None,
    duration_ms: Optional[float] = None,
    retry_count: int = 0
) -> ErrorContext:
    """Convenience function for global error handling"""
    return global_error_handler.handle_exception(
        exception, operation, duration_ms, retry_count
    )


# Decorator for automatic error handling
def with_error_handling(operation_name: str):
    """Decorator to automatically handle errors with timing"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            start_time = time.time()
            try:
                return func(*args, **kwargs)
            except Exception as e:
                duration_ms = (time.time() - start_time) * 1000
                error_context = handle_error(e, operation_name, duration_ms)
                raise LakebaseException(
                    error_context.message,
                    category=error_context.category,
                    severity=error_context.severity,
                    error_code=error_context.error_code,
                    details=error_context.details
                )
        return wrapper
    return decorator