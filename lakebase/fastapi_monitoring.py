#!/usr/bin/env python3
"""
FastAPI Monitoring Layer for Lakebase Benchmarks
Based on the doyledev-lakebase-fastapi-app best practices.

Features:
- Real-time monitoring dashboard
- Benchmark execution API
- Health check endpoints
- Performance metrics API
- WebSocket for live updates
"""

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Dict, List, Optional, Any

from fastapi import FastAPI, HTTPException, Depends, WebSocket, WebSocketDisconnect, BackgroundTasks
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

# Import our enhanced modules
from config import load_settings, LakebaseSettings
from enhanced_production_pool import EnhancedProductionConnectionPool, create_enhanced_pool
from error_handling import ErrorHandler, LakebaseException
from performance_monitoring import PerformanceMonitor
from health_monitoring import HealthMonitor, HealthStatus

logger = logging.getLogger(__name__)

# Global instances
pool: Optional[EnhancedProductionConnectionPool] = None
settings: Optional[LakebaseSettings] = None


# Pydantic models for API responses
class HealthResponse(BaseModel):
    status: str
    timestamp: str
    uptime_seconds: int
    checks: List[Dict[str, Any]]
    summary: Dict[str, int]


class PerformanceResponse(BaseModel):
    uptime_seconds: int
    total_requests: int
    total_errors: int
    overall_success_rate: float
    recent_5min: Dict[str, Any]
    operation_stats: Dict[str, Dict[str, Any]]
    connection_pool: Dict[str, Any]


class PoolStatusResponse(BaseModel):
    pool_size: int
    max_overflow: int
    available_connections: int
    active_connections: int
    utilization_rate: float
    total_checkouts: int
    total_checkins: int
    checkout_timeouts: int
    connection_errors: int


class BenchmarkConfig(BaseModel):
    thread_counts: List[int] = Field(default=[20, 40, 80])
    test_duration_seconds: int = Field(default=30, ge=1, le=300)
    table_rows: int = Field(default=100000, ge=1000)
    batch_size: int = Field(default=10000, ge=100)


class BenchmarkResult(BaseModel):
    timestamp: str
    config: BenchmarkConfig
    results: List[Dict[str, Any]]
    summary: Dict[str, Any]


class ConnectionManager:
    """WebSocket connection manager for live updates"""
    
    def __init__(self):
        self.active_connections: List[WebSocket] = []
    
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")
    
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")
    
    async def broadcast(self, message: Dict[str, Any]):
        """Broadcast message to all connected clients"""
        if not self.active_connections:
            return
        
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.warning(f"Failed to send WebSocket message: {e}")
                disconnected.append(connection)
        
        # Remove disconnected clients
        for connection in disconnected:
            self.disconnect(connection)


# Global WebSocket manager
manager = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle management"""
    global pool, settings
    
    logger.info("Starting Lakebase FastAPI monitoring service")
    
    try:
        # Load settings and create pool
        settings = load_settings()
        pool = create_enhanced_pool()
        pool.initialize_pool()
        
        # Start health monitoring
        if settings.monitoring.enable_health_checks:
            await pool.health_monitor.start_monitoring()
        
        # Start background task for WebSocket updates
        update_task = asyncio.create_task(background_websocket_updates())
        
        logger.info("FastAPI monitoring service started successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start monitoring service: {e}")
        raise
    finally:
        # Cleanup
        logger.info("Shutting down monitoring service")
        
        if pool:
            if hasattr(pool, 'health_monitor'):
                await pool.health_monitor.stop_monitoring()
            pool.close_all()
        
        # Cancel background tasks
        update_task.cancel()
        try:
            await update_task
        except asyncio.CancelledError:
            pass
        
        logger.info("Monitoring service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Lakebase Benchmark Monitoring API",
    description="Real-time monitoring and control for Lakebase benchmarks",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify actual origins
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Performance monitoring middleware
@app.middleware("http")
async def add_process_time_header(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = time.time() - start_time
    response.headers["X-Process-Time"] = str(process_time)
    
    # Record in performance monitor
    if pool and pool.performance_monitor:
        pool.performance_monitor.record_operation(
            operation=f"{request.method} {request.url.path}",
            duration_ms=process_time * 1000,
            success=response.status_code < 400
        )
    
    return response


# Exception handlers
@app.exception_handler(LakebaseException)
async def lakebase_exception_handler(request, exc: LakebaseException):
    return JSONResponse(
        status_code=500 if exc.context.category.value == "critical" else 400,
        content={
            "error": exc.context.message,
            "category": exc.context.category.value,
            "severity": exc.context.severity.value,
            "timestamp": exc.context.timestamp.isoformat()
        }
    )


@app.exception_handler(Exception)
async def general_exception_handler(request, exc: Exception):
    logger.error(f"Unhandled exception on {request.method} {request.url.path}: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error occurred"}
    )


# Health check endpoints
@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health_check():
    """Basic health check endpoint"""
    if not pool:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    # Get health status
    health_data = pool.get_health_status()
    if not health_data:
        # Fallback health check
        return HealthResponse(
            status="healthy",
            timestamp=datetime.now().isoformat(),
            uptime_seconds=0,
            checks=[],
            summary={"total_checks": 0, "healthy": 0, "warning": 0, "unhealthy": 0, "critical": 0}
        )
    
    return HealthResponse(**health_data)


@app.get("/health/detailed", tags=["health"])
async def detailed_health_check():
    """Detailed health check with all components"""
    if not pool:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    # Run fresh health checks
    health_result = await pool.health_monitor.run_health_checks()
    return health_result.to_dict()


# Performance monitoring endpoints
@app.get("/metrics/performance", response_model=PerformanceResponse, tags=["metrics"])
async def get_performance_metrics():
    """Get comprehensive performance metrics"""
    if not pool:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    metrics = pool.get_performance_metrics()
    return PerformanceResponse(**metrics)


@app.get("/metrics/pool", response_model=PoolStatusResponse, tags=["metrics"])
async def get_pool_status():
    """Get connection pool status"""
    if not pool:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    status = pool.get_pool_status()
    return PoolStatusResponse(**status)


@app.get("/metrics/errors", tags=["metrics"])
async def get_error_summary():
    """Get error summary and recent errors"""
    if not pool:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    return pool.error_handler.get_error_summary()


# Database testing endpoints
@app.post("/test/connection", tags=["testing"])
async def test_database_connection():
    """Test database connection"""
    if not pool:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    start_time = time.time()
    try:
        conn = pool.get_connection(timeout=5)
        with conn.cursor() as cursor:
            cursor.execute("SELECT 1 as test, NOW() as timestamp")
            result = cursor.fetchone()
        
        pool.return_connection(conn)
        duration_ms = (time.time() - start_time) * 1000
        
        return {
            "status": "success",
            "duration_ms": round(duration_ms, 2),
            "result": {"test": result[0], "timestamp": str(result[1])},
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "status": "failed",
                "error": str(e),
                "duration_ms": round(duration_ms, 2),
                "timestamp": datetime.now().isoformat()
            }
        )


@app.post("/test/query", tags=["testing"])
async def test_query_performance(query: str = "SELECT COUNT(*) FROM pg_tables"):
    """Test query performance"""
    if not pool:
        raise HTTPException(status_code=503, detail="Service not ready")
    
    # Validate query (basic safety check)
    if any(word in query.upper() for word in ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER']):
        raise HTTPException(status_code=400, detail="Only SELECT queries are allowed")
    
    start_time = time.time()
    try:
        conn = pool.get_connection(timeout=5)
        with conn.cursor() as cursor:
            cursor.execute(query)
            results = cursor.fetchall()
        
        pool.return_connection(conn)
        duration_ms = (time.time() - start_time) * 1000
        
        return {
            "status": "success",
            "duration_ms": round(duration_ms, 2),
            "row_count": len(results),
            "results": results[:10] if results else [],  # Limit to first 10 rows
            "query": query,
            "timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        duration_ms = (time.time() - start_time) * 1000
        raise HTTPException(
            status_code=500,
            detail={
                "status": "failed",
                "error": str(e),
                "duration_ms": round(duration_ms, 2),
                "query": query,
                "timestamp": datetime.now().isoformat()
            }
        )


# Configuration endpoints
@app.get("/config", tags=["configuration"])
async def get_configuration():
    """Get current configuration"""
    if not settings:
        raise HTTPException(status_code=503, detail="Configuration not available")
    
    # Return sanitized configuration (no secrets)
    config = {
        "lakebase": {
            "instance_name": settings.lakebase.instance_name,
            "host": settings.lakebase.host,
            "port": settings.lakebase.port,
            "database": settings.lakebase.database
        },
        "production_pool": {
            "pool_size": settings.production_pool.pool_size,
            "max_overflow": settings.production_pool.max_overflow,
            "pool_timeout": settings.production_pool.pool_timeout,
            "command_timeout": settings.production_pool.command_timeout,
            "pool_recycle_interval": settings.production_pool.pool_recycle_interval,
            "enable_background_refresh": settings.production_pool.enable_background_refresh,
            "health_check_interval": settings.production_pool.health_check_interval
        },
        "monitoring": {
            "log_level": settings.monitoring.log_level,
            "enable_performance_monitoring": settings.monitoring.enable_performance_monitoring,
            "enable_health_checks": settings.monitoring.enable_health_checks
        }
    }
    
    return config


# WebSocket endpoint for real-time updates
@app.websocket("/ws/live-updates")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for live monitoring updates"""
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and handle any incoming messages
            data = await websocket.receive_text()
            # Echo back for now (could implement commands)
            await websocket.send_json({"type": "ack", "message": "received", "data": data})
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        manager.disconnect(websocket)


# Simple monitoring dashboard
@app.get("/dashboard", response_class=HTMLResponse, tags=["dashboard"])
async def monitoring_dashboard():
    """Simple HTML dashboard for monitoring"""
    html_content = """
    <!DOCTYPE html>
    <html>
    <head>
        <title>Lakebase Monitoring Dashboard</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .card { border: 1px solid #ddd; padding: 15px; margin: 10px 0; border-radius: 5px; }
            .healthy { border-left: 5px solid #4CAF50; }
            .warning { border-left: 5px solid #FF9800; }
            .unhealthy { border-left: 5px solid #f44336; }
            .metric { display: inline-block; margin: 10px; padding: 10px; background: #f5f5f5; border-radius: 3px; }
            #status { font-size: 1.2em; font-weight: bold; }
        </style>
    </head>
    <body>
        <h1>🌊 Lakebase Monitoring Dashboard</h1>
        
        <div class="card">
            <h2>System Status</h2>
            <div id="status">Loading...</div>
            <div id="uptime"></div>
        </div>
        
        <div class="card">
            <h2>Connection Pool</h2>
            <div id="pool-metrics"></div>
        </div>
        
        <div class="card">
            <h2>Performance Metrics</h2>
            <div id="performance-metrics"></div>
        </div>
        
        <div class="card">
            <h2>Health Checks</h2>
            <div id="health-checks"></div>
        </div>

        <script>
            async function updateDashboard() {
                try {
                    // Get health status
                    const healthResponse = await fetch('/health');
                    const health = await healthResponse.json();
                    
                    document.getElementById('status').textContent = `System Status: ${health.status.toUpperCase()}`;
                    document.getElementById('status').className = health.status;
                    document.getElementById('uptime').textContent = `Uptime: ${Math.floor(health.uptime_seconds / 60)} minutes`;
                    
                    // Get pool status
                    const poolResponse = await fetch('/metrics/pool');
                    const pool = await poolResponse.json();
                    
                    document.getElementById('pool-metrics').innerHTML = `
                        <div class="metric">Pool Size: ${pool.pool_size}</div>
                        <div class="metric">Active: ${pool.active_connections}</div>
                        <div class="metric">Available: ${pool.available_connections}</div>
                        <div class="metric">Utilization: ${pool.utilization_rate}%</div>
                        <div class="metric">Total Checkouts: ${pool.total_checkouts}</div>
                        <div class="metric">Timeouts: ${pool.checkout_timeouts}</div>
                    `;
                    
                    // Get performance metrics
                    const perfResponse = await fetch('/metrics/performance');
                    const perf = await perfResponse.json();
                    
                    document.getElementById('performance-metrics').innerHTML = `
                        <div class="metric">Total Requests: ${perf.total_requests}</div>
                        <div class="metric">Success Rate: ${perf.overall_success_rate}%</div>
                        <div class="metric">Recent 5min: ${perf.recent_5min.requests} requests</div>
                        <div class="metric">Avg Duration: ${perf.recent_5min.average_duration_ms}ms</div>
                    `;
                    
                    // Display health checks
                    const healthChecks = health.checks.map(check => 
                        `<div class="card ${check.status}">
                            <strong>${check.component}</strong>: ${check.message} 
                            (${check.response_time_ms}ms)
                        </div>`
                    ).join('');
                    document.getElementById('health-checks').innerHTML = healthChecks;
                    
                } catch (error) {
                    console.error('Error updating dashboard:', error);
                    document.getElementById('status').textContent = 'Error loading data';
                    document.getElementById('status').className = 'unhealthy';
                }
            }
            
            // Update every 5 seconds
            updateDashboard();
            setInterval(updateDashboard, 5000);
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)


# Background task for WebSocket updates
async def background_websocket_updates():
    """Background task to send periodic updates via WebSocket"""
    while True:
        try:
            if manager.active_connections and pool:
                # Get current metrics
                health_data = pool.get_health_status()
                pool_status = pool.get_pool_status()
                performance_data = pool.get_performance_metrics()
                
                # Prepare update message
                update = {
                    "type": "metrics_update",
                    "timestamp": datetime.now().isoformat(),
                    "data": {
                        "health": health_data,
                        "pool": pool_status,
                        "performance": {
                            "total_requests": performance_data.get("total_requests", 0),
                            "success_rate": performance_data.get("overall_success_rate", 0),
                            "recent_5min": performance_data.get("recent_5min", {})
                        }
                    }
                }
                
                # Broadcast to connected clients
                await manager.broadcast(update)
            
            # Wait 10 seconds before next update
            await asyncio.sleep(10)
            
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"Error in WebSocket update task: {e}")
            await asyncio.sleep(10)


# Root endpoint
@app.get("/", tags=["root"])
async def root():
    """Root endpoint with API information"""
    return {
        "message": "Lakebase Benchmark Monitoring API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "detailed_health": "/health/detailed",
            "performance_metrics": "/metrics/performance",
            "pool_status": "/metrics/pool",
            "error_summary": "/metrics/errors",
            "test_connection": "/test/connection",
            "configuration": "/config",
            "dashboard": "/dashboard",
            "websocket": "/ws/live-updates",
            "docs": "/docs"
        },
        "timestamp": datetime.now().isoformat()
    }


# Development server runner
def run_dev_server(host: str = "0.0.0.0", port: int = 8000):
    """Run development server"""
    uvicorn.run(
        "fastapi_monitoring:app",
        host=host,
        port=port,
        reload=True,
        log_level="info"
    )


if __name__ == "__main__":
    # Run development server
    run_dev_server()