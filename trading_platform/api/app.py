"""
FastAPI Application Factory

Creates and configures the FastAPI application for the trading platform.
Integrates with existing aop.py orchestrator and data engines.
"""

import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from ..core import TradingEngine
from .routers import strategies, orders, positions, market_data, eod_data
from .dependencies import get_trading_engine

logger = logging.getLogger(__name__)


# Global trading engine instance
trading_engine: TradingEngine = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    global trading_engine
    
    # Startup
    logger.info("Starting trading platform API...")
    
    # Initialize trading engine
    config = {
        'simulation_mode': True,  # Start in simulation mode
        'broker': 'alpaca',
        'redis_url': 'redis://localhost:6379',
        'database_url': 'postgresql://localhost/trading'
    }
    
    trading_engine = TradingEngine(config=config)
    
    try:
        await trading_engine.start()
        logger.info("Trading engine started successfully")
    except Exception as e:
        logger.error(f"Failed to start trading engine: {e}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down trading platform API...")
    
    if trading_engine:
        await trading_engine.stop()
        logger.info("Trading engine stopped")


def create_app() -> FastAPI:
    """Create and configure FastAPI application"""
    
    app = FastAPI(
        title="Bespin Trading Platform API",
        description="A comprehensive quantitative trading platform with strategy execution, order management, and real-time market data processing.",
        version="1.0.0",
        lifespan=lifespan
    )
    
    # Add CORS middleware
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    
    # Include routers
    app.include_router(
        strategies.router,
        prefix="/api/v1/strategies",
        tags=["strategies"]
    )
    
    app.include_router(
        orders.router,
        prefix="/api/v1/orders",
        tags=["orders"]
    )
    
    app.include_router(
        positions.router,
        prefix="/api/v1/positions",
        tags=["positions"]
    )
    
    app.include_router(
        market_data.router,
        prefix="/api/v1/market-data",
        tags=["market-data"]
    )
    
    app.include_router(
        eod_data.router,
        prefix="/api/v1/eod",
        tags=["eod-data"]
    )
    
    # Health check endpoint
    @app.get("/health")
    async def health_check():
        """Health check endpoint"""
        global trading_engine
        
        if not trading_engine:
            return JSONResponse(
                status_code=503,
                content={"status": "unhealthy", "message": "Trading engine not initialized"}
            )
        
        engine_status = trading_engine.get_status()
        
        return {
            "status": "healthy" if engine_status['is_running'] else "degraded",
            "trading_engine": engine_status,
            "timestamp": "2025-07-13T06:03:10Z"
        }
    
    # Root endpoint
    @app.get("/")
    async def root():
        """Root endpoint with API information"""
        return {
            "name": "Bespin Trading Platform API",
            "version": "1.0.0",
            "description": "Quantitative trading platform with strategy execution and order management",
            "endpoints": {
                "health": "/health",
                "strategies": "/api/v1/strategies",
                "orders": "/api/v1/orders", 
                "positions": "/api/v1/positions",
                "market_data": "/api/v1/market-data",
                "eod_data": "/api/v1/eod",
                "docs": "/docs",
                "redoc": "/redoc"
            },
            "integration": {
                "data_orchestrator": "aop.py",
                "broker": "Alpaca",
                "database": "PostgreSQL + TimescaleDB",
                "cache": "Redis"
            }
        }
    
    # Global exception handler
    @app.exception_handler(Exception)
    async def global_exception_handler(request, exc):
        logger.error(f"Global exception: {exc}")
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )
    
    return app


def get_app() -> FastAPI:
    """Get the configured FastAPI application"""
    return create_app()