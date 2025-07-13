"""
Portfolio Management System - Main FastAPI Application
Comprehensive portfolio management platform with real-time tracking
"""

import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from .api.portfolio_api import router as portfolio_router
from .api.positions_api import router as positions_router  
from .api.performance_api import router as performance_router
from .services.portfolio_service import PortfolioService
from .services.real_time_tracker import RealTimeTracker
from .services.performance_engine import PerformanceEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Global instances
portfolio_service = None
real_time_tracker = None
performance_engine = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan management"""
    global portfolio_service, real_time_tracker, performance_engine
    
    # Startup
    logger.info("Starting Portfolio Management System...")
    
    try:
        # Initialize services
        db_connection = "postgresql://user:password@localhost:5432/trading_db"
        portfolio_service = PortfolioService(db_connection)
        performance_engine = PerformanceEngine(db_connection)
        real_time_tracker = RealTimeTracker(portfolio_service)
        
        # Start real-time tracking (if portfolios exist)
        # This would be configured based on active portfolios
        # await real_time_tracker.start_tracking([1, 2, 3])  # Example portfolio IDs
        
        logger.info("Portfolio Management System started successfully")
        
    except Exception as e:
        logger.error(f"Failed to start Portfolio Management System: {str(e)}")
        raise
    
    yield
    
    # Shutdown
    logger.info("Shutting down Portfolio Management System...")
    
    try:
        if real_time_tracker:
            await real_time_tracker.stop_tracking()
        
        logger.info("Portfolio Management System shut down successfully")
        
    except Exception as e:
        logger.error(f"Error during shutdown: {str(e)}")


# Create FastAPI application
app = FastAPI(
    title="Portfolio Management System",
    description="Comprehensive portfolio management with real-time tracking and analytics",
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
app.include_router(portfolio_router)
app.include_router(positions_router)
app.include_router(performance_router)


# Dependency injection
async def get_portfolio_service() -> PortfolioService:
    """Get portfolio service instance"""
    return portfolio_service


async def get_real_time_tracker() -> RealTimeTracker:
    """Get real-time tracker instance"""
    return real_time_tracker


async def get_performance_engine() -> PerformanceEngine:
    """Get performance engine instance"""
    return performance_engine


# Root endpoints
@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "service": "Portfolio Management System",
        "version": "1.0.0",
        "status": "running",
        "timestamp": datetime.now(timezone.utc)
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        # Test database connection
        portfolios = await portfolio_service.list_portfolios()
        
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc),
            "services": {
                "portfolio_service": "healthy",
                "real_time_tracker": "healthy" if real_time_tracker else "inactive",
                "performance_engine": "healthy" if performance_engine else "inactive"
            },
            "portfolios_count": len(portfolios)
        }
    except Exception as e:
        logger.error(f"Health check failed: {str(e)}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.now(timezone.utc)
            }
        )


# Real-time endpoints
@app.get("/api/v1/real-time/portfolio/{portfolio_id}/summary")
async def get_real_time_summary(
    portfolio_id: int,
    tracker: RealTimeTracker = Depends(get_real_time_tracker)
):
    """Get real-time portfolio summary"""
    try:
        if not tracker:
            raise HTTPException(status_code=503, detail="Real-time tracking not available")
        
        summary = await tracker.get_portfolio_summary_real_time(portfolio_id)
        return summary
    except Exception as e:
        logger.error(f"Error getting real-time summary: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/real-time/portfolio/{portfolio_id}/pnl")
async def get_real_time_pnl(
    portfolio_id: int,
    tracker: RealTimeTracker = Depends(get_real_time_tracker)
):
    """Get real-time P&L"""
    try:
        if not tracker:
            raise HTTPException(status_code=503, detail="Real-time tracking not available")
        
        pnl_data = await tracker.get_real_time_pnl(portfolio_id)
        return {
            "portfolio_id": portfolio_id,
            "pnl": pnl_data
        }
    except Exception as e:
        logger.error(f"Error getting real-time P&L: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/api/v1/real-time/start-tracking")
async def start_real_time_tracking(
    portfolio_ids: list[int],
    background_tasks: BackgroundTasks,
    tracker: RealTimeTracker = Depends(get_real_time_tracker)
):
    """Start real-time tracking for specified portfolios"""
    try:
        if not tracker:
            raise HTTPException(status_code=503, detail="Real-time tracking not available")
        
        background_tasks.add_task(tracker.start_tracking, portfolio_ids)
        
        return {
            "message": "Real-time tracking started",
            "portfolio_ids": portfolio_ids,
            "timestamp": datetime.now(timezone.utc)
        }
    except Exception as e:
        logger.error(f"Error starting real-time tracking: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Integration endpoints with trading platform
@app.post("/api/v1/integration/sync-positions")
async def sync_with_trading_platform(
    background_tasks: BackgroundTasks,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Sync positions with trading platform"""
    try:
        # This would integrate with the trading platform's position data
        background_tasks.add_task(_sync_trading_platform_positions, service)
        
        return {
            "message": "Position sync initiated",
            "timestamp": datetime.now(timezone.utc)
        }
    except Exception as e:
        logger.error(f"Error syncing with trading platform: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


async def _sync_trading_platform_positions(service: PortfolioService):
    """Background task to sync positions with trading platform"""
    try:
        # This would integrate with ../trading_platform/core/position_manager/
        # to get current positions and sync them with portfolio system
        logger.info("Syncing positions with trading platform...")
        
        # Placeholder for actual integration
        # positions = await trading_platform.get_all_positions()
        # for position in positions:
        #     await service.sync_position_from_trading_platform(position)
        
        logger.info("Position sync completed")
        
    except Exception as e:
        logger.error(f"Error in position sync: {str(e)}")


# Analytics endpoints
@app.get("/api/v1/analytics/portfolio-metrics")
async def get_portfolio_metrics_overview(
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get overview of all portfolio metrics"""
    try:
        portfolios = await service.list_portfolios()
        
        metrics_overview = []
        for portfolio in portfolios:
            # Get basic metrics for each portfolio
            summary = await service.get_portfolio_summary(portfolio.id)
            performance = await service.calculate_portfolio_performance(portfolio.id)
            
            metrics_overview.append({
                "portfolio_id": portfolio.id,
                "name": portfolio.name,
                "summary": summary.get("summary", {}),
                "performance": {
                    "total_return": performance.total_return,
                    "sharpe_ratio": performance.sharpe_ratio,
                    "max_drawdown": performance.max_drawdown
                }
            })
        
        return {
            "timestamp": datetime.now(timezone.utc),
            "portfolio_count": len(portfolios),
            "portfolios": metrics_overview
        }
    except Exception as e:
        logger.error(f"Error getting portfolio metrics overview: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Error handlers
@app.exception_handler(Exception)
async def global_exception_handler(request, exc):
    """Global exception handler"""
    logger.error(f"Unhandled exception: {str(exc)}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal server error",
            "message": "An unexpected error occurred",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )