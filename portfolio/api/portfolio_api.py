"""
Portfolio Management API Endpoints
FastAPI router for portfolio CRUD operations
"""

from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import JSONResponse
import logging

from ..models.portfolio_models import (
    Portfolio, PortfolioCreate, PortfolioSummary, 
    Instrument, InstrumentCreate,
    Transaction, TransactionCreate,
    PerformanceAnalysis, RiskAnalysis
)
from ..services.portfolio_service import PortfolioService

router = APIRouter(prefix="/api/v1/portfolios", tags=["portfolios"])
logger = logging.getLogger(__name__)

# Dependency injection for portfolio service
async def get_portfolio_service() -> PortfolioService:
    """Get portfolio service instance"""
    # TODO: Replace with proper dependency injection
    connection_string = "postgresql://user:password@localhost:5432/trading_db"
    return PortfolioService(connection_string)


@router.post("/", response_model=dict)
async def create_portfolio(
    portfolio_data: PortfolioCreate,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Create a new portfolio"""
    try:
        portfolio = Portfolio(
            name=portfolio_data.name,
            description=portfolio_data.description,
            base_currency=portfolio_data.base_currency,
            initial_capital=portfolio_data.initial_capital
        )
        
        portfolio_id = await service.create_portfolio(portfolio)
        return {
            "portfolio_id": portfolio_id,
            "message": f"Portfolio '{portfolio_data.name}' created successfully"
        }
    except Exception as e:
        logger.error(f"Error creating portfolio: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/", response_model=List[Portfolio])
async def list_portfolios(
    service: PortfolioService = Depends(get_portfolio_service)
):
    """List all portfolios"""
    try:
        return await service.list_portfolios()
    except Exception as e:
        logger.error(f"Error listing portfolios: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{portfolio_id}", response_model=Portfolio)
async def get_portfolio(
    portfolio_id: int,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get portfolio by ID"""
    try:
        portfolio = await service.get_portfolio(portfolio_id)
        if not portfolio:
            raise HTTPException(status_code=404, detail="Portfolio not found")
        return portfolio
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{portfolio_id}/summary", response_model=dict)
async def get_portfolio_summary(
    portfolio_id: int,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get comprehensive portfolio summary"""
    try:
        summary = await service.get_portfolio_summary(portfolio_id)
        if not summary:
            raise HTTPException(status_code=404, detail="Portfolio not found")
        return summary
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting portfolio summary {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{portfolio_id}/performance", response_model=dict)
async def get_portfolio_performance(
    portfolio_id: int,
    start_date: Optional[datetime] = Query(None, description="Start date for performance analysis"),
    end_date: Optional[datetime] = Query(None, description="End date for performance analysis"),
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get portfolio performance metrics"""
    try:
        performance = await service.calculate_portfolio_performance(
            portfolio_id, start_date, end_date
        )
        return {
            "portfolio_id": portfolio_id,
            "performance": performance
        }
    except Exception as e:
        logger.error(f"Error calculating performance for portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{portfolio_id}/risk", response_model=dict)
async def get_portfolio_risk_metrics(
    portfolio_id: int,
    lookback_days: int = Query(252, description="Number of days for risk calculation"),
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get portfolio risk metrics"""
    try:
        risk_metrics = await service.calculate_risk_metrics(portfolio_id, lookback_days)
        return {
            "portfolio_id": portfolio_id,
            "risk_metrics": risk_metrics
        }
    except Exception as e:
        logger.error(f"Error calculating risk metrics for portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/{portfolio_id}/attribution", response_model=dict)
async def get_attribution_analysis(
    portfolio_id: int,
    benchmark_id: Optional[int] = Query(None, description="Benchmark ID for comparison"),
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get performance attribution analysis"""
    try:
        attribution = await service.calculate_attribution_analysis(portfolio_id, benchmark_id)
        return {
            "portfolio_id": portfolio_id,
            "attribution_analysis": attribution
        }
    except Exception as e:
        logger.error(f"Error calculating attribution for portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{portfolio_id}/transactions", response_model=dict)
async def add_transaction(
    portfolio_id: int,
    transaction_data: TransactionCreate,
    background_tasks: BackgroundTasks,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Add a transaction to portfolio"""
    try:
        transaction = Transaction(**transaction_data.dict())
        transaction.portfolio_id = portfolio_id
        
        transaction_id = await service.add_transaction(transaction)
        
        # Update portfolio performance in background
        background_tasks.add_task(service.update_portfolio_performance, portfolio_id)
        
        return {
            "transaction_id": transaction_id,
            "message": "Transaction added successfully"
        }
    except Exception as e:
        logger.error(f"Error adding transaction to portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/{portfolio_id}/update-positions", response_model=dict)
async def update_portfolio_positions(
    portfolio_id: int,
    background_tasks: BackgroundTasks,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Update portfolio positions with latest market data"""
    try:
        background_tasks.add_task(service.update_positions_with_market_data, portfolio_id)
        background_tasks.add_task(service.update_portfolio_performance, portfolio_id)
        
        return {
            "message": "Position update initiated",
            "portfolio_id": portfolio_id
        }
    except Exception as e:
        logger.error(f"Error updating positions for portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Instrument management endpoints
@router.post("/instruments", response_model=dict)
async def create_instrument(
    instrument_data: InstrumentCreate,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Create a new instrument"""
    try:
        instrument = Instrument(**instrument_data.dict())
        instrument_id = await service.create_instrument(instrument)
        
        return {
            "instrument_id": instrument_id,
            "message": f"Instrument '{instrument_data.symbol}' created successfully"
        }
    except Exception as e:
        logger.error(f"Error creating instrument: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/instruments/{symbol}", response_model=Instrument)
async def get_instrument(
    symbol: str,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get instrument by symbol"""
    try:
        instrument = await service.get_instrument_by_symbol(symbol.upper())
        if not instrument:
            raise HTTPException(status_code=404, detail="Instrument not found")
        return instrument
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting instrument {symbol}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Health check endpoint
@router.get("/health", response_model=dict)
async def health_check():
    """Portfolio API health check"""
    return {
        "status": "healthy",
        "service": "portfolio_api",
        "timestamp": datetime.now(timezone.utc).isoformat()
    }