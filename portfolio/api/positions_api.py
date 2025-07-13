"""
Positions API Endpoints
FastAPI router for position tracking and management
"""

from datetime import datetime, timezone, timedelta
from decimal import Decimal
from typing import List, Optional
from fastapi import APIRouter, HTTPException, Depends, Query, BackgroundTasks
from fastapi.responses import JSONResponse
import logging

from ..models.portfolio_models import (
    Position, PositionResponse, MarketData
)
from ..services.portfolio_service import PortfolioService

router = APIRouter(prefix="/api/v1/positions", tags=["positions"])
logger = logging.getLogger(__name__)


# Dependency injection for portfolio service
async def get_portfolio_service() -> PortfolioService:
    """Get portfolio service instance"""
    # TODO: Replace with proper dependency injection
    connection_string = "postgresql://user:password@localhost:5432/trading_db"
    return PortfolioService(connection_string)


@router.get("/portfolio/{portfolio_id}", response_model=List[PositionResponse])
async def get_portfolio_positions(
    portfolio_id: int,
    include_zero: bool = Query(False, description="Include positions with zero quantity"),
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get all current positions for a portfolio"""
    try:
        positions = await service.get_current_positions(portfolio_id)
        
        # Filter out zero positions if requested
        if not include_zero:
            positions = [pos for pos in positions if pos.quantity != 0]
        
        # Convert to response format with additional data
        response_positions = []
        for pos in positions:
            # Get instrument symbol
            instrument = await service.get_instrument_by_id(pos.instrument_id)
            symbol = instrument.symbol if instrument else "UNKNOWN"
            
            response_positions.append(PositionResponse(
                portfolio_id=pos.portfolio_id,
                instrument_id=pos.instrument_id,
                symbol=symbol,
                quantity=pos.quantity,
                average_cost=pos.average_cost,
                market_value=pos.market_value or Decimal('0'),
                unrealized_pnl=pos.unrealized_pnl or Decimal('0'),
                realized_pnl=pos.realized_pnl,
                return_pct=pos.return_pct,
                timestamp=pos.timestamp
            ))
        
        return response_positions
    except Exception as e:
        logger.error(f"Error getting positions for portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/instrument/{instrument_id}", response_model=PositionResponse)
async def get_position_by_instrument(
    portfolio_id: int,
    instrument_id: int,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get current position for a specific instrument"""
    try:
        positions = await service.get_current_positions(portfolio_id)
        position = next((pos for pos in positions if pos.instrument_id == instrument_id), None)
        
        if not position:
            raise HTTPException(status_code=404, detail="Position not found")
        
        # Get instrument symbol
        instrument = await service.get_instrument_by_id(instrument_id)
        symbol = instrument.symbol if instrument else "UNKNOWN"
        
        return PositionResponse(
            portfolio_id=position.portfolio_id,
            instrument_id=position.instrument_id,
            symbol=symbol,
            quantity=position.quantity,
            average_cost=position.average_cost,
            market_value=position.market_value or Decimal('0'),
            unrealized_pnl=position.unrealized_pnl or Decimal('0'),
            realized_pnl=position.realized_pnl,
            return_pct=position.return_pct,
            timestamp=position.timestamp
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting position for portfolio {portfolio_id}, instrument {instrument_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/history/{instrument_id}", response_model=List[Position])
async def get_position_history(
    portfolio_id: int,
    instrument_id: int,
    start_date: Optional[datetime] = Query(None, description="Start date for history"),
    end_date: Optional[datetime] = Query(None, description="End date for history"),
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get position history for a specific instrument"""
    try:
        if start_date is None:
            start_date = datetime.now(timezone.utc) - timedelta(days=30)
        if end_date is None:
            end_date = datetime.now(timezone.utc)
        
        history = await service.get_position_history(
            portfolio_id, instrument_id, start_date, end_date
        )
        return history
    except Exception as e:
        logger.error(f"Error getting position history: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/pnl", response_model=dict)
async def get_portfolio_pnl(
    portfolio_id: int,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get portfolio-level P&L summary"""
    try:
        positions = await service.get_current_positions(portfolio_id)
        
        total_unrealized_pnl = sum(pos.unrealized_pnl or Decimal('0') for pos in positions)
        total_realized_pnl = sum(pos.realized_pnl for pos in positions)
        total_pnl = total_unrealized_pnl + total_realized_pnl
        total_market_value = sum(pos.market_value or Decimal('0') for pos in positions)
        total_cost_basis = sum(pos.quantity * pos.average_cost for pos in positions)
        
        return {
            "portfolio_id": portfolio_id,
            "timestamp": datetime.now(timezone.utc),
            "pnl_summary": {
                "unrealized_pnl": total_unrealized_pnl,
                "realized_pnl": total_realized_pnl,
                "total_pnl": total_pnl,
                "total_market_value": total_market_value,
                "total_cost_basis": total_cost_basis,
                "return_percentage": (total_pnl / total_cost_basis * 100) if total_cost_basis != 0 else Decimal('0')
            },
            "position_count": len(positions)
        }
    except Exception as e:
        logger.error(f"Error calculating P&L for portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/allocation", response_model=dict)
async def get_portfolio_allocation(
    portfolio_id: int,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get portfolio allocation breakdown"""
    try:
        positions = await service.get_current_positions(portfolio_id)
        total_value = sum(pos.market_value or Decimal('0') for pos in positions)
        
        if total_value == 0:
            return {
                "portfolio_id": portfolio_id,
                "total_value": Decimal('0'),
                "allocations": []
            }
        
        allocations = []
        for pos in positions:
            if pos.quantity != 0:
                # Get instrument details
                instrument = await service.get_instrument_by_id(pos.instrument_id)
                
                weight = (pos.market_value or Decimal('0')) / total_value * 100
                allocations.append({
                    "symbol": instrument.symbol if instrument else "UNKNOWN",
                    "instrument_id": pos.instrument_id,
                    "sector": instrument.sector if instrument else None,
                    "instrument_type": instrument.instrument_type.value if instrument else "unknown",
                    "quantity": pos.quantity,
                    "market_value": pos.market_value or Decimal('0'),
                    "weight_percent": weight,
                    "unrealized_pnl": pos.unrealized_pnl or Decimal('0')
                })
        
        # Sort by weight descending
        allocations.sort(key=lambda x: x["weight_percent"], reverse=True)
        
        return {
            "portfolio_id": portfolio_id,
            "timestamp": datetime.now(timezone.utc),
            "total_value": total_value,
            "allocations": allocations
        }
    except Exception as e:
        logger.error(f"Error calculating allocation for portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/update-market-data", response_model=dict)
async def update_market_data(
    market_data: List[MarketData],
    background_tasks: BackgroundTasks,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Update market data for instruments"""
    try:
        await service.update_market_data(market_data)
        
        # Update all portfolio positions in background
        background_tasks.add_task(service.update_positions_with_market_data)
        
        return {
            "message": f"Updated market data for {len(market_data)} instruments",
            "timestamp": datetime.now(timezone.utc)
        }
    except Exception as e:
        logger.error(f"Error updating market data: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/portfolio/{portfolio_id}/exposure", response_model=dict)
async def get_portfolio_exposure(
    portfolio_id: int,
    service: PortfolioService = Depends(get_portfolio_service)
):
    """Get portfolio exposure analysis by sector, currency, etc."""
    try:
        positions = await service.get_current_positions(portfolio_id)
        total_value = sum(pos.market_value or Decimal('0') for pos in positions)
        
        if total_value == 0:
            return {
                "portfolio_id": portfolio_id,
                "exposures": {
                    "sector": {},
                    "currency": {},
                    "instrument_type": {}
                }
            }
        
        sector_exposure = {}
        currency_exposure = {}
        type_exposure = {}
        
        for pos in positions:
            if pos.quantity != 0:
                instrument = await service.get_instrument_by_id(pos.instrument_id)
                market_value = pos.market_value or Decimal('0')
                weight = market_value / total_value * 100
                
                if instrument:
                    # Sector exposure
                    sector = instrument.sector or "Unknown"
                    sector_exposure[sector] = sector_exposure.get(sector, Decimal('0')) + weight
                    
                    # Currency exposure
                    currency = instrument.currency
                    currency_exposure[currency] = currency_exposure.get(currency, Decimal('0')) + weight
                    
                    # Instrument type exposure
                    inst_type = instrument.instrument_type.value
                    type_exposure[inst_type] = type_exposure.get(inst_type, Decimal('0')) + weight
        
        return {
            "portfolio_id": portfolio_id,
            "timestamp": datetime.now(timezone.utc),
            "total_value": total_value,
            "exposures": {
                "sector": sector_exposure,
                "currency": currency_exposure,
                "instrument_type": type_exposure
            }
        }
    except Exception as e:
        logger.error(f"Error calculating exposure for portfolio {portfolio_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/health", response_model=dict)
async def health_check():
    """Positions API health check"""
    return {
        "status": "healthy",
        "service": "positions_api", 
        "timestamp": datetime.now(timezone.utc).isoformat()
    }