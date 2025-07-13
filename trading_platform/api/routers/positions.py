"""
Position Management API

REST endpoints for position operations and tracking.
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from decimal import Decimal

from ...core import TradingEngine
from ...models import PositionCreate
from ..dependencies import get_trading_engine, require_permission

router = APIRouter()


@router.get("/", response_model=List[dict])
async def list_positions(
    strategy_id: Optional[str] = None,
    symbol: Optional[str] = None,
    status_filter: Optional[str] = None,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """List positions with optional filtering"""
    try:
        positions = await trading_engine.get_positions(
            strategy_id=strategy_id,
            symbol=symbol,
            status=status_filter
        )
        
        return [
            {
                "position_id": p.position_id,
                "strategy_id": p.strategy_id,
                "symbol": p.symbol,
                "status": p.status,
                "quantity": float(p.quantity),
                "position_size": float(p.position_size),
                "is_long": p.is_long,
                "is_short": p.is_short,
                "avg_entry_price": float(p.avg_entry_price),
                "current_price": float(p.current_price) if p.current_price else None,
                "market_value": float(p.market_value) if p.market_value else None,
                "cost_basis": float(p.cost_basis),
                "unrealized_pnl": float(p.unrealized_pnl),
                "realized_pnl": float(p.realized_pnl),
                "total_pnl": float(p.total_pnl),
                "pnl_percentage": float(p.pnl_percentage) if p.pnl_percentage else None,
                "opened_at": p.opened_at,
                "closed_at": p.closed_at,
                "holding_period": p.holding_period,
                "stop_loss": float(p.stop_loss) if p.stop_loss else None,
                "take_profit": float(p.take_profit) if p.take_profit else None,
                "total_fees": float(p.total_fees),
                "is_simulation": p.is_simulation
            }
            for p in positions
        ]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing positions: {str(e)}"
        )


@router.get("/{position_id}")
async def get_position(
    position_id: str,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get detailed information about a specific position"""
    try:
        position = await trading_engine.position_manager.get_position(position_id)
        
        if not position:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Position not found: {position_id}"
            )
        
        # Get related orders
        orders = await trading_engine.get_orders(strategy_id=position.strategy_id)
        related_orders = [o for o in orders if o.symbol == position.symbol]
        
        return {
            "position": {
                "position_id": position.position_id,
                "strategy_id": position.strategy_id,
                "symbol": position.symbol,
                "status": position.status,
                "quantity": float(position.quantity),
                "position_size": float(position.position_size),
                "is_long": position.is_long,
                "is_short": position.is_short,
                "avg_entry_price": float(position.avg_entry_price),
                "current_price": float(position.current_price) if position.current_price else None,
                "market_value": float(position.market_value) if position.market_value else None,
                "cost_basis": float(position.cost_basis),
                "unrealized_pnl": float(position.unrealized_pnl),
                "realized_pnl": float(position.realized_pnl),
                "total_pnl": float(position.total_pnl),
                "pnl_percentage": float(position.pnl_percentage) if position.pnl_percentage else None,
                "opened_at": position.opened_at,
                "closed_at": position.closed_at,
                "holding_period": position.holding_period,
                "stop_loss": float(position.stop_loss) if position.stop_loss else None,
                "take_profit": float(position.take_profit) if position.take_profit else None,
                "total_fees": float(position.total_fees),
                "metadata": position.metadata,
                "is_simulation": position.is_simulation
            },
            "related_orders": [
                {
                    "order_id": o.order_id,
                    "side": o.side,
                    "quantity": float(o.quantity),
                    "status": o.status,
                    "avg_fill_price": float(o.avg_fill_price) if o.avg_fill_price else None,
                    "submitted_at": o.submitted_at,
                    "filled_at": o.filled_at
                }
                for o in related_orders
            ]
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting position: {str(e)}"
        )


@router.post("/")
async def create_position(
    position_data: dict,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("trade"))
):
    """Create a manual position"""
    try:
        # Validate required fields
        required_fields = ["strategy_id", "symbol", "quantity", "avg_entry_price", "cost_basis"]
        for field in required_fields:
            if field not in position_data:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Missing required field: {field}"
                )
        
        # Create position
        position_create = PositionCreate(
            strategy_id=position_data["strategy_id"],
            symbol=position_data["symbol"].upper(),
            quantity=Decimal(str(position_data["quantity"])),
            avg_entry_price=Decimal(str(position_data["avg_entry_price"])),
            cost_basis=Decimal(str(position_data["cost_basis"])),
            stop_loss=Decimal(str(position_data["stop_loss"])) if position_data.get("stop_loss") else None,
            take_profit=Decimal(str(position_data["take_profit"])) if position_data.get("take_profit") else None,
            metadata=position_data.get("metadata", {}),
            is_simulation=position_data.get("is_simulation", trading_engine.is_simulation)
        )
        
        # Create position
        position = await trading_engine.position_manager.create_position(position_create)
        
        if not position:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to create position"
            )
        
        return {
            "position_id": position.position_id,
            "status": "created",
            "message": f"Position created successfully for {position.symbol}",
            "position_details": {
                "symbol": position.symbol,
                "quantity": float(position.quantity),
                "avg_entry_price": float(position.avg_entry_price),
                "cost_basis": float(position.cost_basis)
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating position: {str(e)}"
        )


@router.post("/{position_id}/close")
async def close_position(
    position_id: str,
    close_data: dict,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("trade"))
):
    """Manually close a position"""
    try:
        if "close_price" not in close_data:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing required field: close_price"
            )
        
        close_price = Decimal(str(close_data["close_price"]))
        
        success = await trading_engine.position_manager.close_position_manual(position_id, close_price)
        
        if success:
            return {
                "position_id": position_id,
                "status": "closed",
                "message": f"Position {position_id} closed successfully",
                "close_price": float(close_price)
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to close position: {position_id}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error closing position: {str(e)}"
        )


@router.get("/open")
async def get_open_positions(
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get all open positions"""
    try:
        open_positions = await trading_engine.position_manager.get_open_positions()
        
        return [
            {
                "position_id": p.position_id,
                "strategy_id": p.strategy_id,
                "symbol": p.symbol,
                "quantity": float(p.quantity),
                "is_long": p.is_long,
                "avg_entry_price": float(p.avg_entry_price),
                "current_price": float(p.current_price) if p.current_price else None,
                "unrealized_pnl": float(p.unrealized_pnl),
                "pnl_percentage": float(p.pnl_percentage) if p.pnl_percentage else None,
                "opened_at": p.opened_at,
                "holding_period": p.holding_period
            }
            for p in open_positions
        ]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting open positions: {str(e)}"
        )


@router.get("/stats")
async def get_position_stats(
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get position management statistics"""
    try:
        stats = trading_engine.position_manager.get_stats()
        
        return {
            "position_statistics": stats,
            "timestamp": "2025-07-13T06:05:00Z"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting position stats: {str(e)}"
        )


@router.get("/pnl/summary")
async def get_pnl_summary(
    strategy_id: Optional[str] = None,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get P&L summary across positions"""
    try:
        positions = await trading_engine.get_positions(strategy_id=strategy_id)
        
        open_positions = [p for p in positions if p.is_open]
        closed_positions = [p for p in positions if p.is_closed]
        
        total_unrealized_pnl = sum(float(p.unrealized_pnl) for p in open_positions)
        total_realized_pnl = sum(float(p.realized_pnl) for p in closed_positions)
        total_pnl = total_unrealized_pnl + total_realized_pnl
        
        winning_positions = len([p for p in closed_positions if p.total_pnl > 0])
        losing_positions = len([p for p in closed_positions if p.total_pnl <= 0])
        
        win_rate = (winning_positions / max(len(closed_positions), 1)) * 100
        
        # Group by symbol
        symbol_pnl = {}
        for position in positions:
            symbol = position.symbol
            if symbol not in symbol_pnl:
                symbol_pnl[symbol] = {
                    "symbol": symbol,
                    "total_pnl": 0.0,
                    "unrealized_pnl": 0.0,
                    "realized_pnl": 0.0,
                    "open_positions": 0,
                    "closed_positions": 0
                }
            
            symbol_pnl[symbol]["total_pnl"] += float(position.total_pnl)
            symbol_pnl[symbol]["unrealized_pnl"] += float(position.unrealized_pnl)
            symbol_pnl[symbol]["realized_pnl"] += float(position.realized_pnl)
            
            if position.is_open:
                symbol_pnl[symbol]["open_positions"] += 1
            else:
                symbol_pnl[symbol]["closed_positions"] += 1
        
        return {
            "summary": {
                "total_pnl": total_pnl,
                "unrealized_pnl": total_unrealized_pnl,
                "realized_pnl": total_realized_pnl,
                "open_positions": len(open_positions),
                "closed_positions": len(closed_positions),
                "winning_positions": winning_positions,
                "losing_positions": losing_positions,
                "win_rate": win_rate
            },
            "by_symbol": list(symbol_pnl.values()),
            "strategy_id": strategy_id,
            "timestamp": "2025-07-13T06:05:00Z"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting P&L summary: {str(e)}"
        )