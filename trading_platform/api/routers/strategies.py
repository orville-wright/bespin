"""
Strategy Management API

REST endpoints for strategy operations and management.
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse

from ...core import TradingEngine
from ...models import Strategy, StrategyStatus
from ..dependencies import get_trading_engine, require_permission

router = APIRouter()


@router.get("/", response_model=List[dict])
async def list_strategies(
    status_filter: Optional[str] = None,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """List all strategies with optional status filtering"""
    try:
        strategies = trading_engine.strategy_executor.get_active_strategies()
        
        if status_filter:
            strategies = [s for s in strategies if s.status == status_filter]
        
        return [
            {
                "strategy_id": s.strategy_id,
                "name": s.name,
                "strategy_type": s.strategy_type,
                "status": s.status,
                "symbols": s.symbols,
                "created_at": s.created_at,
                "performance": trading_engine.strategy_executor.get_strategy_performance(s.strategy_id)
            }
            for s in strategies
        ]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing strategies: {str(e)}"
        )


@router.get("/{strategy_id}")
async def get_strategy(
    strategy_id: str,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get detailed information about a specific strategy"""
    try:
        strategy = trading_engine.strategy_executor.get_strategy(strategy_id)
        
        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Strategy not found: {strategy_id}"
            )
        
        # Get associated positions and orders
        positions = await trading_engine.get_positions(strategy_id=strategy_id)
        orders = await trading_engine.get_orders(strategy_id=strategy_id)
        performance = trading_engine.strategy_executor.get_strategy_performance(strategy_id)
        
        return {
            "strategy": {
                "strategy_id": strategy.strategy_id,
                "name": strategy.name,
                "strategy_type": strategy.strategy_type,
                "status": strategy.status,
                "symbols": strategy.symbols,
                "parameters": strategy.parameters,
                "created_at": strategy.created_at,
                "updated_at": strategy.updated_at
            },
            "positions": [
                {
                    "position_id": p.position_id,
                    "symbol": p.symbol,
                    "quantity": float(p.quantity),
                    "status": p.status,
                    "total_pnl": float(p.total_pnl),
                    "opened_at": p.opened_at
                }
                for p in positions
            ],
            "recent_orders": [
                {
                    "order_id": o.order_id,
                    "symbol": o.symbol,
                    "side": o.side,
                    "quantity": float(o.quantity),
                    "status": o.status,
                    "submitted_at": o.submitted_at
                }
                for o in orders[-10:]  # Last 10 orders
            ],
            "performance": performance
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting strategy: {str(e)}"
        )


@router.post("/")
async def create_strategy(
    strategy_data: dict,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("trade"))
):
    """Create and start a new strategy"""
    try:
        # Validate required fields
        required_fields = ["name", "strategy_type", "symbols"]
        for field in required_fields:
            if field not in strategy_data:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Missing required field: {field}"
                )
        
        # Create strategy object
        strategy = Strategy(
            strategy_id=f"strategy_{len(trading_engine.active_strategies) + 1}",
            name=strategy_data["name"],
            strategy_type=strategy_data["strategy_type"],
            symbols=strategy_data["symbols"],
            parameters=strategy_data.get("parameters", {}),
            status=StrategyStatus.PENDING
        )
        
        # Add to trading engine
        success = await trading_engine.add_strategy(strategy)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to create strategy"
            )
        
        return {
            "strategy_id": strategy.strategy_id,
            "status": "created",
            "message": f"Strategy '{strategy.name}' created and started successfully"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating strategy: {str(e)}"
        )


@router.post("/{strategy_id}/start")
async def start_strategy(
    strategy_id: str,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("trade"))
):
    """Start a stopped strategy"""
    try:
        strategy = trading_engine.strategy_executor.get_strategy(strategy_id)
        
        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Strategy not found: {strategy_id}"
            )
        
        if strategy.status == StrategyStatus.ACTIVE:
            return {"status": "already_running", "message": "Strategy is already active"}
        
        # Restart strategy
        success = await trading_engine.add_strategy(strategy)
        
        if success:
            return {"status": "started", "message": f"Strategy {strategy_id} started successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to start strategy"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error starting strategy: {str(e)}"
        )


@router.post("/{strategy_id}/stop")
async def stop_strategy(
    strategy_id: str,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("trade"))
):
    """Stop a running strategy"""
    try:
        success = await trading_engine.remove_strategy(strategy_id)
        
        if success:
            return {"status": "stopped", "message": f"Strategy {strategy_id} stopped successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Strategy not found: {strategy_id}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error stopping strategy: {str(e)}"
        )


@router.delete("/{strategy_id}")
async def delete_strategy(
    strategy_id: str,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("trade"))
):
    """Delete a strategy (must be stopped first)"""
    try:
        strategy = trading_engine.strategy_executor.get_strategy(strategy_id)
        
        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Strategy not found: {strategy_id}"
            )
        
        if strategy.status == StrategyStatus.ACTIVE:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cannot delete active strategy. Stop it first."
            )
        
        # Remove strategy
        success = await trading_engine.remove_strategy(strategy_id)
        
        if success:
            return {"status": "deleted", "message": f"Strategy {strategy_id} deleted successfully"}
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to delete strategy"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error deleting strategy: {str(e)}"
        )


@router.get("/{strategy_id}/performance")
async def get_strategy_performance(
    strategy_id: str,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get detailed performance metrics for a strategy"""
    try:
        strategy = trading_engine.strategy_executor.get_strategy(strategy_id)
        
        if not strategy:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Strategy not found: {strategy_id}"
            )
        
        # Get positions for P&L calculation
        positions = await trading_engine.get_positions(strategy_id=strategy_id)
        
        total_pnl = sum(float(p.total_pnl) for p in positions)
        open_positions = len([p for p in positions if p.is_open])
        closed_positions = len([p for p in positions if p.is_closed])
        winning_positions = len([p for p in positions if p.is_closed and p.total_pnl > 0])
        
        win_rate = (winning_positions / max(closed_positions, 1)) * 100
        
        return {
            "strategy_id": strategy_id,
            "total_pnl": total_pnl,
            "open_positions": open_positions,
            "closed_positions": closed_positions,
            "winning_positions": winning_positions,
            "win_rate": win_rate,
            "performance_metrics": trading_engine.strategy_executor.get_strategy_performance(strategy_id)
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting strategy performance: {str(e)}"
        )