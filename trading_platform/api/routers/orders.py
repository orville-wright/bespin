"""
Order Management API

REST endpoints for order operations and management.
"""

from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from decimal import Decimal

from ...core import TradingEngine
from ...models import OrderCreate, OrderSide, OrderType
from ..dependencies import get_trading_engine, require_permission

router = APIRouter()


@router.get("/", response_model=List[dict])
async def list_orders(
    strategy_id: Optional[str] = None,
    symbol: Optional[str] = None,
    status_filter: Optional[str] = None,
    limit: int = 100,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """List orders with optional filtering"""
    try:
        orders = await trading_engine.get_orders(
            strategy_id=strategy_id,
            status=status_filter
        )
        
        if symbol:
            orders = [o for o in orders if o.symbol.upper() == symbol.upper()]
        
        # Limit results
        orders = orders[-limit:]
        
        return [
            {
                "order_id": o.order_id,
                "strategy_id": o.strategy_id,
                "symbol": o.symbol,
                "side": o.side,
                "order_type": o.order_type,
                "status": o.status,
                "quantity": float(o.quantity),
                "filled_quantity": float(o.filled_quantity),
                "price": float(o.price) if o.price else None,
                "avg_fill_price": float(o.avg_fill_price) if o.avg_fill_price else None,
                "submitted_at": o.submitted_at,
                "filled_at": o.filled_at,
                "broker_order_id": o.broker_order_id,
                "is_simulation": o.is_simulation
            }
            for o in orders
        ]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error listing orders: {str(e)}"
        )


@router.get("/{order_id}")
async def get_order(
    order_id: str,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get detailed information about a specific order"""
    try:
        order = await trading_engine.order_manager.get_order(order_id)
        
        if not order:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Order not found: {order_id}"
            )
        
        return {
            "order_id": order.order_id,
            "strategy_id": order.strategy_id,
            "symbol": order.symbol,
            "side": order.side,
            "order_type": order.order_type,
            "status": order.status,
            "quantity": float(order.quantity),
            "filled_quantity": float(order.filled_quantity),
            "remaining_quantity": float(order.remaining_quantity),
            "price": float(order.price) if order.price else None,
            "stop_price": float(order.stop_price) if order.stop_price else None,
            "avg_fill_price": float(order.avg_fill_price) if order.avg_fill_price else None,
            "time_in_force": order.time_in_force,
            "submitted_at": order.submitted_at,
            "filled_at": order.filled_at,
            "expires_at": order.expires_at,
            "broker_order_id": order.broker_order_id,
            "parent_order_id": order.parent_order_id,
            "metadata": order.metadata,
            "is_simulation": order.is_simulation,
            "is_active": order.is_active,
            "is_filled": order.is_filled
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting order: {str(e)}"
        )


@router.post("/")
async def create_order(
    order_data: dict,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("trade"))
):
    """Create a new order"""
    try:
        # Validate required fields
        required_fields = ["symbol", "side", "quantity"]
        for field in required_fields:
            if field not in order_data:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Missing required field: {field}"
                )
        
        # Validate enum values
        try:
            side = OrderSide(order_data["side"])
            order_type = OrderType(order_data.get("order_type", "market"))
        except ValueError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid enum value: {str(e)}"
            )
        
        # Create order
        order_create = OrderCreate(
            strategy_id=order_data.get("strategy_id", "manual"),
            symbol=order_data["symbol"].upper(),
            side=side,
            order_type=order_type,
            quantity=Decimal(str(order_data["quantity"])),
            price=Decimal(str(order_data["price"])) if order_data.get("price") else None,
            stop_price=Decimal(str(order_data["stop_price"])) if order_data.get("stop_price") else None,
            time_in_force=order_data.get("time_in_force", "DAY"),
            metadata=order_data.get("metadata", {}),
            is_simulation=order_data.get("is_simulation", trading_engine.is_simulation)
        )
        
        # Submit order
        order = await trading_engine.create_manual_order(order_create)
        
        if not order:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Failed to create order"
            )
        
        return {
            "order_id": order.order_id,
            "status": "created",
            "message": f"Order created successfully for {order.symbol}",
            "order_details": {
                "symbol": order.symbol,
                "side": order.side,
                "quantity": float(order.quantity),
                "order_type": order.order_type,
                "status": order.status
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error creating order: {str(e)}"
        )


@router.post("/{order_id}/cancel")
async def cancel_order(
    order_id: str,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("trade"))
):
    """Cancel an existing order"""
    try:
        success = await trading_engine.cancel_order(order_id)
        
        if success:
            return {
                "order_id": order_id,
                "status": "canceled",
                "message": f"Order {order_id} canceled successfully"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Failed to cancel order: {order_id}"
            )
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error canceling order: {str(e)}"
        )


@router.get("/pending")
async def get_pending_orders(
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get all pending orders"""
    try:
        pending_orders = await trading_engine.order_manager.get_pending_orders()
        
        return [
            {
                "order_id": o.order_id,
                "strategy_id": o.strategy_id,
                "symbol": o.symbol,
                "side": o.side,
                "order_type": o.order_type,
                "quantity": float(o.quantity),
                "filled_quantity": float(o.filled_quantity),
                "remaining_quantity": float(o.remaining_quantity),
                "price": float(o.price) if o.price else None,
                "submitted_at": o.submitted_at,
                "status": o.status
            }
            for o in pending_orders
        ]
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting pending orders: {str(e)}"
        )


@router.get("/stats")
async def get_order_stats(
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("view"))
):
    """Get order management statistics"""
    try:
        stats = trading_engine.order_manager.get_stats()
        
        return {
            "order_statistics": stats,
            "timestamp": "2025-07-13T06:04:00Z"
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error getting order stats: {str(e)}"
        )


# Bulk operations
@router.post("/bulk/cancel")
async def cancel_orders_bulk(
    order_data: dict,
    trading_engine: TradingEngine = Depends(get_trading_engine),
    user=Depends(require_permission("trade"))
):
    """Cancel multiple orders at once"""
    try:
        order_ids = order_data.get("order_ids", [])
        strategy_id = order_data.get("strategy_id")
        symbol = order_data.get("symbol")
        
        if not order_ids and not strategy_id and not symbol:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Must provide order_ids, strategy_id, or symbol"
            )
        
        results = []
        
        if order_ids:
            # Cancel specific orders
            for order_id in order_ids:
                success = await trading_engine.cancel_order(order_id)
                results.append({"order_id": order_id, "success": success})
        else:
            # Cancel orders by criteria
            orders = await trading_engine.get_orders(strategy_id=strategy_id)
            
            if symbol:
                orders = [o for o in orders if o.symbol.upper() == symbol.upper()]
            
            # Only cancel active orders
            active_orders = [o for o in orders if o.is_active]
            
            for order in active_orders:
                success = await trading_engine.cancel_order(order.order_id)
                results.append({"order_id": order.order_id, "success": success})
        
        successful_cancellations = len([r for r in results if r["success"]])
        
        return {
            "status": "completed",
            "total_orders": len(results),
            "successful_cancellations": successful_cancellations,
            "failed_cancellations": len(results) - successful_cancellations,
            "results": results
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error in bulk cancel: {str(e)}"
        )