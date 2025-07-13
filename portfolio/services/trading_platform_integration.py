"""
Trading Platform Integration
Integration layer between portfolio management and trading platform
"""

import asyncio
import logging
from datetime import datetime, timezone
from decimal import Decimal
from typing import List, Dict, Any, Optional
import json

from ..models.portfolio_models import (
    Position, Transaction, Instrument, TransactionType
)
from .portfolio_service import PortfolioService

# Import trading platform models (when available)
try:
    from ...trading_platform.models.positions import Position as TradingPosition
    from ...trading_platform.models.orders import Order, OrderStatus
    from ...trading_platform.core.position_manager import PositionManager
except ImportError:
    # Fallback for when trading platform is not available
    TradingPosition = None
    Order = None
    OrderStatus = None
    PositionManager = None


class TradingPlatformIntegration:
    """
    Integration service between portfolio management and trading platform
    Handles position synchronization, order tracking, and real-time updates
    """
    
    def __init__(self, portfolio_service: PortfolioService):
        self.portfolio_service = portfolio_service
        self.logger = logging.getLogger(__name__)
        self.position_manager = None
        self.sync_enabled = False
        
        # Initialize trading platform integration if available
        if PositionManager:
            try:
                self.position_manager = PositionManager()
                self.sync_enabled = True
                self.logger.info("Trading platform integration enabled")
            except Exception as e:
                self.logger.warning(f"Could not initialize trading platform integration: {str(e)}")
    
    async def sync_all_positions(self) -> Dict[str, Any]:
        """Sync all positions from trading platform to portfolio management"""
        if not self.sync_enabled:
            return {"error": "Trading platform integration not available"}
        
        try:
            # Get all positions from trading platform
            trading_positions = await self._get_trading_platform_positions()
            
            sync_results = {
                "synced_positions": 0,
                "new_positions": 0,
                "updated_positions": 0,
                "errors": []
            }
            
            for trading_pos in trading_positions:
                try:
                    result = await self._sync_single_position(trading_pos)
                    if result["action"] == "created":
                        sync_results["new_positions"] += 1
                    elif result["action"] == "updated":
                        sync_results["updated_positions"] += 1
                    
                    sync_results["synced_positions"] += 1
                    
                except Exception as e:
                    error_msg = f"Error syncing position {trading_pos.symbol}: {str(e)}"
                    self.logger.error(error_msg)
                    sync_results["errors"].append(error_msg)
            
            self.logger.info(f"Position sync completed: {sync_results}")
            return sync_results
            
        except Exception as e:
            self.logger.error(f"Error in sync_all_positions: {str(e)}")
            return {"error": str(e)}
    
    async def _get_trading_platform_positions(self) -> List[Any]:
        """Get all positions from trading platform"""
        if not self.position_manager:
            return []
        
        try:
            # This would call the trading platform's position manager
            # For now, return empty list as placeholder
            positions = []
            # positions = await self.position_manager.get_all_positions()
            return positions
        except Exception as e:
            self.logger.error(f"Error getting trading platform positions: {str(e)}")
            return []
    
    async def _sync_single_position(self, trading_position: Any) -> Dict[str, str]:
        """Sync a single position from trading platform"""
        try:
            # Get or create instrument
            instrument = await self.portfolio_service.get_instrument_by_symbol(trading_position.symbol)
            if not instrument:
                # Create instrument if it doesn't exist
                instrument_data = Instrument(
                    symbol=trading_position.symbol,
                    instrument_type=self._map_instrument_type(trading_position),
                    exchange=getattr(trading_position, 'exchange', None),
                    currency="USD"  # Default, could be extracted from trading position
                )
                instrument_id = await self.portfolio_service.create_instrument(instrument_data)
                instrument = await self.portfolio_service.get_instrument_by_id(instrument_id)
            
            # Determine portfolio ID (this would be configured)
            portfolio_id = await self._get_portfolio_for_strategy(trading_position.strategy_id)
            
            # Check if position already exists in portfolio
            existing_positions = await self.portfolio_service.get_current_positions(portfolio_id)
            existing_position = next(
                (pos for pos in existing_positions if pos.instrument_id == instrument.id), 
                None
            )
            
            # Create transaction to represent the position
            transaction = Transaction(
                portfolio_id=portfolio_id,
                instrument_id=instrument.id,
                transaction_type=TransactionType.BUY if trading_position.quantity > 0 else TransactionType.SELL,
                quantity=abs(trading_position.quantity),
                price=trading_position.avg_entry_price,
                execution_time=trading_position.opened_at,
                order_id=getattr(trading_position, 'order_id', None),
                strategy_name=trading_position.strategy_id,
                notes=f"Synced from trading platform: {trading_position.position_id}"
            )
            
            if existing_position:
                # Update existing position if needed
                if (existing_position.quantity != trading_position.quantity or 
                    existing_position.average_cost != trading_position.avg_entry_price):
                    
                    await self.portfolio_service.add_transaction(transaction)
                    return {"action": "updated", "position_id": existing_position.portfolio_id}
                else:
                    return {"action": "unchanged", "position_id": existing_position.portfolio_id}
            else:
                # Create new position
                await self.portfolio_service.add_transaction(transaction)
                return {"action": "created", "position_id": portfolio_id}
                
        except Exception as e:
            self.logger.error(f"Error syncing single position: {str(e)}")
            raise
    
    async def _get_portfolio_for_strategy(self, strategy_id: str) -> int:
        """Get portfolio ID for a given strategy"""
        # This would be configured based on your strategy-to-portfolio mapping
        # For now, return a default portfolio ID
        portfolios = await self.portfolio_service.list_portfolios()
        if portfolios:
            return portfolios[0].id  # Use first portfolio as default
        else:
            # Create a default portfolio if none exist
            from ..models.portfolio_models import Portfolio
            default_portfolio = Portfolio(
                name="Default Trading Portfolio",
                description="Automatically created for trading platform integration",
                initial_capital=Decimal("100000.00")
            )
            return await self.portfolio_service.create_portfolio(default_portfolio)
    
    def _map_instrument_type(self, trading_position: Any) -> str:
        """Map trading platform instrument type to portfolio instrument type"""
        # This would map the trading platform's instrument types
        # to the portfolio management system's types
        return "stock"  # Default mapping
    
    async def handle_order_execution(self, order_data: Dict[str, Any]):
        """Handle order execution events from trading platform"""
        try:
            # Create transaction from order execution
            transaction = Transaction(
                portfolio_id=order_data.get("portfolio_id", 1),  # Default portfolio
                instrument_id=order_data.get("instrument_id"),
                transaction_type=TransactionType.BUY if order_data.get("side") == "buy" else TransactionType.SELL,
                quantity=Decimal(str(order_data.get("quantity", 0))),
                price=Decimal(str(order_data.get("fill_price", 0))),
                total_amount=Decimal(str(order_data.get("total_amount", 0))),
                fees=Decimal(str(order_data.get("fees", 0))),
                commission=Decimal(str(order_data.get("commission", 0))),
                execution_time=datetime.fromisoformat(order_data.get("execution_time", datetime.now(timezone.utc).isoformat())),
                order_id=order_data.get("order_id"),
                strategy_name=order_data.get("strategy_name"),
                notes=f"Order execution: {order_data.get('order_id')}"
            )
            
            # Add transaction to portfolio
            transaction_id = await self.portfolio_service.add_transaction(transaction)
            
            self.logger.info(f"Processed order execution: {order_data.get('order_id')} -> transaction {transaction_id}")
            
            return {"transaction_id": transaction_id, "status": "processed"}
            
        except Exception as e:
            self.logger.error(f"Error handling order execution: {str(e)}")
            return {"error": str(e), "status": "failed"}
    
    async def get_portfolio_positions_for_strategy(self, strategy_id: str) -> List[Position]:
        """Get portfolio positions for a specific strategy"""
        try:
            # Find portfolio for strategy
            portfolio_id = await self._get_portfolio_for_strategy(strategy_id)
            
            # Get positions
            positions = await self.portfolio_service.get_current_positions(portfolio_id)
            
            # Filter by strategy if transaction history includes strategy info
            # This would require enhancing the position model to track strategy
            return positions
            
        except Exception as e:
            self.logger.error(f"Error getting positions for strategy {strategy_id}: {str(e)}")
            return []
    
    async def calculate_strategy_performance(self, strategy_id: str) -> Dict[str, Any]:
        """Calculate performance metrics for a specific strategy"""
        try:
            positions = await self.get_portfolio_positions_for_strategy(strategy_id)
            
            if not positions:
                return {"error": "No positions found for strategy"}
            
            # Calculate strategy-level metrics
            total_pnl = sum(pos.total_pnl for pos in positions)
            total_market_value = sum(pos.market_value or Decimal('0') for pos in positions)
            total_cost_basis = sum(pos.quantity * pos.average_cost for pos in positions)
            
            return {
                "strategy_id": strategy_id,
                "position_count": len(positions),
                "total_pnl": total_pnl,
                "total_market_value": total_market_value,
                "total_cost_basis": total_cost_basis,
                "return_percentage": (total_pnl / total_cost_basis * 100) if total_cost_basis != 0 else Decimal('0'),
                "timestamp": datetime.now(timezone.utc)
            }
            
        except Exception as e:
            self.logger.error(f"Error calculating strategy performance: {str(e)}")
            return {"error": str(e)}
    
    async def start_real_time_sync(self):
        """Start real-time synchronization with trading platform"""
        if not self.sync_enabled:
            self.logger.warning("Real-time sync not available - trading platform integration disabled")
            return
        
        try:
            # Start background task for continuous sync
            asyncio.create_task(self._real_time_sync_loop())
            self.logger.info("Real-time synchronization started")
            
        except Exception as e:
            self.logger.error(f"Error starting real-time sync: {str(e)}")
    
    async def _real_time_sync_loop(self):
        """Background loop for real-time synchronization"""
        while self.sync_enabled:
            try:
                # Sync positions every 30 seconds
                await self.sync_all_positions()
                await asyncio.sleep(30)
                
            except Exception as e:
                self.logger.error(f"Error in real-time sync loop: {str(e)}")
                await asyncio.sleep(60)  # Wait longer on error
    
    def stop_real_time_sync(self):
        """Stop real-time synchronization"""
        self.sync_enabled = False
        self.logger.info("Real-time synchronization stopped")